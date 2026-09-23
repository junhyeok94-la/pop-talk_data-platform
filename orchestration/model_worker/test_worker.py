"""실제 GPU를 사용하지 않는 admission 및 취소 경합 회귀 검사."""

import asyncio
import json
import hashlib
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
from fastapi import HTTPException
import worker


class WorkerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from workbench_test_db import isolated_database
        from airflow_workbench import metadata
        from airflow_workbench import executor_state
        from fastapi.testclient import TestClient
        import os

        isolated_database(self)
        schema = os.environ["AIRFLOW_WORKBENCH_DB_SCHEMA"]
        self.database = lambda: metadata.connection(schema)
        self.client = TestClient(executor_state.app)
        executor_state.app.dependency_overrides[executor_state.executor_identity] = (
            lambda: "test-worker"
        )
        self.addCleanup(executor_state.app.dependency_overrides.clear)
        self.addCleanup(self.client.close)

        def call(method, path, payload=None):
            response = self.client.request(method, path, json=payload)
            if response.status_code >= 400:
                raise HTTPException(response.status_code, response.json()["detail"])
            return response.json()

        storage = patch.object(worker.state, "call", call)
        storage.start()
        self.addCleanup(storage.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.root = patch.object(worker, "ROOT", Path(self.temp.name))
        self.root.start()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.root.stop)

    def insert(self, status="queued"):
        with self.database() as db:
            db.execute(
                "INSERT INTO executor_jobs VALUES ('test-worker','job','key','hash',?,?,?,'{}',NULL,NULL)",
                (status, time.time(), time.time()),
            )

    async def test_queued_cancel_never_starts_subprocess(self):
        self.insert()
        entered = asyncio.Event()
        resumed = asyncio.Event()

        async def report(*args):
            entered.set()
            await resumed.wait()
            return {"ready": True}

        spawn = AsyncMock()
        with patch.object(worker.asyncio, "to_thread", report), patch.object(
            worker.asyncio, "create_subprocess_exec", spawn
        ):
            task = asyncio.create_task(
                worker.execute(
                    "job",
                    worker.JobSpec(key="key", kind="diagnostic", profile="diagnostic"),
                )
            )
            await entered.wait()
            self.assertEqual((await worker.cancel("job"))["status"], "cancelled")
            resumed.set()
            await task
        spawn.assert_not_called()
        self.assertEqual(worker.get_job("job")["status"], "cancelled")

    async def test_terminal_job_cannot_be_overwritten_by_late_result(self):
        self.insert("running")
        worker.update_job("job", "cancelled", error="cancelled")
        worker.update_job("job", "success", result={"late": True})
        self.assertEqual(worker.get_job("job")["status"], "cancelled")

    async def test_reservation_and_jobs_are_mutually_exclusive(self):
        reservation = await worker.reserve_inference()
        with self.assertRaises(HTTPException):
            await worker.reserve_inference()
        with self.assertRaises(HTTPException):
            await worker.submit(
                worker.JobSpec(key="next", kind="diagnostic", profile="diagnostic")
            )
        await worker.release_inference(reservation["id"])
        self.insert("running")
        with self.assertRaises(HTTPException):
            await worker.reserve_inference()

    async def test_preflight_honors_container_ram_and_gpu(self):
        facts = {
            "gpus": [{"free_mib": 100}],
            "ram_available_mib": 100,
            "disk_free_mib": 100,
            "training_packages": {},
        }
        with patch.object(worker, "resources", return_value=facts):
            report = worker.preflight(
                worker.JobSpec(key="k", kind="diagnostic", profile="diagnostic")
            )
        self.assertFalse(report["ready"])
        self.assertTrue(any("GPU" in b for b in report["blockers"]))
        self.assertTrue(any("RAM" in b for b in report["blockers"]))

    async def test_fresh_artifact_directory_can_record_preflight_rejection(self):
        self.insert()
        with patch.object(worker, "ROOT", Path(self.temp.name) / "new-state"), patch.object(
            worker, "preflight", return_value={"ready": False, "blockers": ["GPU unavailable"]}
        ):
            await worker.execute("job", worker.JobSpec(key="key", kind="diagnostic", profile="diagnostic"))
        self.assertEqual(worker.get_job("job")["status"], "rejected")

    async def test_expired_inference_reservation_recovers(self):
        with self.database() as db:
            db.execute(
                "INSERT INTO executor_reservations VALUES ('test-worker','old',?)",
                (time.time() - 1,),
            )
        reservation = await worker.reserve_inference()
        self.assertNotEqual(reservation["id"], "old")

    async def test_unquantized_8b_weights_exceed_12gb_budget(self):
        recipe = SimpleNamespace(
            task="llm_sft", method="lora", batch_size=1, max_seq_length=1024
        )
        self.assertGreater(worker.estimated_gpu_mib(recipe, 8_000_000_000), 12288)
        recipe.method = "qlora"
        self.assertLess(worker.estimated_gpu_mib(recipe, 8_000_000_000), 12288)

    async def test_training_uses_hash_verified_dataset_snapshot(self):
        root = Path(self.temp.name)
        source = root / "dataset"
        source.mkdir()
        data = b'{"test":1}\n'
        (source / "train.jsonl").write_bytes(data)
        spec = worker.JobSpec(
            key="k",
            kind="llm_sft",
            profile="qlora_8b",
            payload={
                "recipe": {"storage": None},
                "datasets": [
                    {"name": "train.jsonl", "sha256": hashlib.sha256(data).hexdigest()}
                ]
            },
        )
        folder = root / "job"
        folder.mkdir()
        with patch.dict("os.environ", {"MODEL_WORKER_DATA_DIR": str(source)}):
            worker.snapshot_training_data(spec, folder)
            (source / "train.jsonl").write_text("changed")
            self.assertEqual((folder / "inputs/train.jsonl").read_bytes(), data)
            with self.assertRaises(ValueError):
                worker.snapshot_training_data(spec, folder)


if __name__ == "__main__":
    unittest.main(verbosity=2)
