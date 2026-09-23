"""네트워크/GPU 없이 Workbench 인증, 영속성, 검색 지표, 학습 계약을 검증한다."""

import asyncio
import json
import math
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(
    0, os.environ.get("AIRFLOW__CORE__PLUGINS_FOLDER", "/opt/airflow/plugins")
)

from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.requests import Request

from airflow_workbench.app import access, app
from airflow_workbench.models import ModelError, rank_embeddings, run_generation
from airflow_workbench.schemas import (
    ChatExperiment,
    EmbeddingExperiment,
    GenerationOptions,
    TrainingLaunch,
    TrainingRecipe,
)
from airflow_workbench.store import Conflict, Store
from airflow_workbench.training import inspect_recipe
from workbench_test_db import isolated_database
from airflow_workbench import environments


class WorkbenchTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        self.environment = environments.save(
            environments.Environment(
                id="test",
                name="Test remote executor",
                connection_id="test_executor",
                dag_prefix="test_model_lab",
                pool="model_lab_gpu",
            )
        )
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = patch.dict(
            os.environ,
            {
                "AIRFLOW_WORKBENCH_DATA_DIR": str(self.root),
                "AIRFLOW_WORKBENCH_TRAINING_DATA_DIR": str(self.root),
                "AIRFLOW_WORKBENCH_LLM_TRAIN_DAG": "",
                "AIRFLOW_WORKBENCH_REQUIRE_WORKER": "false",
            },
        )
        self.env.start()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.env.stop)

        async def executor_response(method, path, payload=None, **kwargs):
            if path == "/preflight" and payload.get("payload", {}).get("recipe"):
                report = inspect_recipe(
                    TrainingRecipe.model_validate(payload["payload"]["recipe"])
                )
                blockers = [b for b in report["blockers"] if "GPU 학습 DAG" not in b]
                return {
                    "ready": not blockers,
                    "blockers": blockers,
                    "datasets": report["datasets"],
                }
            return {"ready": True, "blockers": []}

        worker = patch(
            "airflow_workbench.worker_client.call",
            AsyncMock(side_effect=executor_response),
        )
        worker.start()
        self.addCleanup(worker.stop)

    def recipe(self, **changes):
        return TrainingRecipe(
            base_model="example/base-model",
            revision="a" * 40,
            train_dataset="train.jsonl",
            validation_dataset="validation.jsonl",
            **changes
        )

    def write_data(self, name, split, family, content="answer"):
        (self.root / name).write_text(
            json.dumps(
                {
                    "split": split,
                    "family_id": family,
                    "messages": [
                        {"role": "user", "content": family},
                        {"role": "assistant", "content": content},
                    ],
                }
            ),
            encoding="utf-8",
        )

    def test_optimistic_dashboard_save_survives_new_store(self):
        first, second = Store(), Store()
        a, b = first.dashboard(), second.dashboard()
        a["title"] = "saved"
        self.assertEqual(first.save_dashboard(a)["version"], 1)
        with self.assertRaises(Conflict):
            second.save_dashboard(b)
        self.assertEqual(Store().dashboard()["title"], "saved")

    def test_gpu_lease_cross_store_and_recovery(self):
        first = Store()
        run = first.start_experiment("generation", {"name": "first"}, "user")
        with self.assertRaises(Conflict):
            Store().start_experiment("embedding", {"name": "second"}, "other")
        with first.connection() as db:
            db.execute("UPDATE leases SET expires=?", (time.time() - 1,))
        next_run = Store().start_experiment("embedding", {"name": "second"}, "other")
        self.assertEqual(first.experiment(run)["status"], "interrupted")
        first.finish_experiment(run, result={"late": True})
        with self.assertRaises(Conflict):
            first.start_experiment("generation", {"name": "third"}, "user")
        first.finish_experiment(next_run, result={"models": []})
        self.assertEqual(first.experiment(next_run)["status"], "success")

    def test_retrieval_metrics_known_ranking(self):
        result = rank_embeddings(
            [[1, 0], [1, 0], [0, 1], [-1, 0]], ["a", "b", "c"], [0, 2], 2
        )
        self.assertEqual([r["index"] for r in result["ranking"]], [0, 1, 2])
        self.assertEqual(result["metrics"]["recall_at_k"], 0.5)
        self.assertEqual(result["metrics"]["mrr_at_k"], 1)
        self.assertAlmostEqual(
            result["metrics"]["ndcg_at_k"], 1 / (1 + 1 / math.log2(3))
        )

    def test_unlabeled_retrieval_has_no_quality_score(self):
        self.assertIsNone(rank_embeddings([[1, 0], [0, 1]], ["a"], [], 1)["metrics"])

    def test_invalid_vectors_fail(self):
        for vectors in ([[1, 0], [0]], [[0, 0], [1, 1]], [[1, 0], [math.nan, 1]], []):
            with self.assertRaises(ModelError):
                rank_embeddings(vectors, ["a"], [], 1)

    def test_input_bounds_and_unknown_options(self):
        for kwargs in (
            {"temperature": 9},
            {"temperature": math.nan},
            {"command": "shell"},
        ):
            with self.assertRaises(ValidationError):
                GenerationOptions(**kwargs)
        with self.assertRaises(ValidationError):
            EmbeddingExperiment(
                models=["bge-m3"],
                query="q",
                documents=["a", "b"],
                relevant_indices=[3],
                k=2,
            )
        with self.assertRaises(ValidationError):
            ChatExperiment(models=["qwen3:8b", "qwen3:8b"], prompt="q")
        with self.assertRaises(ValidationError):
            TrainingRecipe(
                base_model="example/base-model",
                revision="a" * 40,
                train_dataset="../holdout.jsonl",
                validation_dataset="valid.jsonl",
            )
        with self.assertRaises(ValidationError):
            self.recipe(task="embedding_contrastive", method="qlora")

    def test_training_missing_dependencies_fail_closed(self):
        report = inspect_recipe(self.recipe())
        self.assertFalse(report["ready"])
        self.assertEqual(len(report["blockers"]), 1)

    def test_training_family_overlap_is_rejected(self):
        self.write_data("train.jsonl", "train", "same")
        self.write_data("validation.jsonl", "validation", "same", "different")
        with patch.dict(os.environ, {"AIRFLOW_WORKBENCH_LLM_TRAIN_DAG": "train_model"}):
            report = inspect_recipe(self.recipe())
        self.assertFalse(report["ready"])
        self.assertTrue(any("family_id" in b for b in report["blockers"]))

    def test_holdout_is_never_training_data(self):
        self.write_data("train.jsonl", "holdout", "a")
        self.write_data("validation.jsonl", "validation", "b")
        self.assertFalse(inspect_recipe(self.recipe())["ready"])

    def test_training_recipe_records_content_hashes(self):
        self.write_data("train.jsonl", "train", "a")
        self.write_data("validation.jsonl", "validation", "b")
        with patch.dict(os.environ, {"AIRFLOW_WORKBENCH_LLM_TRAIN_DAG": "train_model"}):
            report = inspect_recipe(self.recipe())
        self.assertTrue(report["ready"])
        self.assertEqual(len(report["datasets"]), 2)
        self.assertEqual(report["effective_batch_size"], 16)
        self.assertEqual(len(report["datasets"][0]["sha256"]), 64)

    def test_model_execution_records_digest_and_unloads(self):
        spec = ChatExperiment(models=["base", "tuned"], prompt="test")
        installed = [{"name": n, "digest": n + "-sha"} for n in spec.models]
        call = AsyncMock(
            return_value={
                "message": {"content": "result"},
                "eval_count": 4,
                "eval_duration": 1000000000,
            }
        )
        with patch(
            "airflow_workbench.models.installed_models",
            AsyncMock(return_value=installed),
        ), patch("airflow_workbench.models.ollama", call):
            result = asyncio.run(run_generation(spec))
        self.assertEqual(
            [r["digest"] for r in result["models"]], ["base-sha", "tuned-sha"]
        )
        self.assertEqual(call.await_count, 2)
        for args in call.await_args_list:
            self.assertEqual(args.args[1]["keep_alive"], 0)
            self.assertFalse(args.args[1]["stream"])
            self.assertEqual(args.args[1]["options"]["seed"], 42)

    def request(self, method, headers):
        return Request(
            {
                "type": "http",
                "method": method,
                "scheme": "http",
                "path": "/workbench/api/dashboard",
                "server": ("localhost", 8080),
                "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
            }
        )

    def test_csrf_and_readonly_roles(self):
        manager = Mock()
        manager.is_authorized_view.return_value = True
        manager.is_authorized_configuration.return_value = False
        manager.is_authorized_custom_view.return_value = False
        user = SimpleNamespace()
        with patch("airflow_workbench.shared.auth.get_auth_manager", return_value=manager):
            asyncio.run(access(self.request("GET", {}), user))
            for headers in (
                {},
                {"x-workbench-request": "1"},
                {"x-workbench-request": "1", "origin": "http://evil.test"},
            ):
                with self.assertRaises(HTTPException):
                    asyncio.run(access(self.request("PUT", headers), user))
            manager.is_authorized_configuration.return_value = True
            with self.assertRaises(HTTPException):
                asyncio.run(
                    access(
                        self.request(
                            "PUT",
                            {"origin": "http://evil.test", "x-workbench-request": "1"},
                        ),
                        user,
                    )
                )
            asyncio.run(
                access(
                    self.request(
                        "PUT",
                        {"origin": "http://localhost:8080", "x-workbench-request": "1"},
                    ),
                    user,
                )
            )

    def test_api_persistence_and_launch_revalidation(self):
        permission = patch("airflow_workbench.dashboard.api.can_edit", return_value=True)
        permission.start()
        self.addCleanup(permission.stop)
        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "tester"
        )
        self.addCleanup(app.dependency_overrides.clear)
        with TestClient(app) as client:
            dashboard = client.get("/api/dashboard").json()
            self.assertEqual(
                client.put("/api/dashboard", json=dashboard).status_code, 200
            )
            self.assertEqual(
                client.put("/api/dashboard", json=dashboard).status_code, 409
            )
            self.assertEqual(
                client.post(
                    "/api/experiments/generation",
                    json={
                        "models": ["qwen"],
                        "prompt": "q",
                        "options": {"num_ctx": 999999},
                    },
                ).status_code,
                422,
            )
            response = client.post(
                "/api/training/launch",
                json={
                    "recipe": self.recipe().model_dump(),
                    "request_id": "a" * 8
                    + "-"
                    + "a" * 4
                    + "-"
                    + "a" * 4
                    + "-"
                    + "a" * 4
                    + "-"
                    + "a" * 12,
                },
            )
            self.assertEqual(response.status_code, 409)
            self.assertEqual(client.get("/static/not-found.txt").status_code, 404)
            self.assertIn(
                "frame-ancestors 'self'",
                client.get("/dashboard").headers["content-security-policy"],
            )

    def test_training_launch_idempotency_and_content_change(self):
        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "tester"
        )
        self.addCleanup(app.dependency_overrides.clear)
        self.write_data("train.jsonl", "train", "a")
        self.write_data("validation.jsonl", "validation", "b")
        payload = {
            "recipe": self.recipe().model_dump(),
            "request_id": "12345678-1234-1234-1234-123456789abc",
        }
        with patch.dict(os.environ, {"AIRFLOW_WORKBENCH_LLM_TRAIN_DAG": "train_model"}):
            report = inspect_recipe(self.recipe())
            report["environment_fingerprint"] = self.environment.fingerprint()
            existing = {"state": "queued", "conf": {"workbench": report}}
            contract = patch(
                "airflow_workbench.model_lab.operations_api.dag_contract",
                AsyncMock(return_value={"is_paused": False, "blockers": []}),
            )
            contract.start()
            self.addCleanup(contract.stop)
            pool = patch(
                "airflow_workbench.model_lab.operations_api.pool_contract",
                AsyncMock(return_value={"slots": 1, "include_deferred": True}),
            )
            pool.start()
            self.addCleanup(pool.stop)
            with TestClient(app) as client:
                call = AsyncMock(side_effect=[HTTPException(409), existing])
                with patch("airflow_workbench.model_lab.operations_api.airflow_api", call):
                    response = client.post("/api/training/launch", json=payload)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(
                    response.json()["dag_run_id"], "workbench__" + payload["request_id"]
                )
                self.write_data("train.jsonl", "train", "a", "changed")
                call = AsyncMock(side_effect=[HTTPException(409), existing])
                with patch("airflow_workbench.model_lab.operations_api.airflow_api", call):
                    self.assertEqual(
                        client.post("/api/training/launch", json=payload).status_code,
                        409,
                    )

    def test_chunked_request_size_is_bounded(self):
        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "tester"
        )
        self.addCleanup(app.dependency_overrides.clear)
        with TestClient(app) as client:
            response = client.post(
                "/api/presets", content=iter([b"x" * 200000, b"y" * 100000])
            )
            self.assertEqual(response.status_code, 413)

    def test_uncertain_model_request_retains_gpu_lease(self):
        from airflow_workbench.models import ModelRequestInterrupted

        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "tester"
        )
        self.addCleanup(app.dependency_overrides.clear)
        runner = AsyncMock(side_effect=ModelRequestInterrupted("connection lost"))
        with TestClient(app) as client, patch(
            "airflow_workbench.model_lab.legacy_api.models.run_generation", runner
        ):
            response = client.post(
                "/api/experiments/generation",
                json={"models": ["qwen3:8b"], "prompt": "q"},
            )
            self.assertEqual(response.json()["status"], "failed")
            with self.assertRaises(Conflict):
                Store().start_experiment("embedding", {"name": "next"}, "tester")

    def test_paused_training_dag_is_not_triggered(self):
        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "tester"
        )
        self.addCleanup(app.dependency_overrides.clear)
        self.write_data("train.jsonl", "train", "a")
        self.write_data("validation.jsonl", "validation", "b")
        with patch.dict(
            os.environ, {"AIRFLOW_WORKBENCH_LLM_TRAIN_DAG": "train_model"}
        ), TestClient(app) as client:
            call = AsyncMock()
            with patch("airflow_workbench.model_lab.operations_api.airflow_api", call), patch(
                "airflow_workbench.model_lab.operations_api.dag_contract",
                AsyncMock(return_value={"is_paused": True, "blockers": []}),
            ), patch(
                "airflow_workbench.model_lab.operations_api.pool_contract",
                AsyncMock(return_value={"slots": 1, "include_deferred": True}),
            ):
                response = client.post(
                    "/api/training/launch",
                    json={
                        "recipe": self.recipe().model_dump(),
                        "request_id": "12345678-1234-1234-1234-123456789abc",
                    },
                )
            self.assertEqual(response.status_code, 409)
            self.assertEqual(call.await_count, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
