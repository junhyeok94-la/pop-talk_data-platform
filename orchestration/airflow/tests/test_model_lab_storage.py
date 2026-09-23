"""Storage isolation, immutable run paths, and external worker I/O routing."""

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import test_model_lab_workflow as workflow
from airflow_workbench import storage
from airflow_workbench.schemas import TrainingRecipe, TrainingStorage
from airflow_workbench.training import inspect_recipe
from airflow_workbench.environments import Environment, save


class StorageAPITest(unittest.TestCase):
    project = workflow.WorkflowTest.project
    post = workflow.WorkflowTest.post

    def setUp(self):
        workflow.WorkflowTest.setUp(self)
        save(
            Environment(
                id="gpu",
                name="GPU",
                connection_id="executor",
                dag_prefix="test_gpu",
                pool="test_gpu",
            )
        )
        self.profile = {
            "id": "training",
            "name": "Team storage",
            "environment_id": "gpu",
            "locations": {
                "mode": "mounted",
                "data_root": "/datasets/team",
                "model_root": "/models/team",
                "artifact_root": "/artifacts/team",
            },
        }

    def save_profile(self, **changes):
        response = self.client.put(
            "/api/lab/projects/alpha/storage/training", json={**self.profile, **changes}
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def recipe(self, profile):
        return TrainingRecipe(
            environment_id="gpu",
            base_model="org/base",
            revision="a" * 40,
            train_dataset="train.jsonl",
            validation_dataset="validation.jsonl",
            storage=profile["locations"],
        ).model_dump()

    def test_profile_scope_roles_and_optimistic_edit(self):
        first = self.save_profile()
        self.assertEqual(first["locations"]["profile_version"], 1)
        conflict = self.client.put(
            "/api/lab/projects/alpha/storage/training", json=self.profile
        )
        self.assertEqual(conflict.status_code, 409)
        self.user = SimpleNamespace(get_id=lambda: "bob", is_admin=False)
        self.assertEqual(
            self.client.get("/api/lab/projects/alpha/storage").status_code, 403
        )
        self.store.patch(
            "_global", "project", "alpha", {"members": {"bob": "developer"}}
        )
        self.assertEqual(
            self.client.get("/api/lab/projects/alpha/storage").status_code, 200
        )
        self.assertEqual(
            self.client.put(
                "/api/lab/projects/alpha/storage/training",
                json={**self.profile, "version": 1},
            ).status_code,
            403,
        )

    def test_stale_or_tampered_storage_never_reaches_worker(self):
        first = self.save_profile()
        recipe = self.recipe(first)
        self.save_profile(
            version=1,
            locations={**self.profile["locations"], "data_root": "/datasets/new"},
        )
        with patch("airflow_workbench.lab_api.readiness", AsyncMock()) as remote:
            response = self.client.post(
                "/api/lab/projects/alpha/training/validate", json=recipe
            )
        self.assertEqual(response.status_code, 409)
        remote.assert_not_called()

    def test_check_uses_selected_connection_and_does_not_revise_profile(self):
        self.save_profile()
        with patch(
            "airflow_workbench.worker_client.call",
            AsyncMock(return_value={"ready": True, "checks": {}, "blockers": []}),
        ) as remote:
            report = self.post("/projects/alpha/storage/training/check", {})
        self.assertTrue(report["ready"])
        self.assertEqual(remote.await_args.kwargs["connection_id"], "executor")
        self.assertEqual(remote.await_args.args[:2], ("POST", "/storage/check"))
        self.assertEqual(self.store.get("alpha", "storage", "training")["version"], 1)

    def test_submitted_run_keeps_storage_snapshot_after_profile_edit(self):
        import uuid

        first = self.save_profile()
        recipe = self.recipe(first)
        report = {
            "ready": True,
            "recipe": recipe,
            "datasets": [],
            "environment_fingerprint": "env",
            "dag_id": "test_gpu_llm_train",
        }
        body = {
            "request_id": str(uuid.uuid4()),
            "experiment_id": self.experiment["id"],
            "recipe": recipe,
        }
        with patch(
            "airflow_workbench.lab_api.readiness", AsyncMock(return_value=report)
        ), patch(
            "airflow_workbench.lab_api.airflow_api",
            AsyncMock(return_value={"state": "queued"}),
        ):
            run = self.post("/projects/alpha/train", body)
        self.save_profile(
            version=1,
            locations={**self.profile["locations"], "artifact_root": "/artifacts/new"},
        )
        saved = self.store.get("alpha", "run", run["id"])
        self.assertEqual(saved["config"]["storage"], first["locations"])
        self.assertEqual(
            saved["dag_conf"]["workbench"]["recipe"]["storage"], first["locations"]
        )
        with patch("airflow_workbench.lab_api.readiness", AsyncMock()) as remote:
            repeated = self.post("/projects/alpha/train", body)
        self.assertEqual(repeated["id"], run["id"])
        remote.assert_not_called()


class MountedStorageTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.locations = {
            k: str(self.root / name)
            for k, name in [
                ("data_root", "data"),
                ("model_root", "weights"),
                ("artifact_root", "results"),
            ]
        }
        for value in self.locations.values():
            Path(value).mkdir()
        env = patch.dict(
            os.environ,
            {
                "MODEL_WORKER_STORAGE_ROOTS": json.dumps(
                    {k: [v] for k, v in self.locations.items()}
                )
            },
        )
        env.start()
        self.addCleanup(env.stop)
        self.recipe = TrainingRecipe(
            base_model="org/base",
            revision="a" * 40,
            train_dataset="train.jsonl",
            validation_dataset="validation.jsonl",
            storage=TrainingStorage(**self.locations),
        )
        for split in ("train", "validation"):
            value = {
                "split": split,
                "family_id": split,
                "messages": [
                    {"role": "user", "content": split},
                    {"role": "assistant", "content": "answer"},
                ],
            }
            (Path(self.locations["data_root"]) / (split + ".jsonl")).write_text(
                json.dumps(value)
            )

    def test_real_directory_probe_and_independent_dataset_roots(self):
        report = storage.inspect_storage(self.recipe.storage)
        self.assertTrue(report["ready"], report)
        self.assertEqual(list(Path(self.locations["artifact_root"]).iterdir()), [])
        with patch(
            "airflow_workbench.training.training_root",
            side_effect=AssertionError("global path used"),
        ):
            data = inspect_recipe(self.recipe)
        self.assertTrue(data["ready"], data)
        self.assertEqual([r["rows"] for r in data["datasets"]], [1, 1])

    def test_unapproved_path_and_symlink_escape_rejected(self):
        for value in ("/etc", str(self.root / "weights-other")):
            with self.assertRaises(ValueError):
                storage.resolve_locations({**self.locations, "model_root": value})
        outside = self.root / "outside"
        outside.mkdir()
        (Path(self.locations["data_root"]) / "escape").symlink_to(
            outside, target_is_directory=True
        )
        with self.assertRaises(ValueError):
            storage.resolve_locations(
                {**self.locations, "data_root": self.locations["data_root"] + "/escape"}
            )

    def test_locations_reject_credentials_traversal_and_source_overwrite(self):
        for invalid in (
            "/data/../secret",
            "D:\\models",
            "/",
            "/models/%2e%2e/etc",
            "/data?token=secret",
        ):
            with self.assertRaises(ValueError):
                TrainingStorage(**{**self.locations, "data_root": invalid})
        with self.assertRaises(ValueError):
            TrainingStorage(
                **{
                    **self.locations,
                    "artifact_root": self.locations["data_root"] + "/results",
                }
            )
        with self.assertRaises(ValueError):
            TrainingStorage(
                mode="object",
                data_root="s3://key:secret@bucket/data",
                artifact_root="s3://bucket/models",
            )

    def test_worker_snapshots_selected_data_and_detects_changes(self):
        import worker

        report = inspect_recipe(self.recipe)
        job = worker.JobSpec(
            key="test", kind="llm_sft", profile="qlora_8b", payload=report
        )
        folder = self.root / "job"
        folder.mkdir()
        worker.snapshot_training_data(job, folder)
        self.assertEqual(
            hashlib.sha256((folder / "inputs/train.jsonl").read_bytes()).hexdigest(),
            report["datasets"][0]["sha256"],
        )
        (Path(self.locations["data_root"]) / "train.jsonl").write_text("changed")
        with self.assertRaisesRegex(ValueError, "changed"):
            worker.snapshot_training_data(job, folder)

    def test_worker_preflight_reads_selected_model_and_output_capacity(self):
        import worker

        model = storage.model_directory(self.recipe)
        model.mkdir(parents=True)
        (model / "config.json").write_text("{}")
        header = json.dumps({"weight": {"shape": [32, 32]}}).encode()
        (model / "model.safetensors").write_bytes(
            len(header).to_bytes(8, "little") + header
        )
        report = inspect_recipe(self.recipe)
        facts = {
            "gpus": [{"free_mib": 32000}],
            "ram_available_mib": 32000,
            "disk_free_mib": 32000,
            "training_packages": dict.fromkeys(
                ["torch", "transformers", "peft", "sentence_transformers"], True
            ),
        }
        spec = worker.JobSpec(
            key="test", kind="llm_sft", profile="qlora_8b", payload=report
        )
        with patch.object(worker, "resources", return_value=facts), patch(
            "airflow_workbench.storage.shutil.disk_usage",
            return_value=SimpleNamespace(free=32000 * 1048576),
        ):
            actual = worker.preflight(spec)
        self.assertTrue(actual["ready"], actual)
        output = storage.artifact_directory(self.recipe, "run-1")
        self.assertEqual(output, Path(self.locations["artifact_root"]) / "run-1")
        with self.assertRaises(ValueError):
            storage.artifact_directory(self.recipe, "../escape")

    def test_object_recipe_stays_in_selected_prefix(self):
        value = self.recipe.model_dump()
        value.update(
            storage={
                "mode": "object",
                "data_root": "s3://bucket/team/data",
                "artifact_root": "s3://bucket/team/models",
            },
            train_dataset="s3://bucket/team/data/train.jsonl",
            validation_dataset="s3://bucket/team/data/val.jsonl",
            artifact_uri="s3://bucket/team/models",
        )
        self.assertEqual(TrainingRecipe.model_validate(value).storage.mode, "object")
        with self.assertRaises(ValueError):
            TrainingRecipe.model_validate(
                {**value, "train_dataset": "s3://bucket/team/data-other/train.jsonl"}
            )

    def test_trainer_places_model_metrics_and_receipt_in_selected_output(self):
        import sys
        from unittest.mock import Mock
        from train import train
        import worker

        # Exercise the real adapter's file routing with a CPU-only trainer double.
        folder = self.root / "job-routing"
        folder.mkdir()
        report = inspect_recipe(self.recipe)
        worker.snapshot_training_data(
            worker.JobSpec(
                key="test", kind="llm_sft", profile="qlora_8b", payload=report
            ),
            folder,
        )

        def save_model(path):
            path.mkdir(exist_ok=True)
            (path / "adapter.safetensors").write_bytes(b"fixture")

        model = SimpleNamespace(
            config=SimpleNamespace(use_cache=True), save_pretrained=save_model
        )
        tokenizer = SimpleNamespace(
            pad_token_id=0,
            apply_chat_template=lambda messages, **kw: (
                [1] if len(messages) == 1 else [1, 2]
            ),
            save_pretrained=lambda path: (path / "tokenizer.json").write_text("{}"),
        )
        model_loader = Mock(return_value=model)

        def trainer(**kwargs):
            callback = kwargs["callbacks"][0]
            return SimpleNamespace(
                train=lambda: callback.on_log(
                    None, SimpleNamespace(global_step=1), None, {"loss": 1}
                ),
                evaluate=lambda: {"eval_loss": 1},
            )

        transformers = SimpleNamespace(
            AutoModelForCausalLM=SimpleNamespace(from_pretrained=model_loader),
            AutoTokenizer=SimpleNamespace(from_pretrained=lambda *a, **kw: tokenizer),
            BitsAndBytesConfig=lambda **kw: kw,
            DataCollatorForSeq2Seq=lambda *a, **kw: None,
            Trainer=trainer,
            TrainingArguments=lambda **kw: kw,
            TrainerCallback=object,
            set_seed=lambda seed: None,
        )
        peft = SimpleNamespace(
            LoraConfig=lambda **kw: kw,
            get_peft_model=lambda model, config: model,
            prepare_model_for_kbit_training=lambda model: model,
        )
        with patch.dict(
            sys.modules,
            {
                "torch": SimpleNamespace(
                    float16="float16",
                    cuda=SimpleNamespace(max_memory_allocated=lambda: 1024),
                ),
                "transformers": transformers,
                "peft": peft,
            },
        ):
            result = train({"payload": report}, folder)
        target = Path(self.locations["artifact_root"]) / folder.name
        for name in (
            "model/adapter.safetensors",
            "model/tokenizer.json",
            "receipt.json",
            "metrics.jsonl",
        ):
            self.assertTrue((target / name).is_file(), name)
        self.assertFalse((folder / "model").exists())
        self.assertEqual(result["artifact_uri"], target.as_uri())
        self.assertEqual(
            model_loader.call_args.args[0], storage.model_directory(self.recipe)
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
