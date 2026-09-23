"""No GPU/cloud access: exercise dataset integrity and completion publication."""

import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import batch_entrypoint as batch
from airflow_workbench.schemas import TrainingRecipe


class MemoryStore:
    def __init__(self):
        self.values = {}
        self.events = []

    def exists(self, key):
        return key in self.values

    def open(self, key, mode):
        store = self

        class Stream(io.StringIO):
            def close(self):
                if mode == "w":
                    store.values[key] = self.getvalue()
                    store.events.append(key)
                super().close()

        return Stream(self.values.get(key, ""))

    def put_file(self, source, target):
        self.values[target] = Path(source).read_bytes()
        self.events.append(target)


class BatchTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.remote = MemoryStore()
        self.data = {}
        for split in ("train", "validation"):
            self.data["s3://data/" + split + ".jsonl"] = json.dumps(
                {
                    "split": split,
                    "family_id": split,
                    "messages": [
                        {"role": "user", "content": split},
                        {"role": "assistant", "content": "answer"},
                    ],
                }
            ).encode()
        self.recipe = TrainingRecipe(
            base_model="example/base-model",
            environment_id="cluster",
            revision="a" * 40,
            train_dataset="s3://data/train.jsonl",
            validation_dataset="s3://data/validation.jsonl",
            train_sha256=hashlib.sha256(self.data["s3://data/train.jsonl"]).hexdigest(),
            validation_sha256=hashlib.sha256(
                self.data["s3://data/validation.jsonl"]
            ).hexdigest(),
            artifact_uri="s3://models/model",
        )
        self.report = {"recipe": self.recipe.model_dump(), "recipe_sha256": "d" * 64}

        def fake_train(spec, folder):
            (folder / "model").mkdir(exist_ok=True)
            (folder / "model/adapter.json").write_text("artifact")
            (folder / "receipt.json").write_text(
                json.dumps(spec["payload"]["datasets"])
            )
            return {"metrics": {"eval_loss": 0.5}}

        self.train = Mock(side_effect=fake_train)
        modules = {
            "fsspec": SimpleNamespace(
                open=lambda uri, mode: io.BytesIO(self.data[uri]),
                core=SimpleNamespace(url_to_fs=lambda uri: (self.remote, uri)),
            ),
            "huggingface_hub": SimpleNamespace(snapshot_download=Mock()),
            "train": SimpleNamespace(train=self.train),
        }
        for context in (
            patch.dict(sys.modules, modules),
            patch.dict(os.environ, {"WORKBENCH_SCRATCH_DIR": str(self.root)}),
        ):
            context.start()
            self.addCleanup(context.stop)

    def run_batch(self):
        with patch.dict(
            os.environ,
            {
                "WORKBENCH_REQUEST": json.dumps(
                    {
                        "kind": "llm_sft",
                        "run_id": "manual__test",
                        "workbench": self.report,
                    }
                )
            },
        ):
            batch.main(output_path=self.root / "xcom.json")

    def test_checksum_failure_never_trains_or_publishes(self):
        self.data[self.recipe.train_dataset] = b"changed"
        with self.assertRaisesRegex(ValueError, "SHA256"):
            self.run_batch()
        self.train.assert_not_called()
        self.assertEqual(self.remote.events, [])

    def test_completion_manifest_is_last_and_retry_reuses_same_artifact(self):
        self.run_batch()
        self.assertTrue(self.remote.events[-1].endswith("/_SUCCESS.json"))
        receipt = json.loads((self.root / "xcom.json").read_text())
        self.assertTrue(receipt["training_performed"])
        self.assertTrue(receipt["artifact_uri"].startswith(self.recipe.artifact_uri))
        self.run_batch()
        self.train.assert_called_once()
        self.report["recipe_sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "different recipe"):
            self.run_batch()

    def test_partial_upload_does_not_publish_success(self):
        with patch.object(
            self.remote, "put_file", side_effect=OSError("upload failed")
        ), self.assertRaises(OSError):
            self.run_batch()
        self.assertFalse(
            any(key.endswith("_SUCCESS.json") for key in self.remote.values)
        )
        self.assertFalse((self.root / "xcom.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
