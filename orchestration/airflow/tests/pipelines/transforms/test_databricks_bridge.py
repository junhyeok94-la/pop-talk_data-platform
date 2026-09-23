import unittest
import io
import json
import zipfile
from unittest.mock import patch

from pipelines.transforms.databricks_bridge import (
    BridgeError, DatabricksFilesClient, require_same_artifact, stage_daily_bundle,
)


class Response:
    def __init__(self, status, content=b""):
        self.status_code, self.content = status, content


class Session:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


class FilesClientTests(unittest.TestCase):
    def test_source_change_after_deploy_fails_closed(self):
        require_same_artifact("a" * 64, "a" * 64)
        with self.assertRaisesRegex(BridgeError, "changed after notebook deployment"):
            require_same_artifact("a" * 64, "b" * 64)

    def test_existing_identical_file_is_reused_without_put(self):
        session = Session([Response(200, b"same")])
        client = DatabricksFilesClient("https://workspace", "secret", session=session)
        client.put_immutable("/Volumes/a/same.json", b"same")
        self.assertEqual([call[0] for call in session.calls], ["GET"])

    def test_existing_different_file_fails_closed(self):
        client = DatabricksFilesClient("https://workspace", "secret",
                                       session=Session([Response(200, b"different")]))
        with self.assertRaisesRegex(BridgeError, "Immutable"):
            client.put_immutable("/Volumes/a/file.json", b"same")

    def test_missing_file_is_uploaded_without_overwrite(self):
        session = Session([Response(404), Response(204)])
        client = DatabricksFilesClient("https://workspace", "secret", session=session)
        client.put_immutable("/Volumes/a/file.json", b"body")
        self.assertEqual([call[0] for call in session.calls], ["GET", "PUT"])
        self.assertEqual(session.calls[1][2]["params"], {"overwrite": "false"})
        self.assertEqual(session.calls[1][2]["data"], b"body")

    def test_stage_bundle_uploads_one_deterministic_archive(self):
        class Body:
            def __init__(self, value): self.value = value
            def read(self): return self.value

        class S3:
            def __init__(self, values): self.values = values
            def get_object(self, Bucket, Key): return {"Body": Body(self.values[Key])}

        class Files:
            def __init__(self): self.directories, self.uploads = [], []
            def mkdir(self, path): self.directories.append(path)
            def put_immutable(self, path, body): self.uploads.append((path, body))

        ready = {"run_id": "run-1", "raw_success_manifest": "success.json"}
        success = {"stage_manifests": {name: f"{name}.json" for name in
                                       ("movie_list", "details", "kmdb_candidates", "boxoffice")}}
        values = {"ready.json": json.dumps(ready).encode(),
                  "success.json": json.dumps(success).encode()}
        values.update({f"{name}.json": json.dumps({"objects": []}).encode()
                       for name in success["stage_manifests"]})
        files = Files()
        output = {"bronze": [], "silver_movies": [], "silver_boxoffice": []}
        with patch("pipelines.transforms.databricks_bridge.transform_run", return_value=output):
            result = stage_daily_bundle(S3(values), files, bucket="bucket",
                                        ready_key="ready.json", transform_source=b"source",
                                        artifact_version="a" * 64)
        self.assertEqual(result.index_path, result.landing_dir + "/bundle.zip")
        self.assertEqual(len(files.uploads), 1)
        archive = zipfile.ZipFile(io.BytesIO(files.uploads[0][1]))
        self.assertIn("bundle_index.json", archive.namelist())
        self.assertIn("movie_bronze_silver.py", archive.namelist())
        self.assertIn("silver_movies.jsonl", archive.namelist())
        self.assertIn("silver_boxoffice.jsonl", archive.namelist())

    def test_processing_artifacts_use_separate_immutable_paths(self):
        class Body:
            def __init__(self, value): self.value = value
            def read(self): return self.value

        class S3:
            def __init__(self, values): self.values = values
            def get_object(self, Bucket, Key): return {"Body": Body(self.values[Key])}

        class Files:
            def __init__(self): self.uploads = []
            def mkdir(self, path): pass
            def put_immutable(self, path, body): self.uploads.append((path, body))

        ready = {"run_id": "run-1", "raw_success_manifest": "success.json"}
        success = {"stage_manifests": {name: f"{name}.json" for name in
                                       ("movie_list", "details", "kmdb_candidates", "boxoffice")}}
        values = {"ready.json": json.dumps(ready).encode(),
                  "success.json": json.dumps(success).encode()}
        values.update({f"{name}.json": json.dumps({"objects": []}).encode()
                       for name in success["stage_manifests"]})
        files = Files()
        output = {"bronze": [], "silver_movies": [], "silver_boxoffice": []}

        with patch("pipelines.transforms.databricks_bridge.transform_run", return_value=output):
            first = stage_daily_bundle(S3(values), files, bucket="bucket",
                                       ready_key="ready.json", transform_source=b"v1",
                                       artifact_version="a" * 64)
            second = stage_daily_bundle(S3(values), files, bucket="bucket",
                                        ready_key="ready.json", transform_source=b"v2",
                                        artifact_version="b" * 64)

        self.assertNotEqual(first.index_path, second.index_path)
        self.assertIn("/" + "a" * 64 + "/", first.index_path)
        self.assertIn("/" + "b" * 64 + "/", second.index_path)


if __name__ == "__main__":
    unittest.main()
