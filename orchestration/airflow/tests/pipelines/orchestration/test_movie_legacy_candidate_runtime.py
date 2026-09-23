"""Legacy candidate artifact와 원격 재실행 식별자를 Airflow 없이 검증한다."""
from __future__ import annotations

import base64
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipelines.orchestration.movie_legacy_candidate_runtime import (
    ARTIFACT_DEPENDENCIES,
    artifact_sources,
    artifact_version,
    deploy_notebook,
    load_snowflake,
    stage_bundle,
)
from pipelines.transforms.legacy_candidate_bridge import LegacyCandidateBundle


class Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def artifact_root():
    directory = tempfile.TemporaryDirectory()
    root = Path(directory.name)
    for number, relative in enumerate(ARTIFACT_DEPENDENCIES):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"source-{number}".encode())
    return directory, root


class CandidateArtifactTests(unittest.TestCase):
    def test_notebook_checks_archive_and_lineage_before_zip_and_exec(self):
        project_root = (Path(__file__).resolve().parents[3] / "modules")
        source = (
            project_root / "pipelines/databricks/04_movie_legacy_candidate.py"
        ).read_text(encoding="utf-8")
        archive_check = source.index("hashlib.sha256(archive_body).hexdigest()")
        zip_open = source.index("zipfile.ZipFile(io.BytesIO(archive_body))")
        dynamic_exec = source.index("exec(")
        self.assertLess(archive_check, zip_open)
        self.assertLess(zip_open, dynamic_exec)
        self.assertIn('landing_dir != expected_landing', source)
        self.assertIn('index.get("source_run_id") != f"legacy:{source_sha256}"', source)

    def test_every_declared_dependency_changes_artifact(self):
        directory, root = artifact_root()
        with directory:
            baseline = artifact_version(artifact_sources(root))
            for relative in ARTIFACT_DEPENDENCIES:
                path = root / relative
                original = path.read_bytes()
                path.write_bytes(original + b"-changed")
                self.assertNotEqual(baseline, artifact_version(artifact_sources(root)))
                path.write_bytes(original)

    @patch("pipelines.orchestration.movie_legacy_candidate_runtime.requests.get")
    @patch("pipelines.orchestration.movie_legacy_candidate_runtime.requests.post")
    def test_existing_digest_notebook_is_reused_only_for_identical_bytes(self, post, get):
        directory, root = artifact_root()
        with directory:
            notebook = (root / "pipelines/databricks/04_movie_legacy_candidate.py").read_bytes()
            post.return_value = Response(200)
            get.side_effect = [
                Response(200),
                Response(200, {"content": base64.b64encode(notebook).decode("ascii")}),
            ]
            result = deploy_notebook(
                project_root=root, host="https://example", token="secret",
                notebook_directory="/Shared/candidate",
            )
            self.assertTrue(result["notebook_path"].endswith(result["artifact_version"]))

            get.side_effect = [
                Response(200),
                Response(200, {"content": base64.b64encode(b"different").decode("ascii")}),
            ]
            with self.assertRaisesRegex(RuntimeError, "content conflict"):
                deploy_notebook(
                    project_root=root, host="https://example", token="secret",
                    notebook_directory="/Shared/candidate",
                )

    @patch("pipelines.transforms.legacy_candidate_bridge.stage_legacy_candidate_bundle")
    @patch("pipelines.transforms.databricks_bridge.DatabricksFilesClient")
    @patch("pipelines.orchestration.movie_legacy_candidate_runtime.requests.Session")
    def test_same_logical_input_has_same_submit_token_across_calls(
        self, session, files_client, stage_candidate
    ):
        directory, root = artifact_root()
        with directory:
            version = artifact_version(artifact_sources(root))
            stage_candidate.return_value = LegacyCandidateBundle(
                source_run_id="legacy:" + "b" * 64,
                source_sha256="b" * 64,
                manifest_key="manifest",
                landing_dir="/Volumes/workspace/bronze/landing/movie_legacy_candidate/x/y",
                archive_path="/Volumes/workspace/bronze/landing/movie_legacy_candidate/x/y/bundle.zip",
                archive_sha256="c" * 64,
                movie_count=5985,
                eligible_count=4320,
                excluded_count=1665,
            )
            session.return_value.__enter__.return_value = MagicMock()
            arguments = dict(
                project_root=root,
                s3_client=MagicMock(),
                databricks_host="https://example",
                databricks_token="secret",
                bucket="bucket",
                manifest_key="manifest",
                publication_revision=1,
                processing_attempt=1,
                deployed={"notebook_path": "/n", "artifact_version": version},
            )
            first = stage_bundle(**arguments)
            second = stage_bundle(**arguments)
            self.assertEqual(first["submit_token"], second["submit_token"])
            self.assertNotIn("secret", first.values())

    def test_load_rejects_changed_artifact_before_external_io(self):
        directory, root = artifact_root()
        with directory:
            s3 = MagicMock()
            connection = MagicMock()
            with self.assertRaisesRegex(RuntimeError, "artifact changed"):
                load_snowflake(
                    project_root=root,
                    s3_client=s3,
                    snowflake_connection=connection,
                    bucket="bucket",
                    published={
                        "prefix": "candidate",
                        "ready_key": "candidate/EXCHANGE_READY.json",
                        "source_run_id": "legacy:" + "b" * 64,
                        "source_sha256": "b" * 64,
                        "artifact_version": "0" * 64,
                        "publication_revision": 1,
                        "movie_count": 5985,
                        "boxoffice_count": 0,
                    },
                )
            s3.get_object.assert_not_called()
            connection.cursor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
