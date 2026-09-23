"""cohort 입력/출력 고정과 실제 dbt parse를 검증한다. 외부 DB SQL은 실행하지 않는다."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from pipelines.orchestration.dbt_relation_bindings import build_relation_vars


class RelationBindingTests(unittest.TestCase):
    def test_reused_parent_keeps_original_relation(self):
        cohort = {"model_unique_id": "model.pop_talk_dw.dim_movie", "parent_bindings": [
            {"parent_unique_id": "model.pop_talk_dw.stg_movie_observations",
             "relation_id": "POP_TALK_DW_DEV.CANDIDATE.OLD_PARENT"}
        ]}
        result = build_relation_vars(cohort, "POP_TALK_DW_DEV.CANDIDATE.NEW_CHILD")
        self.assertEqual(result["pop_talk_relations"]["model.pop_talk_dw.stg_movie_observations"]["identifier"], "OLD_PARENT")

    def test_s3_manifest_is_not_used_as_sql_table(self):
        cohort = {"model_unique_id": "model.pop_talk_dw.root", "parent_bindings": [
            {"parent_unique_id": "source.pop_talk_dw.raw.movies", "relation_id": "s3://bucket/ready.json"}
        ]}
        with self.assertRaisesRegex(ValueError, "snapshot"):
            build_relation_vars(cohort, "POP_TALK_DW_DEV.CANDIDATE.OUTPUT")

    @unittest.skipUnless(Path("/opt/dbt-venv/bin/dbt").exists(), "dbt 실행 환경 필요")
    def test_dbt_parse_binds_both_output_and_parent_without_changing_graph(self):
        source = Path("/opt/airflow/modules/pipelines/dbt")
        bindings = build_relation_vars({"model_unique_id": "model.pop_talk_dw.dim_movie", "parent_bindings": [
            {"parent_unique_id": "model.pop_talk_dw.stg_movie_observations", "relation_id": "POP_TALK_DW_DEV.CANDIDATE.PARENT"}
        ]}, "POP_TALK_DW_DEV.CANDIDATE.OUTPUT")
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "project"
            shutil.copytree(source, project, ignore=shutil.ignore_patterns("target", "logs", "__pycache__"))
            result = subprocess.run([
                "/opt/dbt-venv/bin/dbt", "parse", "--no-partial-parse",
                "--project-dir", str(project), "--profiles-dir", str(project),
                "--vars", json.dumps(bindings),
            ], capture_output=True, text=True, timeout=60, cwd=directory,
                env={key: value for key, value in os.environ.items() if key != "PYTHONPATH"})
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            manifest = json.loads((project / "target/manifest.json").read_text())
            nodes = manifest["nodes"]
            self.assertEqual(nodes["model.pop_talk_dw.dim_movie"]["alias"], "OUTPUT")
            self.assertEqual(nodes["model.pop_talk_dw.dim_movie"]["schema"], "CANDIDATE")
            self.assertEqual(nodes["model.pop_talk_dw.stg_movie_observations"]["alias"], "PARENT")
            self.assertIn("model.pop_talk_dw.stg_movie_observations", nodes["model.pop_talk_dw.dim_movie"]["depends_on"]["nodes"])
            self.assertEqual(sum(n["resource_type"] == "test" for n in nodes.values()), 43)
