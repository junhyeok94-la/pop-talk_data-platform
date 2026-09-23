"""통합 DW의 격리 입력, 실행 실패, 빠진 품질 검사가 서빙을 통과하지 못함을 검증한다."""
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipelines.orchestration import dw_pipeline as runtime


class DwPipelineTests(unittest.TestCase):
    def test_separate_runs_bind_all_models_and_sources_to_separate_relations(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (datetime(2026, 9, 11, tzinfo=timezone.utc), "s3/ready.json")
        models = tuple("model.pop_talk_dw." + name for names in runtime.MODEL_GROUPS.values() for name in names)
        with patch.object(runtime, "deployment_for", return_value=SimpleNamespace(model_unique_ids=models)):
            first = runtime.prepare_inputs(connection, {}, "run-a")
            second = runtime.prepare_inputs(connection, {}, "run-b")
        self.assertEqual(len(first["bindings"]), 10)
        self.assertNotEqual(first["token"], second["token"])
        self.assertTrue(all(first["bindings"][name] != second["bindings"][name] for name in models))
        clones = [call for call in cursor.execute.call_args_list if " CLONE " in call.args[0]]
        self.assertEqual(len(clones), 6)
        self.assertEqual({call.args[1] for call in clones}, {(first["cutoff"],)})

    def run_checks(self, results, returncode=0):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            project = root / "source"
            project.mkdir()
            deployment = SimpleNamespace(project_path=project, test_unique_ids=("test.a", "test.b"))
            def execute(command, **kwargs):
                target = Path(command[command.index("--target-path") + 1])
                target.mkdir(parents=True, exist_ok=True)
                (target / "run_results.json").write_text(json.dumps({"results": results}))
                self.assertNotIn("PYTHONPATH", kwargs["env"])
                return SimpleNamespace(returncode=returncode)
            with patch.object(runtime, "ARTIFACT_ROOT", root / "artifacts"), \
                 patch.object(runtime, "deployment_for", return_value=deployment), \
                 patch.object(runtime.subprocess, "run", side_effect=execute):
                return runtime.run_dbt({"identity": {}, "token": "run", "bindings": {}}, "test")

    def test_all_tests_must_pass(self):
        self.assertEqual(self.run_checks([{"unique_id": name, "status": "pass"} for name in ("test.a", "test.b")])["passed"], 2)

    def test_missing_test_is_failure_even_when_process_succeeds(self):
        with self.assertRaisesRegex(RuntimeError, "예상 모델/검사"):
            self.run_checks([{"unique_id": "test.a", "status": "pass"}])

    def test_failed_or_skipped_test_is_not_accepted(self):
        for status in ("fail", "skipped", "warn"):
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                self.run_checks([{"unique_id": "test.a", "status": "pass"}, {"unique_id": "test.b", "status": status}])

    def test_failed_process_is_not_accepted_even_with_success_artifact(self):
        with self.assertRaisesRegex(RuntimeError, "dbt test 실패"):
            self.run_checks([{"unique_id": name, "status": "pass"} for name in ("test.a", "test.b")], returncode=1)


if __name__ == "__main__":
    unittest.main()
