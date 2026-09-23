"""Cosmos dbt 실행 wrapper가 graph/source/명령을 섞지 않는지 검증한다."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from scripts.dbt_cosmos_runner import _require_same_identity, prepare_invocation


class DbtCosmosRunnerTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[SimpleNamespace, Path]:
        source = root / "deployment" / "project"
        source.mkdir(parents=True)
        (source / "dbt_project.yml").write_text(
            "name: pop_talk_dw\n", encoding="utf-8"
        )
        (source / "profiles.yml").write_text("pop_talk_dw: {}\n", encoding="utf-8")
        models = source / "models"
        models.mkdir()
        (models / "dim_movie.sql").write_text("select 1", encoding="utf-8")
        deployment = SimpleNamespace(
            project_path=source,
            profile_name="pop_talk_dw",
            target_name="dev",
            model_selectors=(
                "fqn:pop_talk_dw.dw.dim_movie",
                "fqn:pop_talk_dw.dw.fct_boxoffice_daily",
            ),
        )
        cosmos_project = root / "cosmos-task"
        cosmos_project.mkdir()
        (cosmos_project / "stale.sql").write_text("select 2", encoding="utf-8")
        return deployment, cosmos_project

    @staticmethod
    def _run_arguments(project: Path, selector: str) -> list[str]:
        return [
            "run",
            "--select",
            selector,
            "--project-dir",
            str(project),
            "--profiles-dir",
            "/tmp/stale-profile",
            "--profile",
            "pop_talk_dw",
            "--target",
            "dev",
        ]

    def test_model_run_replaces_cosmos_copy_and_keeps_writable_project(self) -> None:
        """고정 source는 Cosmos 임시 project로 복사하고 원본에는 쓰지 않는다."""
        with tempfile.TemporaryDirectory() as directory:
            deployment, cosmos_project = self._fixture(Path(directory))
            arguments = prepare_invocation(
                self._run_arguments(
                    cosmos_project, "fqn:pop_talk_dw.dw.dim_movie"
                ),
                deployment=deployment,
                airflow_task_id="build_snowflake_gold.dim_movie_run",
            )

            self.assertFalse((cosmos_project / "stale.sql").exists())
            self.assertEqual(
                "select 1",
                (cosmos_project / "models" / "dim_movie.sql").read_text(
                    encoding="utf-8"
                ),
            )
            project_index = arguments.index("--project-dir")
            profiles_index = arguments.index("--profiles-dir")
            self.assertEqual(arguments[project_index + 1], str(cosmos_project.resolve()))
            self.assertEqual(arguments[profiles_index + 1], str(cosmos_project.resolve()))
            self.assertNotIn("--target-path", arguments)
            self.assertNotIn("--log-path", arguments)

    def test_unknown_or_wrong_task_selector_is_rejected(self) -> None:
        """고정 manifest 밖 모델과 다른 task 이름의 모델 실행을 모두 거부한다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment, cosmos_project = self._fixture(root)
            with self.assertRaises(RuntimeError):
                prepare_invocation(
                    self._run_arguments(
                        cosmos_project, "fqn:pop_talk_dw.dw.not_in_manifest"
                    ),
                    deployment=deployment,
                    airflow_task_id="build_snowflake_gold.not_in_manifest_run",
                )

            with self.assertRaises(RuntimeError):
                prepare_invocation(
                    self._run_arguments(
                        cosmos_project, "fqn:pop_talk_dw.dw.dim_movie"
                    ),
                    deployment=deployment,
                    airflow_task_id="build_snowflake_gold.fct_boxoffice_daily_run",
                )

    def test_test_gate_must_run_every_test_without_selection(self) -> None:
        """AFTER_ALL gate에 select/exclude가 붙어 테스트가 누락되면 실패한다."""
        with tempfile.TemporaryDirectory() as directory:
            deployment, cosmos_project = self._fixture(Path(directory))
            base = [
                "test",
                "--project-dir",
                str(cosmos_project),
                "--profiles-dir",
                "/tmp/stale-profile",
                "--profile",
                "pop_talk_dw",
                "--target",
                "dev",
            ]
            prepare_invocation(
                base,
                deployment=deployment,
                airflow_task_id="build_snowflake_gold.project_test",
            )

            with self.assertRaises(RuntimeError):
                prepare_invocation(
                    [*base, "--select", "dim_movie"],
                    deployment=deployment,
                    airflow_task_id="build_snowflake_gold.project_test",
                )

    def test_path_override_and_duplicate_critical_option_are_rejected(self) -> None:
        """target/log 탈출과 마지막 option 우선순위 우회를 허용하지 않는다."""
        with tempfile.TemporaryDirectory() as directory:
            deployment, cosmos_project = self._fixture(Path(directory))
            base = self._run_arguments(
                cosmos_project, "fqn:pop_talk_dw.dw.dim_movie"
            )
            with self.assertRaises(RuntimeError):
                prepare_invocation(
                    [*base, "--target-path", "/tmp/override"],
                    deployment=deployment,
                    airflow_task_id="build_snowflake_gold.dim_movie_run",
                )
            with self.assertRaises(RuntimeError):
                prepare_invocation(
                    [*base, "--project-dir", "/tmp/second"],
                    deployment=deployment,
                    airflow_task_id="build_snowflake_gold.dim_movie_run",
                )

    def test_graph_and_locked_source_identity_must_match(self) -> None:
        """A graph와 B source 조합은 양쪽 배포가 각각 유효해도 실패한다."""
        first = {
            "deployment_id": "a" * 64,
            "manifest_sha256": "b" * 64,
            "model_version": "c" * 64,
        }
        _require_same_identity(first, dict(first))
        second = {**first, "deployment_id": "d" * 64}
        with self.assertRaises(RuntimeError):
            _require_same_identity(first, second)


if __name__ == "__main__":
    unittest.main()
