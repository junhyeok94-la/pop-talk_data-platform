"""Cosmos용 불변 dbt 배포 계약의 회귀 테스트."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from pipelines.orchestration.dbt_deployment import (
    DbtDeploymentError,
    canonical_json,
    load_current_deployment,
    model_version,
    source_file_digests,
    validate_deployment,
)


def _write_deployment(
    root: Path,
    sql: str,
    *,
    model_name: str = "movie",
    depends_on: list[str] | None = None,
) -> tuple[str, dict[str, object]]:
    """테스트용 최소 배포본을 실제 content-address 규칙으로 작성한다."""
    temporary = root / "building"
    project = temporary / "project"
    (project / "models").mkdir(parents=True)
    (project / "models" / f"{model_name}.sql").write_text(sql, encoding="utf-8")
    (project / "dbt_project.yml").write_text(
        "name: pop_talk_dw\nversion: 1.0.0\n", encoding="utf-8"
    )
    (project / "profiles.yml").write_text("pop_talk_dw: {}\n", encoding="utf-8")

    manifest = {
        "metadata": {"project_name": "pop_talk_dw", "dbt_version": "1.10.11"},
        "nodes": {
            f"model.pop_talk_dw.{model_name}": {
                "resource_type": "model",
                "fqn": ["pop_talk_dw", model_name],
                "depends_on": {"nodes": depends_on or []},
            },
            f"test.pop_talk_dw.{model_name}_not_null": {"resource_type": "test"},
        },
    }
    manifest_body = canonical_json(manifest)
    (temporary / "manifest.json").write_bytes(manifest_body)
    contract: dict[str, object] = {
        "schema_version": 1,
        "project_name": "pop_talk_dw",
        "profile_name": "pop_talk_dw",
        "target_name": "dev",
        "model_version": model_version(project),
        "manifest_sha256": hashlib.sha256(manifest_body).hexdigest(),
        "dbt_versions": {"dbt-core": "1.10.11", "dbt-snowflake": "1.10.3"},
        "source_files": source_file_digests(project),
        "model_unique_ids": [f"model.pop_talk_dw.{model_name}"],
        "test_unique_ids": [f"test.pop_talk_dw.{model_name}_not_null"],
    }
    deployment_id = hashlib.sha256(canonical_json(contract)).hexdigest()
    descriptor = {**contract, "deployment_id": deployment_id}
    (temporary / "deployment.json").write_bytes(canonical_json(descriptor))
    temporary.rename(root / deployment_id)
    return deployment_id, descriptor


class DbtDeploymentTests(unittest.TestCase):
    def test_invalid_deployment_id_is_rejected_before_path_access(self) -> None:
        """외부 입력이 배포 root 밖의 경로를 선택할 수 없어야 한다."""
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(DbtDeploymentError):
                validate_deployment(Path(directory), "../outside")

    def test_current_pointer_can_move_without_changing_fixed_deployment(self) -> None:
        """새 run용 pointer가 바뀌어도 기존 run은 이전 불변 경로를 계속 사용한다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_id, first = _write_deployment(root, "select 1 as movie_id")
            (root / "current.json").write_bytes(
                canonical_json({"deployment_id": first_id})
            )
            fixed = load_current_deployment(root)

            second_id, _ = _write_deployment(root, "select 2 as movie_id")
            (root / "current.json").write_bytes(
                canonical_json({"deployment_id": second_id})
            )

            self.assertEqual(second_id, load_current_deployment(root).deployment_id)
            self.assertEqual(
                first_id,
                validate_deployment(
                    root,
                    fixed.deployment_id,
                    expected_manifest_sha256=str(first["manifest_sha256"]),
                    expected_model_version=str(first["model_version"]),
                ).deployment_id,
            )

    def test_source_mutation_is_rejected(self) -> None:
        """불변 경로 안의 SQL이 변하면 게시 전에 즉시 실패해야 한다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment_id, _ = _write_deployment(root, "select 1")
            model = root / deployment_id / "project" / "models" / "movie.sql"
            model.write_text("select 2", encoding="utf-8")

            with self.assertRaises(DbtDeploymentError):
                validate_deployment(root, deployment_id)

    def test_manifest_mutation_is_rejected(self) -> None:
        """렌더링 그래프가 변하면 source가 같아도 실행을 허용하지 않는다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deployment_id, _ = _write_deployment(root, "select 1")
            manifest = root / deployment_id / "manifest.json"
            manifest.write_bytes(manifest.read_bytes() + b" ")

            with self.assertRaises(DbtDeploymentError):
                validate_deployment(root, deployment_id)

    def test_graph_and_execution_from_different_deployments_are_rejected(self) -> None:
        """모델/edge가 다른 A graph와 B source의 혼합 상태를 거부한다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_id, first = _write_deployment(root, "select 1")
            second_id, second = _write_deployment(
                root,
                "select * from {{ ref('upstream') }}",
                model_name="movie_v2",
                depends_on=["model.pop_talk_dw.upstream"],
            )

            with self.assertRaises(DbtDeploymentError):
                validate_deployment(
                    root,
                    second_id,
                    expected_manifest_sha256=str(first["manifest_sha256"]),
                    expected_model_version=str(first["model_version"]),
                )
            self.assertNotEqual(first_id, second_id)
            self.assertNotEqual(first["model_unique_ids"], second["model_unique_ids"])


if __name__ == "__main__":
    unittest.main()
