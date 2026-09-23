"""Airflow 없이 실행 artifact 계약을 검증한다."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pipelines.orchestration.movie_daily_runtime import (
    TRANSFORM_DEPENDENCIES,
    dbt_artifact,
    transform_artifact,
)


class ArtifactIdentityTests(unittest.TestCase):
    def test_transform_digest_covers_every_declared_dependency(self) -> None:
        """어느 전이 의존성이 바뀌어도 새 실행 artifact가 발급되어야 한다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for position, relative in enumerate(TRANSFORM_DEPENDENCIES):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(f"source-{position}".encode("utf-8"))

            baseline = transform_artifact(root)
            self.assertEqual(baseline, transform_artifact(root))

            for relative in TRANSFORM_DEPENDENCIES:
                path = root / relative
                original = path.read_bytes()
                path.write_bytes(original + b"-changed")
                self.assertNotEqual(baseline.version, transform_artifact(root).version)
                path.write_bytes(original)

    def test_dbt_digest_uses_sql_yaml_and_ignores_target(self) -> None:
        """실행 모델만 Gold identity에 포함하고 생성 artifact는 제외한다."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dbt_root = root / "pipelines" / "dbt"
            (dbt_root / "models").mkdir(parents=True)
            (dbt_root / "target").mkdir()
            model = dbt_root / "models" / "movie.sql"
            config = dbt_root / "dbt_project.yml"
            generated = dbt_root / "target" / "manifest.json"
            model.write_text("select 1", encoding="utf-8")
            config.write_text("name: pop_talk", encoding="utf-8")
            generated.write_text("first", encoding="utf-8")

            baseline = dbt_artifact(root)
            generated.write_text("second", encoding="utf-8")
            self.assertEqual(baseline, dbt_artifact(root))

            model.write_text("select 2", encoding="utf-8")
            self.assertNotEqual(baseline, dbt_artifact(root))


if __name__ == "__main__":
    unittest.main()
