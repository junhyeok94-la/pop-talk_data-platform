"""S3 원천 lineage와 Snowflake 입력 테이블을 분리하고 재시도 시점을 보존한다."""
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from pipelines.orchestration.source_snapshot_runtime import freeze_source
from pipelines.orchestration.model_generation_contract import (
    create_source_snapshot, source_snapshot_body, validate_source_snapshot,
)


class SourceSnapshotTests(unittest.TestCase):
    def test_freeze_keeps_manifest_and_reuses_same_point_in_time(self):
        connection = MagicMock()
        evidence = {"content_sha256": "a" * 64, "row_count": 2, "query_id": "query-a"}
        inputs = dict(source_unique_id="source.pop_talk_dw.pop_talk_staging.movie_observations_raw",
                      cutoff=datetime(2026, 9, 11, tzinfo=timezone.utc), manifest_key="s3://bucket/ready.json",
                      source_batch_id="batch-a", dataset_revision=1)
        with patch("pipelines.orchestration.source_snapshot_runtime.fingerprint_candidate_relation", return_value=evidence):
            first, _ = freeze_source(connection, **inputs)
            second, _ = freeze_source(connection, **inputs)
        self.assertEqual(first, second)
        self.assertEqual(first.manifest_key, inputs["manifest_key"])
        self.assertTrue(first.relation_id.startswith("POP_TALK_DW_DEV.CANDIDATE.SOURCE_"))
        validate_source_snapshot(first)
        with self.assertRaises(ValueError):
            validate_source_snapshot(replace(first, relation_id="POP_TALK_DW_DEV.CANDIDATE.OTHER"))
        sql, params = connection.cursor.return_value.__enter__.return_value.execute.call_args.args
        self.assertIn("IF NOT EXISTS", sql)
        self.assertEqual(params, ("2026-09-11T00:00:00+00:00",))

    def test_old_snapshot_body_remains_unchanged(self):
        snapshot = create_source_snapshot(source_unique_id="source.pkg.raw.movies", source_batch_id="batch",
            dataset_revision=1, manifest_key="old/manifest.json", cutoff="2026-09-11", content_sha256="a" * 64)
        self.assertNotIn("relation_id", source_snapshot_body(snapshot))
        validate_source_snapshot(snapshot)
