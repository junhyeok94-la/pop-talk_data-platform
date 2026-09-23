from __future__ import annotations

import unittest

from pipelines.orchestration.asset_contract import (
    RAW_DAILY_DAG_ID,
    build_asset_event_extra,
    confirm_loaded_batch,
    resolve_ready_inputs,
)

BUCKET = "bucket"
ASSET_URI = "s3://bucket/manifests/movie_api_daily/v1/DAILY_READY"


def ready_key(day: str, run_id: str) -> str:
    return (
        "manifests/movie_api_daily/v1/"
        f"collection_date={day}/run_id={run_id}/DAILY_READY.json"
    )


def event(day: str, run_id: str, source_run_id: str = "scheduled__one") -> dict:
    key = ready_key(day, run_id)
    extra = {
        "contract_version": 1,
        "bucket": BUCKET,
        "ready_manifest_key": key,
        "source_dag_id": RAW_DAILY_DAG_ID,
        "source_run_id": source_run_id,
        "collection_date": day,
        "raw_run_id": run_id,
    }
    return {
        "asset_uri": ASSET_URI,
        "source_dag_id": RAW_DAILY_DAG_ID,
        "source_run_id": source_run_id,
        "extra": extra,
    }


class AssetContractTest(unittest.TestCase):
    def resolve(self, events: list[dict], run_type: str = "asset_triggered") -> list:
        return resolve_ready_inputs(
            run_type=run_type,
            events=events,
            manual_ready_key=ready_key("2026-09-09", "a" * 24),
            expected_asset_uri=ASSET_URI,
            expected_bucket=BUCKET,
            max_map_length=1024,
        )

    def test_build_event_extra_does_not_infer_conflicting_lineage(self) -> None:
        key = ready_key("2026-09-10", "a" * 24)
        plan = {
            "run_id": "a" * 24,
            "airflow_run_id": "scheduled__one",
            "scope": {"collection_date": "2026-09-10"},
        }
        value = build_asset_event_extra(
            plan=plan,
            bucket=BUCKET,
            ready_manifest_key=key,
            source_dag_id=RAW_DAILY_DAG_ID,
            source_run_id="scheduled__one",
        )
        self.assertEqual(value["raw_run_id"], "a" * 24)
        with self.assertRaisesRegex(ValueError, "airflow_run_id"):
            build_asset_event_extra(
                plan=plan,
                bucket=BUCKET,
                ready_manifest_key=key,
                source_dag_id=RAW_DAILY_DAG_ID,
                source_run_id="scheduled__different",
            )

    def test_asset_batch_deduplicates_and_sorts(self) -> None:
        first = event("2026-09-09", "a" * 24)
        second = event("2026-09-10", "b" * 24, "scheduled__two")
        resolved = self.resolve([second, first, dict(first)])
        self.assertEqual(
            [item["raw_run_id"] for item in resolved],
            ["a" * 24, "b" * 24],
        )

    def test_manual_run_uses_only_validated_param(self) -> None:
        resolved = self.resolve([], run_type="DagRunType.MANUAL")
        self.assertEqual(resolved[0]["source_dag_id"], "manual")
        self.assertEqual(resolved[0]["raw_run_id"], "a" * 24)

    def test_asset_run_rejects_missing_or_wrong_provenance(self) -> None:
        with self.assertRaisesRegex(ValueError, "triggering event"):
            self.resolve([])
        wrong = event("2026-09-10", "a" * 24)
        wrong["source_run_id"] = "different"
        with self.assertRaisesRegex(ValueError, "실제 source run"):
            self.resolve([wrong])

    def test_asset_run_rejects_invalid_contract_and_map_overflow(self) -> None:
        wrong = event("2026-09-10", "a" * 24)
        wrong["extra"] = {**wrong["extra"], "contract_version": 2}
        with self.assertRaisesRegex(ValueError, "계약 버전"):
            self.resolve([wrong])
        with self.assertRaisesRegex(ValueError, "max_map_length"):
            resolve_ready_inputs(
                run_type="asset_triggered",
                events=[
                    event("2026-09-09", "a" * 24),
                    event("2026-09-10", "b" * 24, "scheduled__two"),
                ],
                manual_ready_key="",
                expected_asset_uri=ASSET_URI,
                expected_bucket=BUCKET,
                max_map_length=1,
            )

    def test_conflicting_duplicate_is_rejected(self) -> None:
        first = event("2026-09-10", "a" * 24)
        conflicting = event("2026-09-10", "a" * 24, "scheduled__two")
        with self.assertRaisesRegex(ValueError, "lineage가 서로 충돌"):
            self.resolve([first, conflicting])

    def test_loaded_batch_barrier_confirms_identity_and_revision(self) -> None:
        ready = self.resolve(
            [
                event("2026-09-09", "a" * 24),
                event("2026-09-10", "b" * 24, "scheduled__two"),
            ]
        )
        staged = [
            {
                "run_id": item["raw_run_id"],
                "ready_key": item["ready_manifest_key"],
                "artifact_version": "c" * 64,
                "publication_revision": 1,
            }
            for item in ready
        ]
        loaded = [
            {
                "source_run_id": item["raw_run_id"],
                "artifact_version": "c" * 64,
                "publication_revision": 1,
                "exchange_ready_key": (
                    "exchange/movie_silver/v1/"
                    f"source_run_id={item['raw_run_id']}/"
                    f"artifact_version={'c' * 64}/publication_revision=1/"
                    "EXCHANGE_READY.json"
                ),
            }
            for item in reversed(ready)
        ]
        result = confirm_loaded_batch(
            ready_inputs=ready,
            staged_results=staged,
            loaded_results=loaded,
            publication_revision=1,
        )
        self.assertEqual(result["input_count"], 2)
        self.assertEqual(result["raw_run_ids"], ["a" * 24, "b" * 24])

        swapped_staged = [dict(item) for item in staged]
        swapped_staged[0]["ready_key"], swapped_staged[1]["ready_key"] = (
            swapped_staged[1]["ready_key"],
            swapped_staged[0]["ready_key"],
        )
        with self.assertRaisesRegex(ValueError, "해당 Raw 입력"):
            confirm_loaded_batch(
                ready_inputs=ready,
                staged_results=swapped_staged,
                loaded_results=loaded,
                publication_revision=1,
            )

        with self.assertRaisesRegex(ValueError, "Snowflake load"):
            confirm_loaded_batch(
                ready_inputs=ready,
                staged_results=staged,
                loaded_results=loaded[:1],
                publication_revision=1,
            )


if __name__ == "__main__":
    unittest.main()
