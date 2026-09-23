"""모델별 DAG event 정규화와 factory topology 계약 테스트."""
from __future__ import annotations

import importlib.util
import sys
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

from pipelines.orchestration.model_dag_runtime import (
    event_id_for,
    normalize_cohort_asset_event,
    normalize_model_asset_event,
    rescan_generation,
    publish_outbox_event,
)
from pipelines.orchestration.model_generation_contract import CohortReadyEventV2


class ModelDagRuntimeTests(unittest.TestCase):
    def test_completed_terminal_event_does_not_emit_again(self):
        event = {"event_id": "done", "asset_uri": "pop-talk://model/end/ready",
                 "partition_key": "gen-a", "event_body": {"model": "end"}}
        with patch("pipelines.orchestration.model_dag_runtime.connect_control") as connect, patch(
            "pipelines.orchestration.model_dag_runtime.PostgresControlStore"
        ) as store:
            connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
                event["asset_uri"], event["partition_key"], event["event_body"])
            self.assertIsNone(publish_outbox_event(event, publisher_id="worker"))
            store.return_value.claim_outbox_event.assert_not_called()

    def test_invalid_plan_does_not_acknowledge_or_claim_event(self) -> None:
        """대상이 잘못된 요청은 DB 연결조차 하지 않아 이벤트를 소비하지 않는다."""
        with patch("pipelines.orchestration.model_dag_runtime.connect_control") as connect:
            with self.assertRaisesRegex(RuntimeError, "plan이 다릅니다"):
                rescan_generation(
                    plan_id="other-plan",
                    parent_event={"event_body": {"plan_id": "event-plan"}},
                    claim_owner="dispatcher",
                )
            with self.assertRaisesRegex(RuntimeError, "plan_id가 없습니다"):
                rescan_generation(plan_id="", parent_event=None, claim_owner="dispatcher")
            connect.assert_not_called()

    def test_duplicate_work_does_not_block_readiness_rescan(self) -> None:
        """완료/처리 중인 work는 건너뛰고 이번에 얻은 claim만 완료한다."""
        event = {
            "event_id": "event-a", "asset_uri": "pop-talk://model/a/ready",
            "partition_key": "gen-a", "event_body": {"plan_id": "plan-a"},
        }
        with patch("pipelines.orchestration.model_dag_runtime.connect_control"), patch(
            "pipelines.orchestration.model_dag_runtime.PostgresControlStore"
        ) as store_class:
            store = store_class.return_value
            store.delivery_consumers.return_value = ["publication.movie_gold", "completed", "active", "pending"]
            claim = object()
            store.claim_work.side_effect = [None, None, claim]
            store.register_reuse_outbox.return_value = []
            store.materialize_ready_cohorts.return_value = []
            self.assertEqual(rescan_generation(
                plan_id="", parent_event=event, claim_owner="dispatcher"
            ), [])
            store.materialize_ready_cohorts.assert_called_once_with(plan_id="plan-a")
            store.complete_work.assert_called_once_with(claim)
            self.assertEqual(store.acknowledge_delivery.call_count, 3)

    def test_cohort_event_requires_exact_envelope_identity(self) -> None:
        model_id = "model.pop_talk_dw.synthetic"
        asset_uri = f"pop-talk://cohort/{model_id}/ready"
        body = asdict(
            CohortReadyEventV2(
                contract_version=2,
                partition_key="gen-000000000001",
                generation_id="gen-000000000001",
                plan_id="plan-a",
                deployment_id="d" * 64,
                model_unique_id=model_id,
                build_id="build-a",
                cohort_manifest_id="cohort-a",
            )
        )
        event_id = event_id_for(
            aggregate_kind="COHORT_READY",
            aggregate_id="cohort-a",
            asset_uri=asset_uri,
            partition_key="gen-000000000001",
            event_body=body,
        )
        normalized = normalize_cohort_asset_event(
            asset_uri=asset_uri,
            partition_key="gen-000000000001",
            extra={"event_id": event_id, "event_body": body},
            expected_model_unique_id=model_id,
        )
        self.assertEqual(normalized["event_id"], event_id)
        with self.assertRaisesRegex(RuntimeError, "event ID"):
            normalize_cohort_asset_event(
                asset_uri=asset_uri,
                partition_key="gen-000000000001",
                extra={"event_id": "event-tampered", "event_body": body},
                expected_model_unique_id=model_id,
            )

    def test_model_ready_event_requires_matching_model_asset_uri(self) -> None:
        model_id = "model.pop_talk_dw.synthetic"
        body = {
            "contract_version": 2,
            "partition_key": "gen-000000000001",
            "generation_id": "gen-000000000001",
            "plan_id": "plan-a",
            "deployment_id": "d" * 64,
            "model_unique_id": model_id,
            "result_manifest_id": "result-a",
            "origin_generation_id": "gen-000000000001",
            "relation_id": "DB.SCHEMA.TABLE",
            "row_count": 1,
            "content_sha256": "a" * 64,
        }
        uri = f"pop-talk://model/{model_id}/ready"
        event_id = event_id_for(
            aggregate_kind="MODEL_READY",
            aggregate_id="result-a",
            asset_uri=uri,
            partition_key="gen-000000000001",
            event_body=body,
        )
        self.assertEqual(
            normalize_model_asset_event(
                asset_uri=uri,
                partition_key="gen-000000000001",
                extra={"event_id": event_id, "event_body": body},
            )["event_id"],
            event_id,
        )
        with self.assertRaisesRegex(RuntimeError, "URI"):
            normalize_model_asset_event(
                asset_uri="pop-talk://model/model.pop_talk_dw.other/ready",
                partition_key="gen-000000000001",
                extra={"event_id": event_id, "event_body": body},
            )


if __name__ == "__main__":
    unittest.main()
