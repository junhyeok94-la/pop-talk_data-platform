"""Airflow 모델별 DAG의 얇은 runtime adapter.

Airflow Asset event는 실행 신호만 전달한다. 이 모듈은 event를 PostgreSQL 원장과 대사하고,
영속 inbox/build claim과 exact dbt 계약을 준비한다. model/test 실행 증거와 실제
Snowflake candidate 내용을 검증한 뒤에만 다음 모델에 READY를 전달한다.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from pipelines.paths import DBT_DEPLOYMENT_ROOT
from typing import Any, Mapping

import psycopg

from pipelines.orchestration.dbt_deployment import validate_deployment
from pipelines.orchestration.dbt_invocation_contract import contract_from_json
from pipelines.orchestration.model_generation_contract import (
    ModelResultManifest,
    canonical_json,
    content_id,
    create_model_result,
    validate_model_ready_event,
)
from pipelines.orchestration.postgres_control_store import (
    BuildClaim,
    ExecutionReceipt,
    OutboxEvent,
    PostgresControlStore,
    WorkClaim,
    parent_vector_sha256,
    tests_sha256,
)

DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
DEFAULT_ARTIFACT_ROOT = "/opt/airflow/dbt-attempt-artifacts"


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"필수 control DB 환경변수가 없습니다: {name}")
    return value


def connect_control() -> psycopg.Connection:
    """DAG runtime 전용 최소 권한 PostgreSQL role로 연결한다."""
    return psycopg.connect(
        host=_required_environment("POP_TALK_CONTROL_POSTGRES_HOST"),
        port=int(os.environ.get("POP_TALK_CONTROL_POSTGRES_PORT", "5432")),
        dbname=_required_environment("POP_TALK_CONTROL_POSTGRES_DB"),
        user=_required_environment("POP_TALK_CONTROL_POSTGRES_USER"),
        password=_required_environment("POP_TALK_CONTROL_POSTGRES_PASSWORD"),
        connect_timeout=5,
        # 읽기 한 번 때문에 다음 store transaction까지 열린 상태가 되지 않게 한다.
        # 쓰기의 원자성은 각 store 메서드의 transaction이 담당한다.
        autocommit=True,
        options="-c statement_timeout=5000 -c lock_timeout=3000",
    )


def event_id_for(
    *, aggregate_kind: str, aggregate_id: str, asset_uri: str,
    partition_key: str, event_body: Mapping[str, Any],
) -> str:
    """outbox와 같은 envelope에서 event ID를 재계산한다."""
    return content_id(
        "event",
        {
            "aggregate_kind": aggregate_kind,
            "aggregate_id": aggregate_id,
            "asset_uri": asset_uri,
            "partition_key": partition_key,
            "event_body": json.loads(canonical_json(event_body)),
        },
    )


def normalize_cohort_asset_event(
    *,
    asset_uri: str,
    partition_key: str,
    extra: Mapping[str, Any],
    expected_model_unique_id: str,
) -> dict[str, Any]:
    """Airflow event extra를 권위 event body와 결정적 event ID로 정규화한다."""
    if set(extra) != {"event_id", "event_body"} or not isinstance(extra["event_body"], Mapping):
        raise RuntimeError("COHORT_READY Asset extra 형식이 정확하지 않습니다")
    body = json.loads(canonical_json(extra["event_body"]))
    if body.get("model_unique_id") != expected_model_unique_id:
        raise RuntimeError("다른 model의 COHORT_READY event입니다")
    expected_event_id = event_id_for(
        aggregate_kind="COHORT_READY",
        aggregate_id=str(body.get("cohort_manifest_id", "")),
        asset_uri=asset_uri,
        partition_key=partition_key,
        event_body=body,
    )
    if extra["event_id"] != expected_event_id:
        raise RuntimeError("COHORT_READY event ID가 outbox envelope와 다릅니다")
    return {
        "event_id": expected_event_id,
        "asset_uri": asset_uri,
        "partition_key": partition_key,
        "event_body": body,
    }


def normalize_model_asset_event(
    *, asset_uri: str, partition_key: str, extra: Mapping[str, Any]
) -> dict[str, Any]:
    """dispatcher가 받은 MODEL_READY event를 partition 및 outbox identity와 대사한다."""
    if set(extra) != {"event_id", "event_body"} or not isinstance(extra["event_body"], Mapping):
        raise RuntimeError("MODEL_READY Asset extra 형식이 정확하지 않습니다")
    body = json.loads(canonical_json(extra["event_body"]))
    model_unique_id = str(body.get("model_unique_id", ""))
    validate_model_ready_event(
        body,
        actual_partition_key=partition_key,
        expected_model_unique_id=model_unique_id,
    )
    if asset_uri != f"pop-talk://model/{model_unique_id}/ready":
        raise RuntimeError("MODEL_READY Asset URI와 model identity가 다릅니다")
    expected_event_id = event_id_for(
        aggregate_kind="MODEL_READY",
        aggregate_id=str(body["result_manifest_id"]),
        asset_uri=asset_uri,
        partition_key=partition_key,
        event_body=body,
    )
    if extra["event_id"] != expected_event_id:
        raise RuntimeError("MODEL_READY event ID가 outbox envelope와 다릅니다")
    return {
        "event_id": expected_event_id,
        "asset_uri": asset_uri,
        "partition_key": partition_key,
        "event_body": body,
    }


def rescan_generation(
    *,
    plan_id: str,
    parent_event: Mapping[str, Any] | None,
    claim_owner: str,
) -> list[dict[str, Any]]:
    """root/REUSE 또는 parent READY 도착 뒤 direct-parent readiness를 다시 계산한다."""
    # 잘못된 실행 대상은 ACK/claim 전에 거절해 정상 이벤트의 처리 상태를 보존한다.
    if parent_event is not None:
        event_plan_id = str(parent_event["event_body"]["plan_id"])
        if plan_id and plan_id != event_plan_id:
            raise RuntimeError("dispatcher plan Param과 MODEL_READY plan이 다릅니다")
        plan_id = event_plan_id
    if not plan_id:
        raise RuntimeError("재스캔할 plan_id가 없습니다")
    with connect_control() as connection:
        store = PostgresControlStore(connection)
        claims: list[WorkClaim] = []
        if parent_event is not None:
            event_id = str(parent_event["event_id"])
            for consumer_id in store.delivery_consumers(event_id=event_id):
                # 서빙 게시의 ACK/완료는 publication DAG가 실제 게시 후 처리한다.
                if consumer_id.startswith("publication."):
                    continue
                store.acknowledge_delivery(
                    event_id=event_id,
                    consumer_id=consumer_id,
                    actual_asset_uri=str(parent_event["asset_uri"]),
                    actual_partition_key=str(parent_event["partition_key"]),
                    actual_event_body=parent_event["event_body"],
                )
                claim = store.claim_work(
                    consumer_id=consumer_id,
                    event_id=event_id,
                    claim_owner=claim_owner,
                    lease_seconds=300,
                    required=False,
                )
                # 재전달된 완료 work나 다른 dispatcher가 처리 중인 work는 건너뛴다.
                if claim is not None:
                    claims.append(claim)
        events = list(store.register_reuse_outbox(plan_id=plan_id))
        events.extend(store.materialize_ready_cohorts(plan_id=plan_id))
        for claim in claims:
            store.complete_work(claim)
    return [{**asdict(event), "event_body": dict(event.event_body)} for event in events]


def acknowledge_cohort_event(event: Mapping[str, Any], *, model_unique_id: str) -> None:
    """Asset delivery ACK와 EXECUTE_MODEL inbox 생성을 원자 수행한다."""
    with connect_control() as connection:
        PostgresControlStore(connection).acknowledge_cohort_delivery(
            event_id=str(event["event_id"]),
            expected_model_unique_id=model_unique_id,
            actual_asset_uri=str(event["asset_uri"]),
            actual_partition_key=str(event["partition_key"]),
            actual_event_body=event["event_body"],
        )


def prepare_execution(
    event: Mapping[str, Any],
    *,
    model_unique_id: str,
    claim_owner: str,
    artifact_root: str = DEFAULT_ARTIFACT_ROOT,
) -> dict[str, Any]:
    """inbox/build을 claim하고 Cosmos 두 task가 사용할 exact 계약을 반환한다."""
    with connect_control() as connection:
        bundle = PostgresControlStore(connection).prepare_model_execution(
            event_id=str(event["event_id"]),
            model_unique_id=model_unique_id,
            claim_owner=claim_owner,
            artifact_root=artifact_root,
            lease_seconds=14_400,
            bind_relations=True,
        )
    return {
        "work_claim": {**asdict(bundle.work_claim), "lease_expires_at": bundle.work_claim.lease_expires_at.isoformat()},
        "build_claim": {**asdict(bundle.build_claim), "lease_expires_at": bundle.build_claim.lease_expires_at.isoformat()},
        "cohort": asdict(bundle.cohort),
        "model_contract_json": canonical_json(bundle.model_contract.body).decode("utf-8"),
        "test_contract_json": canonical_json(bundle.test_contract.body).decode("utf-8"),
    }


def validate_execution_deployment(execution: Mapping[str, Any]) -> dict[str, str]:
    """parse 시 고정된 deployment가 worker에서도 byte 단위로 같은지 확인한다."""
    contract = contract_from_json(str(execution["model_contract_json"]))
    deployment = validate_deployment(
        DBT_DEPLOYMENT_ROOT,
        contract.deployment_id,
        expected_manifest_sha256=contract.manifest_sha256,
        expected_model_version=contract.model_version,
    )
    required_macros = ("generate_database_name.sql", "generate_schema_name.sql", "generate_alias_name.sql", "source.sql", "ref.sql")
    if any(not (deployment.project_path / "macros" / name).is_file() for name in required_macros):
        raise RuntimeError("candidate relation 연결을 지원하는 dbt 배포본이 필요합니다")
    return {
        "deployment_id": deployment.deployment_id,
        "manifest_sha256": deployment.manifest_sha256,
        "model_version": deployment.model_version,
    }


def _work_claim(raw: Mapping[str, Any]) -> WorkClaim:
    return WorkClaim(
        **{**raw, "claim_token": int(raw["claim_token"]), "lease_expires_at": datetime.fromisoformat(str(raw["lease_expires_at"]))}
    )


def _build_claim(raw: Mapping[str, Any]) -> BuildClaim:
    return BuildClaim(
        **{
            **raw,
            "attempt_no": int(raw["attempt_no"]),
            "fence_token": int(raw["fence_token"]),
            "lease_expires_at": datetime.fromisoformat(str(raw["lease_expires_at"])),
        }
    )


def verify_execution(
    execution: Mapping[str, Any],
    *,
    snowflake_connection: Any,
) -> dict[str, Any]:
    """model/test 증거와 실제 candidate 내용을 검증한 뒤 게시할 result를 만든다."""
    from pipelines.orchestration.model_relation_validation import fingerprint_candidate_relation
    model_contract = contract_from_json(str(execution["model_contract_json"]))
    test_contract = contract_from_json(str(execution["test_contract_json"]))
    build = _build_claim(execution["build_claim"])
    work = _work_claim(execution["work_claim"])
    with connect_control() as connection:
        store = PostgresControlStore(connection)
        model_artifact = store.load_dbt_invocation_artifact(model_contract)
        test_artifact = store.load_dbt_invocation_artifact(test_contract)
        if model_artifact is None or test_artifact is None:
            raise RuntimeError("model/owned-test exact dbt artifact가 모두 필요합니다")
        catalog = store.load_catalog()
        cohort = catalog.cohorts.get(model_contract.cohort_manifest_id)
        plan = catalog.plans.get(model_contract.plan_id)
        if cohort is None or plan is None:
            raise RuntimeError("result를 만들 plan/cohort가 원장에 없습니다")
        evidence = fingerprint_candidate_relation(snowflake_connection, build.relation_id)
        receipt = ExecutionReceipt(
            build_id=build.build_id,
            attempt_no=build.attempt_no,
            fence_token=build.fence_token,
            cohort_manifest_id=cohort.cohort_manifest_id,
            deployment_id=cohort.deployment_id,
            parent_vector_sha256=parent_vector_sha256(cohort),
            claim_owner=build.claim_owner,
            relation_id=build.relation_id,
            row_count=evidence["row_count"],
            content_sha256=evidence["content_sha256"],
            model_invocation_id=model_artifact.orchestration_invocation_id,
            model_unique_id=cohort.model_unique_id,
            model_run_results_sha256=str(model_artifact.run_results_sha256),
            test_invocation_id=test_artifact.orchestration_invocation_id,
            test_run_results_sha256=test_artifact.run_results_sha256,
            executed_test_ids=test_artifact.executed_unique_ids,
            tests_sha256=tests_sha256(test_artifact.executed_unique_ids),
            relation_validation_query_id=evidence["query_id"],
            status="SUCCEEDED",
        )
        store.record_receipt(receipt)
        result = create_model_result(
            plan=plan,
            cohort=cohort,
            relation_id=build.relation_id,
            row_count=receipt.row_count,
            content_sha256=receipt.content_sha256,
            catalog=catalog,
        )
    return {
        "result": asdict(result),
        "build_claim": dict(execution["build_claim"]),
        "work_claim": dict(execution["work_claim"]),
    }


def commit_execution(verified: Mapping[str, Any]) -> dict[str, Any]:
    """검증된 result와 MODEL_READY outbox를 commit한 뒤 inbox work를 완료한다."""
    raw_result = dict(verified["result"])
    raw_result["parent_binding_ids"] = tuple(raw_result["parent_binding_ids"])
    result = ModelResultManifest(**raw_result)
    build = _build_claim(verified["build_claim"])
    work = _work_claim(verified["work_claim"])
    with connect_control() as connection:
        store = PostgresControlStore(connection)
        event = store.commit_result(result=result, claim=build)
        store.complete_work(work)
    return {**asdict(event), "event_body": dict(event.event_body)}


def publish_outbox_event(event: Mapping[str, Any], *, publisher_id: str) -> OutboxEvent | None:
    """exact event를 claim하고 성공한 Airflow emit 직후 기록할 claim을 반환한다."""
    with connect_control() as connection:
        store = PostgresControlStore(connection)
        # 수신자가 없는 마지막 모델과 이미 전달을 마친 이벤트는 재발행할 필요가 없다.
        completed = connection.execute("""SELECT asset_uri,partition_key,event_body
            FROM dw_control.asset_outbox WHERE event_id=%s AND state='COMPLETE'""",
            (str(event["event_id"]),)).fetchone()
        if completed is not None:
            if completed != (event["asset_uri"], event["partition_key"], event["event_body"]):
                raise RuntimeError("완료된 outbox event의 본문이 다릅니다")
            return None
        claim = store.claim_outbox_event(
            event_id=str(event["event_id"]), publisher_id=publisher_id
        )
        if asdict(claim.event) != dict(event):
            raise RuntimeError("발행 대상 event가 commit 결과와 다릅니다")
        store.mark_outbox_sent(claim)
        return claim.event


def reconcile_one_event(*, publisher_id: str) -> OutboxEvent | None:
    """미발행/만료 outbox 또는 복구가 필요한 inbox의 event 하나를 다시 claim한다."""
    with connect_control() as connection:
        store = PostgresControlStore(connection)
        store.requeue_one_recoverable_inbox_event(min_pending_age_seconds=600)
        claims = store.claim_outbox(
            publisher_id=publisher_id,
            lease_seconds=60,
            limit=1,
        )
        if not claims:
            return None
        store.mark_outbox_sent(claims[0])
        return claims[0].event
