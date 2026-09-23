"""Airflow Asset event와 영화 일일 처리 batch 사이의 순수 데이터 계약이다.

Airflow 객체를 직접 import하지 않으므로 event 정규화 이후의 검증, 중복 제거와 load
barrier 대사를 일반 단위 테스트로 검증할 수 있다.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, TypedDict

ASSET_CONTRACT_VERSION = 1
RAW_DAILY_DAG_ID = "pop_talk_movie_raw_daily"
READY_KEY_PATTERN = re.compile(
    r"^manifests/movie_api_daily/v1/"
    r"collection_date=(\d{4}-\d{2}-\d{2})/"
    r"run_id=([0-9a-f]{24})/DAILY_READY[.]json$"
)


class ReadyInput(TypedDict):
    """메인 DAG의 mapped 처리 한 건을 식별하는 작은 lineage 값."""

    contract_version: int
    bucket: str
    ready_manifest_key: str
    source_dag_id: str
    source_run_id: str
    collection_date: str
    raw_run_id: str


class NormalizedAssetEvent(TypedDict):
    """Airflow 런타임 객체에서 꺼낸 검증 가능한 event 표현."""

    asset_uri: str
    source_dag_id: str
    source_run_id: str
    extra: Mapping[str, Any]


def _key_lineage(ready_key: str) -> tuple[str, str]:
    match = READY_KEY_PATTERN.fullmatch(ready_key)
    if not match:
        raise ValueError("READY manifest key 형식이 movie_api_daily/v1 계약과 다릅니다")
    return match.group(1), match.group(2)


def build_asset_event_extra(
    *,
    plan: Mapping[str, Any],
    bucket: str,
    ready_manifest_key: str,
    source_dag_id: str,
    source_run_id: str,
) -> ReadyInput:
    """검증 완료된 Raw plan을 JSON 직렬화 가능한 Asset event metadata로 만든다."""
    collection_date, raw_run_id = _key_lineage(ready_manifest_key)
    plan_scope = plan.get("scope")
    if not isinstance(plan_scope, Mapping):
        raise ValueError("Raw plan scope가 없습니다")
    if plan.get("run_id") != raw_run_id:
        raise ValueError("Raw plan run_id와 READY key가 다릅니다")
    if plan_scope.get("collection_date") != collection_date:
        raise ValueError("Raw plan collection_date와 READY key가 다릅니다")
    if plan.get("airflow_run_id") != source_run_id:
        raise ValueError("Raw plan airflow_run_id와 Asset provenance가 다릅니다")
    return {
        "contract_version": ASSET_CONTRACT_VERSION,
        "bucket": bucket,
        "ready_manifest_key": ready_manifest_key,
        "source_dag_id": source_dag_id,
        "source_run_id": source_run_id,
        "collection_date": collection_date,
        "raw_run_id": raw_run_id,
    }


def _validated_event(
    event: NormalizedAssetEvent,
    *,
    expected_asset_uri: str,
    expected_bucket: str,
) -> ReadyInput:
    if event["asset_uri"] != expected_asset_uri:
        raise ValueError("예상하지 않은 Asset URI가 triggering event에 포함됐습니다")
    extra = event["extra"]
    required = {
        "contract_version",
        "bucket",
        "ready_manifest_key",
        "source_dag_id",
        "source_run_id",
        "collection_date",
        "raw_run_id",
    }
    if not isinstance(extra, Mapping) or not required.issubset(extra):
        raise ValueError("Asset event metadata의 필수 lineage가 없습니다")
    if extra["contract_version"] != ASSET_CONTRACT_VERSION:
        raise ValueError("지원하지 않는 Asset event 계약 버전입니다")
    if extra["bucket"] != expected_bucket:
        raise ValueError("Asset event bucket이 파이프라인 bucket과 다릅니다")
    if extra["source_dag_id"] != RAW_DAILY_DAG_ID:
        raise ValueError("Asset event 생산 DAG가 일일 Raw DAG가 아닙니다")
    if event["source_dag_id"] != extra["source_dag_id"]:
        raise ValueError("Asset event의 실제 source DAG와 metadata가 다릅니다")
    if event["source_run_id"] != extra["source_run_id"]:
        raise ValueError("Asset event의 실제 source run과 metadata가 다릅니다")

    ready_key = str(extra["ready_manifest_key"])
    collection_date, raw_run_id = _key_lineage(ready_key)
    if extra["collection_date"] != collection_date:
        raise ValueError("Asset collection_date와 READY key가 다릅니다")
    if extra["raw_run_id"] != raw_run_id:
        raise ValueError("Asset raw_run_id와 READY key가 다릅니다")
    return {
        "contract_version": ASSET_CONTRACT_VERSION,
        "bucket": expected_bucket,
        "ready_manifest_key": ready_key,
        "source_dag_id": RAW_DAILY_DAG_ID,
        "source_run_id": str(extra["source_run_id"]),
        "collection_date": collection_date,
        "raw_run_id": raw_run_id,
    }


def resolve_ready_inputs(
    *,
    run_type: str,
    events: Iterable[NormalizedAssetEvent],
    manual_ready_key: str,
    expected_asset_uri: str,
    expected_bucket: str,
    max_map_length: int,
) -> list[ReadyInput]:
    """실행 유형에 맞는 READY 목록을 검증하고 논리 중복 제거 후 정렬한다."""
    normalized_run_type = str(run_type).split(".")[-1].lower()
    if normalized_run_type == "manual":
        collection_date, raw_run_id = _key_lineage(manual_ready_key)
        return [
            {
                "contract_version": ASSET_CONTRACT_VERSION,
                "bucket": expected_bucket,
                "ready_manifest_key": manual_ready_key,
                "source_dag_id": "manual",
                "source_run_id": "",
                "collection_date": collection_date,
                "raw_run_id": raw_run_id,
            }
        ]
    if normalized_run_type != "asset_triggered":
        raise ValueError(f"지원하지 않는 메인 DAG run type입니다: {run_type}")

    deduplicated: dict[tuple[str, str, str], ReadyInput] = {}
    for event in events:
        ready = _validated_event(
            event,
            expected_asset_uri=expected_asset_uri,
            expected_bucket=expected_bucket,
        )
        identity = (
            ready["bucket"],
            ready["ready_manifest_key"],
            ready["raw_run_id"],
        )
        previous = deduplicated.get(identity)
        if previous is not None and previous != ready:
            raise ValueError("동일 READY identity의 Asset lineage가 서로 충돌합니다")
        deduplicated[identity] = ready

    if not deduplicated:
        raise ValueError("Asset-triggered run에 triggering event가 없습니다")
    resolved = sorted(
        deduplicated.values(),
        key=lambda item: (item["collection_date"], item["ready_manifest_key"]),
    )
    if len(resolved) > max_map_length:
        raise ValueError(
            f"READY batch {len(resolved)}건이 Airflow max_map_length "
            f"{max_map_length}를 초과했습니다"
        )
    return resolved


def confirm_loaded_batch(
    *,
    ready_inputs: Sequence[Mapping[str, Any]],
    staged_results: Sequence[Mapping[str, Any]],
    loaded_results: Sequence[Mapping[str, Any]],
    publication_revision: int,
) -> dict[str, Any]:
    """모든 mapped 입력이 같은 artifact/revision으로 Snowflake까지 갔는지 대사한다."""
    expected_by_id = {
        str(item["raw_run_id"]): str(item["ready_manifest_key"])
        for item in ready_inputs
    }
    staged_by_id = {str(item["run_id"]): item for item in staged_results}
    loaded_by_id = {str(item["source_run_id"]): item for item in loaded_results}
    if not expected_by_id or len(expected_by_id) != len(ready_inputs):
        raise ValueError("READY batch raw_run_id가 비었거나 중복됐습니다")
    if (
        len(staged_by_id) != len(staged_results)
        or set(staged_by_id) != set(expected_by_id)
    ):
        raise ValueError("Databricks stage 결과가 READY batch와 일치하지 않습니다")
    if (
        len(loaded_by_id) != len(loaded_results)
        or set(loaded_by_id) != set(expected_by_id)
    ):
        raise ValueError("Snowflake load 결과가 READY batch와 일치하지 않습니다")

    artifact_versions = {str(item["artifact_version"]) for item in staged_results}
    if len(artifact_versions) != 1:
        raise ValueError("mapped load가 하나의 처리 artifact를 공유하지 않습니다")
    revisions = {int(item["publication_revision"]) for item in staged_results}
    if revisions != {publication_revision}:
        raise ValueError("mapped load publication revision이 입력과 다릅니다")
    artifact_version = next(iter(artifact_versions))
    for raw_run_id, expected_ready_key in expected_by_id.items():
        staged_item = staged_by_id[raw_run_id]
        if str(staged_item["ready_key"]) != expected_ready_key:
            raise ValueError("stage 결과의 READY key가 해당 Raw 입력과 다릅니다")
        loaded_item = loaded_by_id[raw_run_id]
        if str(loaded_item["artifact_version"]) != artifact_version:
            raise ValueError("Snowflake load artifact가 해당 stage 입력과 다릅니다")
        if int(loaded_item["publication_revision"]) != publication_revision:
            raise ValueError("Snowflake load revision이 해당 stage 입력과 다릅니다")
        expected_exchange_key = (
            "exchange/movie_silver/v1/"
            f"source_run_id={raw_run_id}/artifact_version={artifact_version}/"
            f"publication_revision={publication_revision}/EXCHANGE_READY.json"
        )
        if str(loaded_item["exchange_ready_key"]) != expected_exchange_key:
            raise ValueError(
                "Snowflake load의 Exchange READY lineage가 해당 입력과 다릅니다"
            )
    return {
        "input_count": len(ready_inputs),
        "raw_run_ids": sorted(expected_by_id),
        "artifact_version": artifact_version,
        "publication_revision": publication_revision,
    }
