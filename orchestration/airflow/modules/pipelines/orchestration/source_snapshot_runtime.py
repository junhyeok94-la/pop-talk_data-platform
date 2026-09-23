"""일일 적재가 끝난 원천 테이블을 같은 시점의 모델 입력으로 고정한다."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipelines.orchestration.model_generation_contract import content_id, create_source_snapshot
from pipelines.orchestration.model_relation_validation import fingerprint_candidate_relation


SOURCE_TABLES = {
    f"source.pop_talk_dw.pop_talk_staging.{name}": f"POP_TALK_DW_DEV.STAGING.{name.upper()}"
    for name in ("movie_observations_raw", "boxoffice_observations_raw", "silver_exchange_loads")
}


def freeze_source(
    connection: Any, *, source_unique_id: str, cutoff: datetime,
    manifest_key: str, source_batch_id: str, dataset_revision: int,
):
    """같은 source/시점은 재시도에도 같은 clone을 사용한다. 과거 데이터가 없으면 실패한다."""
    if source_unique_id not in SOURCE_TABLES or cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("지원하는 source와 시간대가 명시된 cutoff가 필요합니다")
    if not manifest_key or not source_batch_id or dataset_revision < 1:
        raise ValueError("source manifest/batch/revision이 필요합니다")
    fixed_time = cutoff.astimezone(timezone.utc).isoformat()
    identity = content_id("source", {"source": source_unique_id, "cutoff": fixed_time}).split("-", 1)[1]
    relation = f"POP_TALK_DW_DEV.CANDIDATE.SOURCE_{identity.upper()}"
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS POP_TALK_DW_DEV.CANDIDATE")
        # 테이블 전체를 Python으로 복사하지 않는다. 세 source에는 같은 cutoff를 전달한다.
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {relation} CLONE {SOURCE_TABLES[source_unique_id]} "
            "AT (TIMESTAMP => %s::TIMESTAMP_TZ)", (fixed_time,),
        )
    evidence = fingerprint_candidate_relation(connection, relation)
    snapshot = create_source_snapshot(
        source_unique_id=source_unique_id, source_batch_id=source_batch_id,
        dataset_revision=dataset_revision, manifest_key=manifest_key, cutoff=fixed_time,
        content_sha256=evidence["content_sha256"], relation_id=relation,
    )
    return snapshot, evidence
