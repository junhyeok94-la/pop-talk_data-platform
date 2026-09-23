"""실행별 Snowflake candidate 테이블을 읽어 실제 결과를 확인한다.

행 수는 내용 동일성의 증거가 아니다. 이 결과만으로 result를 게시하지 않는다.
"""
from __future__ import annotations

import re
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class CandidateRelationStats:
    relation_id: str
    row_count: int
    query_id: str


def fingerprint_candidate_relation(connection: Any, relation_id: str) -> dict[str, Any]:
    """정규화한 행의 SHA-256을 정렬해 조회 순서와 무관한 내용 증거를 만든다.

    원본 행은 1,000개씩 소비하고 메모리에는 행당 32-byte digest만 보존한다.
    NULL, 숫자, 날짜와 JSON을 구분하며 중복 행도 digest 계산에 포함한다.
    """
    from pipelines.orchestration.dbt_relation_bindings import relation_parts

    parts = relation_parts(relation_id)
    quoted = ".".join(f'"{parts[key]}"' for key in ("database", "schema", "identifier"))

    def encode(value):
        if isinstance(value, (date, datetime, time, Decimal)):
            return {"type": type(value).__name__, "value": str(value)}
        if isinstance(value, bytes):
            return {"type": "bytes", "value": value.hex()}
        raise TypeError(f"지원하지 않는 Snowflake 결과 타입: {type(value).__name__}")

    row_hashes = []
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {quoted}")
        # 컬럼 이름과 타입도 포함해 빈 테이블의 스키마 변경을 구분한다.
        columns = [(column[0], column[1]) for column in cursor.description]
        query_id = cursor.sfqid
        if not query_id:
            raise RuntimeError("Snowflake query ID가 없습니다")
        while rows := cursor.fetchmany(1000):
            for row in rows:
                # Connector는 VARIANT/OBJECT/ARRAY를 JSON 문자열로 반환할 수 있다.
                # JSON null은 SQL NULL과 구분하고 객체 key 순서는 정규화한다.
                values = [
                    {"snowflake_json": json.loads(value) if isinstance(value, str) else value}
                    if columns[index][1] in (5, 9, 10) and value is not None else value
                    for index, value in enumerate(row)
                ]
                encoded = json.dumps(values, default=encode, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode("utf-8")
                row_hashes.append(hashlib.sha256(encoded).digest())
    digest = hashlib.sha256(json.dumps(columns, separators=(",", ":")).encode("utf-8"))
    for row_hash in sorted(row_hashes):
        digest.update(row_hash)
    return {"row_count": len(row_hashes), "content_sha256": digest.hexdigest(), "query_id": query_id}


def validate_candidate_relation(connection: Any, relation_id: str) -> CandidateRelationStats:
    """build claim에 기록된 대상만 조회하며, 없는 테이블/권한 오류는 그대로 실패시킨다."""
    # 현재 claim_build가 생성하는 unquoted 식별자만 허용한다. 외부 입력을 SQL로 쓰지 않는다.
    if not re.fullmatch(
        r"POP_TALK_DW_DEV\.CANDIDATE\.[A-Z0-9_]+__B[A-Z0-9]{12}__A[1-9][0-9]*__F[1-9][0-9]*",
        relation_id,
    ):
        raise ValueError("build candidate 테이블 식별자가 아닙니다")
    quoted = ".".join(f'"{part}"' for part in relation_id.split("."))
    with connection.cursor() as cursor:
        # 데이터를 worker로 가져오지 않고 Snowflake에서 집계한다. 빈 결과도 유효하다.
        cursor.execute(f"SELECT COUNT(*) FROM {quoted}")
        row = cursor.fetchone()
        query_id = cursor.sfqid
    if row is None or type(row[0]) is not int or row[0] < 0 or not query_id:
        raise RuntimeError("candidate 행 수 또는 Snowflake query ID를 확인할 수 없습니다")
    return CandidateRelationStats(relation_id, row[0], query_id)
