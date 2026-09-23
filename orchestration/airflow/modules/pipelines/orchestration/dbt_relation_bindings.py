"""모델 실행의 읽기·쓰기 테이블을 cohort에 고정한다.

S3 manifest 경로는 SQL 테이블이 아니다. source snapshot을 Snowflake에 준비한 뒤
그 테이블 위치를 전달해야 하며, 현재 STAGING 테이블로 조용히 대체하지 않는다.
"""
from __future__ import annotations

import re
from typing import Any, Mapping


def relation_parts(relation_id: str) -> dict[str, str]:
    """이 프로젝트의 격리된 candidate 스키마에 속한 식별자만 허용한다."""
    if not re.fullmatch(r"POP_TALK_DW_DEV\.CANDIDATE\.[A-Z_][A-Z0-9_]*", relation_id):
        raise ValueError("Snowflake candidate snapshot 테이블이 필요합니다")
    database, schema, identifier = relation_id.split(".")
    return {"database": database, "schema": schema, "identifier": identifier}


def build_relation_vars(
    cohort: Mapping[str, Any], target_relation: str
) -> dict[str, Any]:
    """본인 출력과 직접 부모 입력만 전달한다. REUSE도 원본 테이블을 유지한다."""
    model_id = str(cohort["model_unique_id"])
    relations = {model_id: relation_parts(target_relation)}
    for binding in cohort["parent_bindings"]:
        parent_id = str(binding["parent_unique_id"])
        if parent_id in relations:
            raise ValueError("cohort에 중복 또는 자기 자신인 부모가 있습니다")
        relations[parent_id] = relation_parts(str(binding["relation_id"]))
    return {"pop_talk_relations": relations}
