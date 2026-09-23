#!/usr/bin/env python3
"""current dbt deployment의 immutable model DAG registry를 control DB에 등록한다."""
from __future__ import annotations

import os
from pathlib import Path
from pipelines.paths import DBT_DEPLOYMENT_ROOT

import psycopg

from pipelines.orchestration.dbt_deployment import load_current_deployment
from pipelines.orchestration.dbt_model_registry import CURRENT_TEST_OWNER_OVERRIDES, registry_from_validated_deployment
from pipelines.orchestration.postgres_control_store import PostgresControlStore


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"필수 환경변수가 없습니다: {name}")
    return value


def main() -> None:
    project_root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", Path(__file__).resolve().parents[1] / "modules"))
    deployment = load_current_deployment(DBT_DEPLOYMENT_ROOT)
    with psycopg.connect(
        host=_required("POP_TALK_POSTGRES_HOST"),
        port=int(os.environ.get("POP_TALK_POSTGRES_PORT", "5432")),
        dbname=_required("POP_TALK_POSTGRES_DB"),
        user=_required("POP_TALK_POSTGRES_USER"),
        password=_required("POP_TALK_POSTGRES_PASSWORD"),
    ) as connection:
        store = PostgresControlStore(connection)
        # 신규 배포는 전달 대상부터 등록한다. 서빙에는 영화·일별 흥행·품질 결과만 넘긴다.
        registry = registry_from_validated_deployment(
            deployment, explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
        )
        store.register_delivery_registry(registry, terminal_recipient_work_by_model={
            f"model.pop_talk_dw.{name}": {"publication.movie_gold": "ASSEMBLE_PUBLICATION"}
            for name in ("dim_movie", "fct_boxoffice_daily", "mart_data_quality")
        })
        store.register_dbt_invocation_registry(
            deployment,
            explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
        )
    print(f"model DAG deployment registry 확인: {deployment.deployment_id}")


if __name__ == "__main__":
    main()
