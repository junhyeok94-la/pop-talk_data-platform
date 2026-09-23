"""초기 영화 원천을 제한된 범위로 수집하고 S3 Raw 실행을 게시한다.

흐름::

    수집 계획 -> KOFIC 목록 -> KOFIC 상세 -> KMDb 후보 ─┐
                └---------------- KOFIC 박스오피스 ------┴-> SUCCESS 검증

원천 payload는 정제하지 않고 불변 S3 key에 보존한다. KMDb 결과도 이 단계에서는
확정 매칭하지 않는다. 모든 stage manifest를 대사한 ``SUCCESS.json``이 생성되어야
후속 처리에 사용할 수 있다.
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import Connection, Param, dag, get_current_context, task

AWS_CONN_ID = "pop_talk_aws"
KOFIC_CONN_ID = "pop_talk_kofic"
KMDB_CONN_ID = "pop_talk_kmdb"
BUCKET = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
BUSINESS_TIMEZONE = "Asia/Seoul"


def _runtime(plan: dict[str, object] | None = None) -> Any:
    """태스크 실행 시점에 S3/API adapter와 선택적 초기 수집기를 조립한다."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    project_root = os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)

    from pipelines.collectors.movie_raw import ApiClient, Collector, S3Store

    store = S3Store(
        S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
        BUCKET,
    )
    api = ApiClient(
        Connection.get(KOFIC_CONN_ID).password,
        Connection.get(KMDB_CONN_ID).password,
    )
    return Collector(store, api, plan) if plan is not None else (store, api)


@dag(
    dag_id="pop_talk_movie_raw_initial",
    description="KOFIC/KMDb 초기 원천을 불변 S3 Raw와 SUCCESS manifest로 게시",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 9, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=8),
    },
    tags=["pop-talk", "raw", "kofic", "kmdb", "initial"],
    params={
        "start_year": Param(
            2026,
            type="integer",
            minimum=2020,
            maximum=2200,
            title="시작 개봉연도",
        ),
        "end_year": Param(
            2026,
            type="integer",
            minimum=2020,
            maximum=2200,
            title="마지막 개봉연도",
        ),
        "max_movies_per_year": Param(
            10,
            type="integer",
            minimum=0,
            maximum=10000,
            description="10: 소량 검증, 0: 지정 연도 전체. 원천은 장르/유형을 제외하지 않음",
        ),
        "max_pages_per_year": Param(
            100,
            type="integer",
            minimum=1,
            maximum=200,
            description="안전 상한. 요청 범위 완료 전에 도달하면 실패 처리",
        ),
        "kmdb_max_pages": Param(
            3,
            type="integer",
            minimum=1,
            maximum=10,
            description="제목당 100개씩 후보 수집. 상한 초과는 truncated로 기록",
        ),
        "boxoffice_start": Param(
            "",
            type="string",
            pattern=r"^(|\d{4}-\d{2}-\d{2})$",
            description="선택: YYYY-MM-DD. 종료일과 함께 지정하며 최대 31일",
        ),
        "boxoffice_end": Param(
            "",
            type="string",
            pattern=r"^(|\d{4}-\d{2}-\d{2})$",
        ),
    },
)
def initial_raw():
    """초기 적재 범위를 고정하고 독립적인 원천 stage를 수집한다."""

    @task(multiple_outputs=False, task_display_name='초기 수집 연도·날짜 범위 고정')
    def plan_collection() -> dict[str, object]:
        """Connection을 선검증하고 Params와 한국 기준일을 불변 plan으로 만든다."""
        _runtime()

        from pipelines.collectors.movie_raw import make_plan

        context = get_current_context()
        anchor = (
            pendulum.instance(context["dag_run"].start_date)
            .in_timezone(BUSINESS_TIMEZONE)
            .date()
            .isoformat()
        )
        return make_plan(context["params"], context["run_id"], anchor)

    @task(task_display_name='KOFIC 연도별 영화 목록')
    def collect_movie_list(plan: dict[str, object]) -> str:
        """연도별 KOFIC 목록 원문과 선택 범위의 stage manifest를 저장한다."""
        return _runtime(plan).movie_list()

    @task(task_display_name='KOFIC 영화 상세')
    def collect_details(list_manifest: str) -> str:
        """목록 완료 후 선택 영화의 KOFIC 상세 원문을 수집한다."""
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        del list_manifest
        return _runtime(plan).details()

    @task(task_display_name='KMDb 상세 후보')
    def collect_kmdb_candidates(detail_manifest: str) -> str:
        """상세 영화별 KMDb 후보를 보존하며 이 단계에서는 확정 매칭하지 않는다."""
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        del detail_manifest
        return _runtime(plan).kmdb_candidates()

    @task(task_display_name='KOFIC 선택 날짜 박스오피스')
    def collect_boxoffice(plan: dict[str, object]) -> str:
        """선택 날짜의 KOFIC 일별 박스오피스를 수집하거나 명시적으로 생략한다."""
        return _runtime(plan).boxoffice()

    @task(multiple_outputs=False, task_display_name='초기 수집 검증 · S3 SUCCESS')
    def validate_raw_run(
        kmdb_manifest: str,
        boxoffice_manifest: str,
    ) -> dict[str, str]:
        """모든 stage를 대사하고 완전한 실행의 SUCCESS manifest key를 반환한다."""
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        del kmdb_manifest, boxoffice_manifest
        return {
            "bucket": BUCKET,
            "success_manifest_key": _runtime(plan).validate_run(),
        }

    plan = plan_collection()
    listing = collect_movie_list(plan)
    details = collect_details(listing)
    kmdb = collect_kmdb_candidates(details)
    boxoffice = collect_boxoffice(plan)
    validate_raw_run(kmdb, boxoffice)


initial_raw()
