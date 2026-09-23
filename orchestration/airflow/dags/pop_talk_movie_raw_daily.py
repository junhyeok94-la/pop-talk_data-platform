"""일일 영화 변경 후보를 수집하고 검증된 S3 Raw 실행으로 게시한다.

흐름::

    수집 계획 -> KOFIC 후보 -> KOFIC 상세 -> KMDb 후보 -> DAILY_READY 검증

각 단계는 원문과 stage manifest를 불변 S3 key에 저장한다. 마지막 검증 태스크가
성공해 ``DAILY_READY.json``을 게시해야 후속 Bronze/Silver DAG가 이 실행을 읽을 수
있다. 재시도는 같은 plan의 이미 검증된 S3 객체를 재사용한다.
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import Asset, Connection, Param, dag, get_current_context, task

AWS_CONN_ID = "pop_talk_aws"
KOFIC_CONN_ID = "pop_talk_kofic"
KMDB_CONN_ID = "pop_talk_kmdb"
BUCKET = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
BUSINESS_TIMEZONE = "Asia/Seoul"
DAILY_MOVIE_RAW_READY_URI = (
    f"s3://{BUCKET}/manifests/movie_api_daily/v1/DAILY_READY"
)
DAILY_MOVIE_RAW_READY_ASSET = Asset(
    uri=DAILY_MOVIE_RAW_READY_URI,
    name="pop_talk_daily_movie_raw_ready",
)


def _collector(plan: dict[str, object]) -> Any:
    """태스크 실행 시점에 인증 client와 일일 수집기를 조립한다."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    project_root = os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)

    from pipelines.collectors.movie_daily import DailyCollector
    from pipelines.collectors.movie_raw import ApiClient, S3Store

    store = S3Store(
        S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
        BUCKET,
    )
    api = ApiClient(
        Connection.get(KOFIC_CONN_ID).password,
        Connection.get(KMDB_CONN_ID).password,
    )
    return DailyCollector(store, api, plan)


@dag(
    dag_id="pop_talk_movie_raw_daily",
    description="KOFIC/KMDb 일일 변경 후보를 불변 S3 Raw와 DAILY_READY로 게시",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 9, 9, tz=BUSINESS_TIMEZONE),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=8),
    },
    tags=["pop-talk", "raw", "daily"],
    params={
        "days_back": Param(7, type="integer", minimum=1, maximum=31),
        "max_movies": Param(
            0,
            type="integer",
            minimum=0,
            maximum=1000,
            description="0: 일일 후보 전체, 양수: smoke 표본 상한",
        ),
        "kmdb_max_pages": Param(3, type="integer", minimum=1, maximum=10),
    },
)
def daily_raw():
    """업무 날짜를 고정한 뒤 단계별 manifest를 순서대로 완성한다."""

    @task(multiple_outputs=False, task_display_name='수집 기준일·범위 고정')
    def plan_collection() -> dict[str, object]:
        """Params와 기준일을 불변 plan으로 만든다. 이후 재시도는 이 plan을 공유한다."""
        project_root = os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)

        from pipelines.collectors.movie_daily import daily_plan

        context = get_current_context()
        dag_run = context["dag_run"]
        # 정기 실행은 data interval 끝, 수동 실행은 시작 시각을 업무 기준일로 삼는다.
        anchor = (
            context["data_interval_end"]
            if str(dag_run.run_type) == "scheduled"
            else dag_run.start_date
        )
        collection_date = (
            pendulum.instance(anchor)
            .in_timezone(BUSINESS_TIMEZONE)
            .date()
            .isoformat()
        )
        return daily_plan(
            context["params"],
            context["run_id"],
            collection_date,
        )

    @task(task_display_name="KOFIC 일별 박스오피스 수집")
    def collect_boxoffice(plan):
        """기본 최근 7일의 순위·관객·매출 원문을 S3에 보존한다."""
        return _collector(plan).boxoffice()

    @task(task_display_name="박스오피스 + 올해 장편 영화 후보 선정")
    def collect_candidates(boxoffice_manifest: str) -> str:
        """KOFIC 목록에서 일일 재확인 후보를 수집하고 stage manifest를 반환한다."""
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        return _collector(plan).movie_list()

    @task(task_display_name='KOFIC 영화 상세 수집')
    def collect_details(candidates: str) -> str:
        """후보의 KOFIC 상세를 수집한다. candidates XCom은 완료 의존성을 보장한다."""
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        del candidates
        return _collector(plan).details()

    @task(task_display_name='KMDb 상세 후보 수집')
    def collect_kmdb(details: str) -> str:
        """상세 수집이 끝난 영화의 KMDb 후보를 수집하며 확정 매칭은 하지 않는다."""
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        del details
        return _collector(plan).kmdb_candidates()

    @task(multiple_outputs=False, outlets=[DAILY_MOVIE_RAW_READY_ASSET], task_display_name='수집 검증 · S3 READY 발행')
    def validate_daily(
        kmdb: str,
        *,
        outlet_events,
    ) -> dict[str, str]:
        """모든 stage를 대사하고 READY 게시 뒤 Asset event를 발행한다.

        실패하거나 skip된 attempt는 event를 만들지 않는다. 성공 태스크를 clear하여 다시
        실행하면 같은 READY event가 중복될 수 있으므로 소비 DAG가 논리 중복을 제거한다.
        """
        plan = get_current_context()["ti"].xcom_pull(task_ids="plan_collection")
        del kmdb
        collector = _collector(plan)
        ready_key = collector.validate_run()
        from pipelines.orchestration.asset_contract import build_asset_event_extra

        context = get_current_context()
        outlet_events[DAILY_MOVIE_RAW_READY_ASSET].extra = build_asset_event_extra(
            plan=plan,
            bucket=BUCKET,
            ready_manifest_key=ready_key,
            source_dag_id=context["dag"].dag_id,
            source_run_id=context["run_id"],
        )
        return {
            "bucket": BUCKET,
            "manifest_key": ready_key,
        }

    plan = plan_collection()
    boxoffice = collect_boxoffice(plan)
    candidates = collect_candidates(boxoffice)
    details = collect_details(candidates)
    kmdb = collect_kmdb(details)
    validate_daily(kmdb)


daily_raw()
