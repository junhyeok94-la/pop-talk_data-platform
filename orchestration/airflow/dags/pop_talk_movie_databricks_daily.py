"""S3 Raw를 변환하고 Snowflake 적재 후 DW·서빙 DAG에 인계한다.
이 DAG 성공은 원천 적재와 모델 실행 요청 완료를 뜻한다. DW 계산과 PostgreSQL
serving 활성화는 pop_talk_dw_serving이 담당한다.
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.sdk import Asset, Connection, Param, dag, get_current_context, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

AWS_CONN_ID = "pop_talk_aws"
DATABRICKS_CONN_ID = "pop_talk_databricks"
SNOWFLAKE_CONN_ID = "pop_talk_snowflake"
BUCKET = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
DEFAULT_READY_KEY = (
    "manifests/movie_api_daily/v1/collection_date=2026-09-09/"
    "run_id=4834bb8200e5a76f2cf8b408/DAILY_READY.json"
)
NOTEBOOK_DIRECTORY = "/Shared/pop-talk/movie_bronze_silver_daily_versions"
DAILY_MOVIE_RAW_READY_URI = (
    f"s3://{BUCKET}/manifests/movie_api_daily/v1/DAILY_READY"
)
DAILY_MOVIE_RAW_READY_ASSET = Asset(
    uri=DAILY_MOVIE_RAW_READY_URI,
    name="pop_talk_daily_movie_raw_ready",
)

def _runtime() -> tuple[Path, Any]:
    """DAG 파싱 중 I/O를 피하도록 태스크 실행 시점에 프로젝트 코드를 불러온다."""
    project_root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT))

    from pipelines.orchestration import movie_daily_runtime

    return project_root, movie_daily_runtime


def _databricks_credentials() -> tuple[str, str]:
    """Airflow가 관리하는 Databricks host와 PAT를 조회하고 필수값을 검증한다."""
    connection = Connection.get(DATABRICKS_CONN_ID)
    if not connection.host or not connection.password:
        raise RuntimeError("Databricks connection에는 host와 access token이 모두 필요합니다")
    return connection.host, connection.password


@dag(
    dag_id="pop_talk_movie_databricks_daily",
    description="검증된 S3 Raw를 Databricks로 변환하고 Snowflake 적재 후 DW DAG에 인계",
    schedule=[DAILY_MOVIE_RAW_READY_ASSET],
    start_date=pendulum.datetime(2026, 9, 9, tz="Asia/Seoul"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["pop-talk", "bronze", "silver", "databricks"],
    params={
        "ready_manifest_key": Param(
            DEFAULT_READY_KEY,
            type="string",
            pattern=r"^manifests/movie_api_daily/.+/DAILY_READY[.]json$",
        ),
        "publication_revision": Param(1, type="integer", minimum=1),
        "processing_attempt": Param(1, type="integer", minimum=1),
        # 기존 장애 시나리오 검증과 호환하기 위해 이름과 기본값을 유지한다.
        "snowflake_fail_after_movie_merge": Param(False, type="boolean"),
    },
)
def movie_databricks_daily():
    """각 외부 시스템의 게시 경계를 보존하면서 전체 표본 파이프라인을 구성한다."""

    @task(multiple_outputs=False, task_display_name='Raw READY · 처리할 수집 배치 확인')
    def resolve_ready_inputs(triggering_asset_events=None) -> list[dict[str, object]]:
        """이번 run을 만든 Asset event들을 검증해 mapped READY 목록으로 고정한다.

        Asset 실행은 이번 DagRun의 triggering events만 사용한다. 수동 실행만 기존 Param을
        허용하며, 잘못된 event를 최신 Asset이나 기본 Param으로 조용히 대체하지 않는다.
        """
        from airflow.configuration import conf
        from pipelines.orchestration.asset_contract import resolve_ready_inputs

        context = get_current_context()
        normalized_events = []
        for asset, events in (triggering_asset_events or {}).items():
            for event in events:
                source_dag_run = event.source_dag_run
                normalized_events.append(
                    {
                        "asset_uri": asset.uri,
                        "source_dag_id": source_dag_run.dag_id,
                        "source_run_id": source_dag_run.run_id,
                        "extra": event.extra,
                    }
                )
        return resolve_ready_inputs(
            run_type=str(context["dag_run"].run_type),
            events=normalized_events,
            manual_ready_key=str(context["params"]["ready_manifest_key"]),
            expected_asset_uri=DAILY_MOVIE_RAW_READY_URI,
            expected_bucket=BUCKET,
            max_map_length=conf.getint("core", "max_map_length"),
        )

    @task(multiple_outputs=False, task_display_name='Databricks 실행 코드 배포')
    def deploy_notebook() -> list[dict[str, object]]:
        """digest별 노트북을 배포한다. 재시도하면 같은 불변 경로를 재사용한다."""
        project_root, runtime = _runtime()
        host, token = _databricks_credentials()
        notebook = runtime.deploy_notebook(
            project_root=project_root,
            host=host,
            token=token,
            notebook_directory=NOTEBOOK_DIRECTORY,
        )
        # 다음 mapped 단계가 바로 앞 태스크의 출력만으로 입력 개수와 배포본을 받는다.
        ti = get_current_context()["ti"]
        ti.xcom_push(key="notebook", value=notebook)
        return [{"deployed": notebook, "ready_input": ready}
                for ready in ti.xcom_pull(task_ids="resolve_ready_inputs")]

    @task(multiple_outputs=False, max_active_tis_per_dag=1, task_display_name='S3 원천 입력 묶음 준비')
    def stage_bundle(
        deployed: dict[str, object],
        ready_input: dict[str, object],
    ) -> dict[str, object]:
        """Raw를 검증·staging하고 동일 원격 시도를 식별할 token을 반환한다."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        project_root, runtime = _runtime()
        context = get_current_context()
        host, token = _databricks_credentials()
        return runtime.stage_bundle(
            project_root=project_root,
            s3_client=S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
            databricks_host=host,
            databricks_token=token,
            bucket=BUCKET,
            ready_key=str(ready_input["ready_manifest_key"]),
            expected_raw_run_id=str(ready_input["raw_run_id"]),
            publication_revision=int(context["params"]["publication_revision"]),
            processing_attempt=int(context["params"]["processing_attempt"]),
            deployed=deployed,
        )

    ready_inputs = resolve_ready_inputs()
    deployed = deploy_notebook()
    staged = stage_bundle.expand_kwargs(deployed)

    @task(multiple_outputs=False, task_display_name='원격 실행 요청 준비')
    def prepare_transform_requests(
        staged_results,
    ) -> list[dict[str, object]]:
        """각 stage 결과를 Databricks mapped operator의 완성된 요청 하나로 만든다.

        json과 idempotency token을 같은 dict에 묶어 map index의 Cartesian product나
        mapped Jinja 조회를 방지한다.
        """
        deployed_notebook = get_current_context()["ti"].xcom_pull(task_ids="deploy_notebook", key="notebook")
        requests = []
        for result in staged_results:
            requests.append(
                {
                    "json": {
                        "run_name": f"pop-talk-movie-bronze-silver-{result['run_id']}",
                        "performance_target": "STANDARD",
                        "tasks": [
                            {
                                "task_key": "movie_bronze_silver",
                                "notebook_task": {
                                    "notebook_path": deployed_notebook["notebook_path"],
                                    "source": "WORKSPACE",
                                    "base_parameters": {
                                        "landing_dir": result["landing_dir"],
                                        "ready_manifest_key": result["ready_key"],
                                        "artifact_version": result[
                                            "artifact_version"
                                        ],
                                        "publication_revision": str(
                                            result["publication_revision"]
                                        ),
                                        "processing_attempt": str(
                                            result["processing_attempt"]
                                        ),
                                    },
                                },
                                "timeout_seconds": 3600,
                            }
                        ],
                    },
                    "idempotency_token": result["submit_token"],
                }
            )
        return requests

    transform_requests = prepare_transform_requests(staged)

    # 이 Operator가 원격 Jobs run의 상태를 추적한다. 같은 Airflow 시도의 재시도는
    # 동일 token을 사용하므로 Databricks 작업을 불필요하게 중복 생성하지 않는다.
    transformed = DatabricksSubmitRunOperator.partial(
        task_id="transform_bronze_silver", task_display_name='Databricks · Bronze → Silver 영화·흥행 정제',
        databricks_conn_id=DATABRICKS_CONN_ID,
        polling_period_seconds=10,
        databricks_retry_limit=2,
        max_active_tis_per_dag=1,
    ).expand_kwargs(transform_requests)

    @task(multiple_outputs=False, max_active_tis_per_dag=1, task_display_name='Silver 결과 · S3 Exchange 게시')
    def publish_exchange(staged_result: dict[str, object]) -> dict[str, object]:
        """Silver 파일을 S3 불변 key에 쓴 뒤 마지막에 EXCHANGE_READY를 게시한다."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        project_root, runtime = _runtime()
        host, token = _databricks_credentials()
        return runtime.publish_exchange(
            project_root=project_root,
            s3_client=S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
            databricks_host=host,
            databricks_token=token,
            bucket=BUCKET,
            staged=staged_result,
        )

    # Databricks operator는 return_value가 없다. stage 출력으로 정확한 매핑 수를 유지한다.
    published = publish_exchange.expand(staged_result=staged)
    transformed >> published

    @task(multiple_outputs=False, max_active_tis_per_dag=1, task_display_name='Snowflake STAGING · 영화·흥행 적재')
    def load_snowflake(exchange_result: dict[str, object]) -> dict[str, object]:
        """두 observation 테이블과 적재 ledger를 하나의 DML 트랜잭션으로 반영한다."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        _, runtime = _runtime()
        connection = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        try:
            return runtime.load_snowflake(
                s3_client=S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
                snowflake_connection=connection,
                bucket=BUCKET,
                exchange=exchange_result,
                fail_after_movie_merge=bool(
                    get_current_context()["params"][
                        "snowflake_fail_after_movie_merge"
                    ]
                ),
            )
        finally:
            connection.close()

    loaded = load_snowflake.expand(exchange_result=published)

    @task(multiple_outputs=False, task_display_name='원천 ↔ 적재 결과 대사')
    def confirm_loaded_batch(
        loaded_results,
    ) -> dict[str, object]:
        """모든 READY가 동일 artifact/revision으로 Snowflake까지 갔는지 대사한다.

        이 ALL_SUCCESS barrier 뒤에서만 DW·서빙 DAG를 한 번 요청한다.
        """
        from pipelines.orchestration.asset_contract import confirm_loaded_batch

        ti = get_current_context()["ti"]
        ready = list(ti.xcom_pull(task_ids="resolve_ready_inputs"))
        # 인덱스 목록을 명시해야 mapped 결과가 1건이어도 dict 대신 목록으로 온다.
        return confirm_loaded_batch(
            ready_inputs=ready,
            staged_results=list(ti.xcom_pull(task_ids="stage_bundle", map_indexes=list(range(len(ready))))),
            loaded_results=list(loaded_results),
            publication_revision=int(
                get_current_context()["params"]["publication_revision"]
            ),
        )

    loaded_batch = confirm_loaded_batch(loaded)

    # 최종 serving 완료는 pop_talk_dw_serving에서 확인한다.
    start_models = TriggerDagRunOperator(
        task_id="start_model_generation", task_display_name='DW 모델·서비스 반영 실행', trigger_dag_id="pop_talk_dw_serving",
        trigger_run_id="daily__{{ run_id }}", skip_when_already_exists=True,
        wait_for_completion=False,
    )
    ready_inputs >> deployed
    loaded_batch >> start_models


movie_databricks_daily()
