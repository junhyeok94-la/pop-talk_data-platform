"""기존 초기 스냅샷을 현재 STAGING에 적재하고 DW·PostgreSQL 서빙 완료까지 기다린다.

S3 원문 검증 → 기존 Databricks 변환 → 검증된 Exchange → STAGING 적재·대사 → DW·서빙.
초기 API 재수집 없이 기존 5,985건을 사용하며 최신 일일 관측과 서비스 운영 데이터는 보존한다.
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sdk import Connection, Param, dag, get_current_context, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

AWS_CONN_ID = "pop_talk_aws"
DATABRICKS_CONN_ID = "pop_talk_databricks"
SNOWFLAKE_CONN_ID = "pop_talk_snowflake"
BUCKET = "amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an"
PROJECT_ROOT = "/opt/airflow/modules"
NOTEBOOK_DIRECTORY = "/Shared/pop-talk/movie_legacy_candidate_versions"
DEFAULT_MANIFEST_KEY = (
    "manifests/legacy_snapshot/v1/"
    "sha256=7e0e303fa1224ea15d864b90c0f98677089c3fddd3dc7e709f6785cf52a651fc/"
    "SUCCESS.json"
)


def _runtime():
    """네트워크 없는 DAG parse를 위해 프로젝트 runtime을 태스크 실행 때 import한다."""
    root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", PROJECT_ROOT))
    from pipelines.orchestration import movie_legacy_candidate_runtime

    return root, movie_legacy_candidate_runtime


def _databricks_credentials() -> tuple[str, str]:
    """PAT는 Connection에서만 읽고 XCom·Params·로그로 반환하지 않는다."""
    connection = Connection.get(DATABRICKS_CONN_ID)
    if not connection.host or not connection.password:
        raise RuntimeError("Databricks connection에는 host와 access token이 필요합니다")
    return connection.host, connection.password


@dag(
    dag_id="pop_talk_movie_initial_load",
    description="초기 5,985건을 현재 STAGING에 적재하고 DW·서빙 완료까지 확인",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Seoul"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["pop-talk", "initial", "serving", "manual"],
    params={
        "manifest_key": Param(
            DEFAULT_MANIFEST_KEY,
            type="string",
            pattern=(
                r"^manifests/legacy_snapshot/v1/sha256=[0-9a-f]{64}/"
                r"SUCCESS[.]json$"
            ),
            description="대사 완료된 Legacy SUCCESS manifest의 정확한 S3 key",
        ),
    },
)
def movie_initial_load():
    """초기 적재는 수동 실행하며, 마지막 태스크는 DW·서빙의 성공까지 확인한다."""

    @task(multiple_outputs=False)
    def deploy_notebook() -> dict[str, str]:
        """artifact digest 경로에 동일 bytes의 notebook만 생성·재사용한다."""
        project_root, runtime = _runtime()
        host, token = _databricks_credentials()
        return runtime.deploy_notebook(
            project_root=project_root,
            host=host,
            token=token,
            notebook_directory=NOTEBOOK_DIRECTORY,
        )

    @task(multiple_outputs=False, max_active_tis_per_dag=1)
    def stage_bundle(deployed: dict[str, str]) -> dict[str, object]:
        """manifest/source를 GET 전에 allowlist하고 검증된 bundle만 Volume에 쓴다."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        project_root, runtime = _runtime()
        host, token = _databricks_credentials()
        params = get_current_context()["params"]
        return runtime.stage_bundle(
            project_root=project_root,
            s3_client=S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
            databricks_host=host,
            databricks_token=token,
            bucket=BUCKET,
            manifest_key=str(params["manifest_key"]),
            publication_revision=1,
            processing_attempt=1,
            deployed=deployed,
        )

    deployed = deploy_notebook()
    staged = stage_bundle(deployed)

    # 단일 candidate 입력이므로 Jinja는 각 dict field를 같은 stage XCom에서 읽는다.
    # submit token은 Airflow run ID가 아니라 입력 identity로 만들어 cross-run replay도 같다.
    transformed = DatabricksSubmitRunOperator(
        task_id="transform_candidate_bronze_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        polling_period_seconds=10,
        databricks_retry_limit=2,
        max_active_tis_per_dag=1,
        idempotency_token="{{ ti.xcom_pull(task_ids='stage_bundle')['submit_token'] }}",
        json={
            "run_name": (
                "pop-talk-movie-legacy-candidate-"
                "{{ ti.xcom_pull(task_ids='stage_bundle')['source_sha256'][:12] }}"
            ),
            "performance_target": "STANDARD",
            "tasks": [{
                "task_key": "movie_legacy_candidate",
                "notebook_task": {
                    "notebook_path": (
                        "{{ ti.xcom_pull(task_ids='deploy_notebook')['notebook_path'] }}"
                    ),
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "landing_dir": "{{ ti.xcom_pull(task_ids='stage_bundle')['landing_dir'] }}",
                        "manifest_key": "{{ params.manifest_key }}",
                        "artifact_version": (
                            "{{ ti.xcom_pull(task_ids='stage_bundle')['artifact_version'] }}"
                        ),
                        "source_sha256": (
                            "{{ ti.xcom_pull(task_ids='stage_bundle')['source_sha256'] }}"
                        ),
                        "archive_sha256": (
                            "{{ ti.xcom_pull(task_ids='stage_bundle')['archive_sha256'] }}"
                        ),
                        "publication_revision": "{{ ti.xcom_pull(task_ids='stage_bundle')['publication_revision'] }}",
                        "processing_attempt": "{{ ti.xcom_pull(task_ids='stage_bundle')['processing_attempt'] }}",
                    },
                },
                "timeout_seconds": 3600,
            }],
        },
    )
    staged >> transformed

    @task(multiple_outputs=False, max_active_tis_per_dag=1)
    def publish_exchange() -> dict[str, object]:
        """원격 재계산이 성공한 bundle을 candidate S3 prefix에 READY-last 게시한다."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        staged_result = get_current_context()["ti"].xcom_pull(task_ids="stage_bundle")
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

    published = publish_exchange()
    transformed >> published

    @task(multiple_outputs=False, max_active_tis_per_dag=1)
    def load_snowflake(published_result: dict[str, object]) -> dict[str, object]:
        """현재 STAGING의 observation과 SUCCESS ledger를 하나의 트랜잭션으로 적재한다."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        from pipelines.orchestration.movie_initial_load import load_initial_snapshot
        connection = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        try:
            return load_initial_snapshot(
                s3_client=S3Hook(aws_conn_id=AWS_CONN_ID).get_conn(),
                snowflake_connection=connection, bucket=BUCKET, published=published_result,
            )
        finally:
            connection.close()

    @task(multiple_outputs=False)
    def confirm_initial_load(
        loaded_result: dict[str, object],
    ) -> dict[str, object]:
        """세 저장소의 lineage와 5,985/4,320/1,665 건수를 최종 대사한다."""
        ti = get_current_context()["ti"]
        staged_result = ti.xcom_pull(task_ids="stage_bundle")
        published_result = ti.xcom_pull(task_ids="publish_exchange")
        identities = {
            (
                str(result["source_run_id"]),
                str(result["source_sha256"]),
                str(result["artifact_version"]),
                int(result["publication_revision"]),
            )
            for result in (staged_result, published_result, loaded_result)
        }
        if len(identities) != 1:
            raise RuntimeError("초기 stage/exchange/load lineage가 다릅니다")
        if (
            int(staged_result["movie_count"]) != 5985
            or int(staged_result["eligible_count"]) != 4320
            or int(staged_result["excluded_count"]) != 1665
            or int(published_result["movie_count"]) != 5985
            or int(published_result["boxoffice_count"]) != 0
            or int(loaded_result["movie_count"]) != 5985
        ):
            raise RuntimeError("초기 기준선 건수 대사가 실패했습니다")
        return {
            "status": "SUCCESS",
            "source_run_id": loaded_result["source_run_id"],
            "source_sha256": loaded_result["source_sha256"],
            "artifact_version": loaded_result["artifact_version"],
            "publication_revision": loaded_result["publication_revision"],
            "movie_count": 5985,
            "eligible_count": 4320,
            "excluded_count": 1665,
            "boxoffice_count": 0,
            "exchange_ready_key": loaded_result["exchange_ready_key"],
        }

    loaded = load_snowflake(published)
    confirmed = confirm_initial_load(loaded)
    # 재시도 시 동일한 하위 run을 재실행하고, 실제 서빙 완료를 부모 성공 조건으로 삼는다.
    serving = TriggerDagRunOperator(
        task_id="build_dw_and_serve", trigger_dag_id="pop_talk_dw_serving",
        trigger_run_id="initial__{{ run_id }}", reset_dag_run=True,
        wait_for_completion=True, deferrable=True, poke_interval=15,
        execution_timeout=timedelta(hours=3),
    )
    confirmed >> serving


movie_initial_load()
