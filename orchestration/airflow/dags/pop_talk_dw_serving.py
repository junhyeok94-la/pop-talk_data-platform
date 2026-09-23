"""원천 적재 이후 DW 계산·검사·서빙을 한 DAG에서 완료한다."""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context
from pipelines.orchestration.dbt_deployment import load_current_deployment
from pipelines.paths import DBT_DEPLOYMENT_ROOT
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, DbtResourceType
from airflow.providers.standard.operators.python import PythonOperator


def execute_model(model):
    from pipelines.orchestration.dw_pipeline import run_dbt
    return run_dbt(get_current_context()["ti"].xcom_pull(task_ids="prepare"), model)


def model_task(dag, task_group, node, task_id, **kwargs):
    # Cosmos는 모델 의존성을 그리며 실행은 기존 고정 입력·배포본을 사용한다.
    return PythonOperator(task_id=task_id, dag=dag, task_group=task_group,
        python_callable=execute_model, op_args=[node.unique_id.rsplit(".", 1)[1]],
        doc_md=node.unique_id)


DEPLOYMENT = load_current_deployment(DBT_DEPLOYMENT_ROOT)


def _snowflake():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id="pop_talk_snowflake").get_conn()


@dag(
    dag_id="pop_talk_dw_serving", schedule=None,
    start_date=pendulum.datetime(2026, 9, 11, tz="Asia/Seoul"), catchup=False,
    is_paused_upon_creation=True, max_active_runs=1, dagrun_timeout=timedelta(hours=3),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=40)},
    tags=["pop-talk", "dw", "serving"],
)
def dw_serving():
    @task(multiple_outputs=False, task_display_name='Snowflake 입력 시점 고정')
    def prepare(identity):
        from pipelines.orchestration.dw_pipeline import prepare_inputs
        connection = _snowflake()
        try:
            return prepare_inputs(connection, identity, get_current_context()["run_id"])
        finally:
            connection.close()

    @task
    def execute(stage):
        # 실행 입력은 고정 XCom에서 읽고 실행 순서는 아래 체인으로 명시한다.
        inputs = get_current_context()["ti"].xcom_pull(task_ids="prepare")
        from pipelines.orchestration.dw_pipeline import run_dbt
        return run_dbt(inputs, stage)

    @task(task_display_name='PostgreSQL dev · 영화·흥행 반영')
    def publish():
        inputs = get_current_context()["ti"].xcom_pull(task_ids="prepare")
        import os
        import psycopg
        from pipelines.orchestration.dw_pipeline import publish_serving
        connection = _snowflake()
        try:
            with psycopg.connect(host=os.environ["POP_TALK_POSTGRES_HOST"],
                port=int(os.environ.get("POP_TALK_POSTGRES_PORT", "5432")),
                dbname=os.environ["POP_TALK_POSTGRES_DB"], user=os.environ["POP_TALK_POSTGRES_USER"],
                password=os.environ["POP_TALK_POSTGRES_PASSWORD"], connect_timeout=5) as postgres:
                return publish_serving(inputs, connection, postgres, "dw:" + get_current_context()["run_id"])
        finally:
            connection.close()

    # 배포 ID를 serialized task 인자로 고정한다. 입력/상태 전달은 XCom과 태스크 의존성만 사용한다.
    inputs = prepare({"deployment_id": DEPLOYMENT.deployment_id, "manifest_sha256": DEPLOYMENT.manifest_sha256})
    models = DbtTaskGroup(
        group_id="dw",
        project_config=ProjectConfig(dbt_project_path=DEPLOYMENT.project_path,
            manifest_path=DEPLOYMENT.manifest_path),
        profile_config=ProfileConfig(profile_name=DEPLOYMENT.profile_name,
            target_name=DEPLOYMENT.target_name,
            profiles_yml_filepath=DEPLOYMENT.project_path / "profiles.yml"),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.NONE, emit_datasets=False,
            group_nodes_by_folder=True, node_converters={DbtResourceType.MODEL: model_task}),
    )
    checks = execute.override(task_id="quality_checks", task_display_name="dbt 전체 품질 검사")("test")
    inputs >> models >> checks >> publish()



dw_serving()
