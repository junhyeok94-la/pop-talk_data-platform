"""로컬 Airflow가 프로젝트 파일과 serving PostgreSQL에 접근하는지 검사한다.

두 태스크 모두 데이터를 변경하지 않는다. 프로젝트 mount가 정상인 경우에만 PostgreSQL
연결을 검사하며, 성공 결과는 접속한 database/user 식별 정보다.
"""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
import psycopg
from airflow.sdk import dag, task

DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"
POSTGRES_ENV_NAMES = (
    "POP_TALK_POSTGRES_HOST",
    "POP_TALK_POSTGRES_PORT",
    "POP_TALK_POSTGRES_DB",
    "POP_TALK_POSTGRES_USER",
    "POP_TALK_POSTGRES_PASSWORD",
)


@dag(
    dag_id="pop_talk_environment_check",
    description="Airflow metadata DB and Pop Talk project mount smoke test",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 9, tz="UTC"),
    catchup=False,
    tags=["pop-talk", "smoke-test"],
)
def environment_check():
    """mount 검증 뒤 읽기 전용 PostgreSQL query를 실행한다."""

    @task
    def check_project_mount() -> str:
        """후속 파이프라인에 필요한 대표 파일이 container에 mount됐는지 확인한다."""
        project_root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT))
        required_paths = [
            project_root / "pipelines" / "databricks" / "01_ingest_movie_sample.py",
            project_root / "pipelines" / "snowflake" / "01_load_movie_sample.sql",
            project_root / "pipelines" / "dbt" / "dbt_project.yml",
            project_root / "pipelines" / "orchestration" / "service_sync.py",
        ]
        missing = [str(path) for path in required_paths if not path.is_file()]
        if missing:
            raise RuntimeError(f"Required project files are missing: {missing}")
        return str(project_root)

    @task
    def check_serving_postgres(project_root: str) -> str:
        """mount 성공 후 serving DB에 읽기 전용 query를 보내 연결 identity를 확인한다."""
        # 값 자체보다 upstream XCom을 받는 것이 mount 검사 성공을 의존 관계로 만든다.
        del project_root
        missing = [name for name in POSTGRES_ENV_NAMES if not os.getenv(name)]
        if missing:
            raise RuntimeError(f"Required environment variables are missing: {missing}")

        with psycopg.connect(
            host=os.environ["POP_TALK_POSTGRES_HOST"],
            port=int(os.environ["POP_TALK_POSTGRES_PORT"]),
            dbname=os.environ["POP_TALK_POSTGRES_DB"],
            user=os.environ["POP_TALK_POSTGRES_USER"],
            password=os.environ["POP_TALK_POSTGRES_PASSWORD"],
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user")
                database, user = cursor.fetchone()
        return f"database={database}, user={user}"

    check_serving_postgres(check_project_mount())


environment_check()
