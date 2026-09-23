"""Read-only checks for raw collection files and the platform PostgreSQL connection."""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
import psycopg
from airflow.sdk import dag, task


@dag(
    dag_id="pop_talk_environment_check",
    description="Platform PostgreSQL and raw collection mount smoke test",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 9, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["pop-talk", "smoke-test"],
)
def environment_check():
    @task
    def check_project_mount() -> str:
        root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", "/opt/airflow/modules"))
        required_paths = [root / "pipelines" / relative for relative in (
            "collectors/movie_raw.py", "collectors/movie_daily.py",
            "transforms/movie_bronze_silver.py", "orchestration/asset_contract.py",
        )]
        missing = [str(path) for path in required_paths if not path.is_file()]
        if missing:
            raise RuntimeError(f"Required project files are missing: {missing}")
        return str(root)

    @task
    def check_platform_postgres(project_root: str) -> str:
        del project_root
        # Reuse the Airflow login for a connection-only check; no administrator credentials.
        with psycopg.connect(
            host=os.environ["POP_TALK_PLATFORM_POSTGRES_HOST"],
            port=int(os.environ.get("POP_TALK_PLATFORM_POSTGRES_PORT", "5432")),
            dbname=os.environ["POP_TALK_PLATFORM_POSTGRES_DB"],
            user=os.environ["AIRFLOW_DB_USER"],
            password=os.environ["AIRFLOW_DB_PASSWORD"],
            connect_timeout=5,
            options="-c default_transaction_read_only=on",
        ) as connection:
            database, user = connection.execute("SELECT current_database(), current_user").fetchone()
        return f"database={database}, user={user}"

    check_platform_postgres(check_project_mount())


environment_check()
