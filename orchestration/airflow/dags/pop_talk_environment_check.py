"""Read-only checks for raw collection files and the platform PostgreSQL connection."""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
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
            "platform/ingest.py", "platform/build.py", "platform/schema.sql",
        )]
        required_paths.append(Path(os.environ.get('POP_TALK_DBT_PROJECT_DIR','/opt/airflow/dbt'))/'dbt_project.yml')
        missing = [str(path) for path in required_paths if not path.is_file()]
        if missing:
            raise RuntimeError(f"Required project files are missing: {missing}")
        return str(root)

    @task
    def check_platform_postgres(project_root: str) -> str:
        del project_root
        from pipelines.platform.database import connect
        with connect() as connection, connection.transaction():
            connection.execute('SET TRANSACTION READ ONLY')
            database, user = connection.execute("SELECT current_database(), current_user").fetchone()
            connection.execute('SELECT load_id FROM ops.source_loads LIMIT 0')
            connection.execute('SELECT load_id FROM stg.movie_observations LIMIT 0')
        return f"database={database}, user={user}"

    check_platform_postgres(check_project_mount())


environment_check()
