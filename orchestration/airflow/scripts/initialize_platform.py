"""Prepare the platform database and local artifacts before starting Airflow."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from migrate_dw_control import _migration_paths, _required, migrate


def main() -> None:
    print("Preparing platform control schema and Airflow database...", flush=True)
    # Only this one-shot container receives the platform administrator credentials.
    with psycopg.connect(
        host=_required("POP_TALK_CONTROL_POSTGRES_HOST"),
        port=int(os.environ.get("POP_TALK_CONTROL_POSTGRES_PORT", "5432")),
        dbname=_required("POP_TALK_CONTROL_POSTGRES_DB"),
        user=_required("POP_TALK_CONTROL_ADMIN_USER"),
        password=_required("POP_TALK_CONTROL_ADMIN_PASSWORD"),
        autocommit=True,
    ) as connection:
        airflow_user = _required("AIRFLOW_DB_USER")
        if connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (airflow_user,)).fetchone() is None:
            connection.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(airflow_user), sql.Literal(_required("AIRFLOW_DB_PASSWORD"))))
        else:
            # Reused volumes may retain the password from an older local configuration.
            connection.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                sql.Identifier(airflow_user), sql.Literal(_required("AIRFLOW_DB_PASSWORD"))))
        if connection.execute("SELECT 1 FROM pg_database WHERE datname='airflow'").fetchone() is None:
            connection.execute(sql.SQL("CREATE DATABASE airflow OWNER {}").format(sql.Identifier(airflow_user)))
        connection.execute(sql.SQL("ALTER DATABASE airflow OWNER TO {}").format(sql.Identifier(airflow_user)))
        migrate(connection, runtime_password=_required("POP_TALK_CONTROL_POSTGRES_PASSWORD"),
                migration_paths=_migration_paths())

    # Do not pass database administrator credentials to dbt or Airflow subprocesses.
    os.environ.pop("POP_TALK_CONTROL_ADMIN_USER")
    os.environ.pop("POP_TALK_CONTROL_ADMIN_PASSWORD")
    scripts = Path(__file__).resolve().parent
    print("Preparing local dbt execution snapshot...", flush=True)
    subprocess.run([
        sys.executable, str(scripts / "deploy_dbt_cosmos_artifact.py"),
        "--source", str(Path(_required("POP_TALK_PROJECT_ROOT")) / "pipelines" / "dbt"),
        "--output", _required("POP_TALK_DBT_DEPLOYMENT_ROOT"),
    ], check=True)
    # The image entrypoint migrates metadata before creating the first administrator.
    print("Migrating Airflow and preparing administrator, Workbench and pools...", flush=True)
    os.environ["_AIRFLOW_DB_MIGRATE"] = "true"
    os.execv("/entrypoint", ["/entrypoint", "bash", "-ec",
        'python -m airflow_workbench.metadata\n'
        'airflow pools set pop_talk_dbt_pool 1 "Serial dbt execution"\n'
        'airflow pools set model_lab_gpu 1 "Model Lab external GPU jobs" --include-deferred'])


if __name__ == "__main__":
    main()
