"""Prepare the local Airflow database before starting the platform."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg
from psycopg import sql


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable: {name}")
    return value


def main() -> None:
    print("Preparing Airflow database...", flush=True)
    with psycopg.connect(
        host=required("POP_TALK_PLATFORM_POSTGRES_HOST"),
        port=int(os.environ.get("POP_TALK_PLATFORM_POSTGRES_PORT", "5432")),
        dbname=required("POP_TALK_PLATFORM_POSTGRES_DB"),
        user=required("POP_TALK_PLATFORM_ADMIN_USER"),
        password=required("POP_TALK_PLATFORM_ADMIN_PASSWORD"),
        autocommit=True,
    ) as connection:
        airflow_user = required("AIRFLOW_DB_USER")
        exists = connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (airflow_user,)).fetchone()
        statement = "ALTER ROLE {} LOGIN PASSWORD {}" if exists else "CREATE ROLE {} LOGIN PASSWORD {}"
        connection.execute(sql.SQL(statement).format(
            sql.Identifier(airflow_user), sql.Literal(required("AIRFLOW_DB_PASSWORD"))))
        if connection.execute("SELECT 1 FROM pg_database WHERE datname='airflow'").fetchone() is None:
            connection.execute(sql.SQL("CREATE DATABASE airflow OWNER {}").format(sql.Identifier(airflow_user)))
        connection.execute(sql.SQL("ALTER DATABASE airflow OWNER TO {}").format(sql.Identifier(airflow_user)))
        # Environment checks only need a connection, not access to platform tables.
        connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(required("POP_TALK_PLATFORM_POSTGRES_DB")), sql.Identifier(airflow_user)))
        pipeline_user = required('POP_TALK_PLATFORM_POSTGRES_USER')
        if pipeline_user in (airflow_user, required('POP_TALK_PLATFORM_ADMIN_USER')):
            raise RuntimeError('Platform runtime role must be separate from administrator and Airflow')
        exists = connection.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (pipeline_user,)).fetchone()
        statement = 'ALTER ROLE {} LOGIN PASSWORD {}' if exists else 'CREATE ROLE {} LOGIN PASSWORD {}'
        connection.execute(sql.SQL(statement).format(sql.Identifier(pipeline_user),
            sql.Literal(required('POP_TALK_PLATFORM_POSTGRES_PASSWORD'))))
        connection.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(
            sql.Identifier(required('POP_TALK_PLATFORM_POSTGRES_DB')), sql.Identifier(pipeline_user)))
        with connection.transaction():
            connection.execute(Path('/opt/airflow/modules/pipelines/platform/schema.sql').read_text())
            for schema in ('ops', 'stg', 'dw', 'mart'):
                connection.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(
                    sql.Identifier(schema), sql.Identifier(pipeline_user)))
            connection.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO {}').format(
                sql.Identifier(pipeline_user)))
            connection.execute(sql.SQL('REVOKE UPDATE ON ALL TABLES IN SCHEMA stg FROM {}').format(
                sql.Identifier(pipeline_user)))
            connection.execute(sql.SQL('GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA stg TO {}').format(
                sql.Identifier(pipeline_user)))
            for schema in ('dw', 'mart'):
                connection.execute(sql.SQL('GRANT CREATE ON SCHEMA {} TO {}').format(
                    sql.Identifier(schema), sql.Identifier(pipeline_user)))

    os.environ.pop("POP_TALK_PLATFORM_ADMIN_USER")
    os.environ.pop("POP_TALK_PLATFORM_ADMIN_PASSWORD")
    print("Migrating Airflow and preparing administrator, Workbench and model pool...", flush=True)
    os.environ["_AIRFLOW_DB_MIGRATE"] = "true"
    os.execv("/entrypoint", ["/entrypoint", "bash", "-ec",
        'python -m airflow_workbench.metadata\n'
        'airflow pools set model_lab_gpu 1 "Model Lab external GPU jobs" --include-deferred'])


if __name__ == "__main__":
    main()
