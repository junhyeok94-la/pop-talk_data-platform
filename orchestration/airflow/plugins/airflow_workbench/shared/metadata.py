"""Workbench tables in the configured Airflow PostgreSQL database.

Only the plugin API/administration process uses this module. DAG tasks use the
Task SDK and external executors; they never connect to the metadata database.
"""

import re
from contextvars import ContextVar
from contextlib import contextmanager

from sqlalchemy import text

connection_override = ContextVar("workbench_connection_override", default=None)

TABLES = {
    "dashboard_actions": "id TEXT PRIMARY KEY, owner TEXT NOT NULL, created DOUBLE PRECISION NOT NULL, expires DOUBLE PRECISION NOT NULL, status TEXT NOT NULL, value TEXT NOT NULL",
    "dashboard_cache": "key TEXT PRIMARY KEY, value TEXT, sampled DOUBLE PRECISION NOT NULL DEFAULT 0, expires DOUBLE PRECISION NOT NULL DEFAULT 0, lease_owner TEXT, lease_until DOUBLE PRECISION NOT NULL DEFAULT 0",
    "lab_records": "project TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL, version INTEGER NOT NULL, created DOUBLE PRECISION NOT NULL, value TEXT NOT NULL, PRIMARY KEY(project,kind,id)",
    "documents": "key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL",
    "experiments": "id TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, actor TEXT NOT NULL, created DOUBLE PRECISION NOT NULL, finished DOUBLE PRECISION, status TEXT NOT NULL, config TEXT NOT NULL, result TEXT, error TEXT",
    "leases": "name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires DOUBLE PRECISION NOT NULL",
    "executor_jobs": "environment_id TEXT NOT NULL, id TEXT NOT NULL, key TEXT NOT NULL, hash TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('queued','running','cancelling','cancelled','interrupted','rejected','timeout','failed','success')), created DOUBLE PRECISION NOT NULL, updated DOUBLE PRECISION NOT NULL, request TEXT NOT NULL, result TEXT, error TEXT, PRIMARY KEY(environment_id,id), UNIQUE(environment_id,key)",
    "executor_reservations": "environment_id TEXT NOT NULL, id TEXT NOT NULL, expires DOUBLE PRECISION NOT NULL, PRIMARY KEY(environment_id,id)",
    "studio_boards": "owner TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(owner,id)",
    "studio_revisions": "owner TEXT NOT NULL, board_id TEXT NOT NULL, version INTEGER NOT NULL, value TEXT NOT NULL, PRIMARY KEY(owner,board_id,version)",
    "studio_templates": "owner TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(owner,id)",
    "studio_preferences": "owner TEXT PRIMARY KEY, last_board_id TEXT, legacy_imported INTEGER NOT NULL DEFAULT 0",
    "schema_migrations": "version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), detail TEXT NOT NULL",
}


def engine():
    from airflow import settings

    if settings.engine.dialect.name != "postgresql":
        raise RuntimeError(
            "Workbench requires the Airflow PostgreSQL metadata database."
        )
    return settings.engine


def schema_name(value):
    if not re.fullmatch(r"(?:workbench|wb_test_[a-f0-9]{32})", value):
        raise ValueError(
            "Workbench schema must be workbench or an isolated test schema."
        )
    return value


class Row(dict):
    def __getitem__(self, key):
        return (
            tuple(self.values())[key]
            if isinstance(key, int)
            else super().__getitem__(key)
        )


class Result:
    def __init__(self, result):
        self.result = result
        self.rowcount = result.rowcount

    def fetchone(self):
        row = self.result.fetchone()
        return Row(row._mapping) if row is not None else None

    def fetchall(self):
        return [Row(row._mapping) for row in self.result.fetchall()]

    def __iter__(self):
        return iter(self.fetchall())


class Database:
    def __init__(self, connection, schema):
        self.connection, self.schema = connection, schema

    def execute(self, query, parameters=()):
        # Existing repository queries use positional binds. All SQL is application
        # code, never user-entered SQL; SQLAlchemy binds values without interpolation.
        pieces = query.split("?")
        if len(pieces) != len(parameters) + 1:
            raise ValueError("SQL parameter count mismatch")
        statement = pieces[0] + "".join(
            f":p{i}" + part for i, part in enumerate(pieces[1:])
        )
        return Result(
            self.connection.execute(
                text(statement), {f"p{i}": v for i, v in enumerate(parameters)}
            )
        )

    def executemany(self, query, rows):
        for row in rows:
            self.execute(query, row)

    def lock(self, key):
        self.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(?,0))",
            (self.schema + ":" + key,),
        )


@contextmanager
def connection(schema="workbench", *, database_engine=None):
    schema = schema_name(schema)
    override = connection_override.get()
    if override is not None and database_engine is None:
        with override(schema) as db:
            yield db
        return
    with (database_engine if database_engine is not None else engine()).begin() as conn:
        conn.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        yield Database(conn, schema)


def migrate(schema="workbench"):
    """Explicit versioned setup. Does not edit Airflow's tables or Alembic version."""
    schema = schema_name(schema)
    with engine().begin() as conn:
        conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
            {"key": schema + ":migration"},
        )
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        for table, columns in TABLES.items():
            conn.execute(
                text(f'CREATE TABLE IF NOT EXISTS "{schema}".{table} ({columns})')
            )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS dashboard_actions_owner_idx ON "{schema}".dashboard_actions(owner,created DESC)'
            )
        )
        conn.execute(
            text(
                f"INSERT INTO \"{schema}\".schema_migrations(version,detail) VALUES (6,'Personal work queue and retry receipts') ON CONFLICT(version) DO NOTHING"
            )
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS experiments_created_idx ON "{schema}".experiments(created DESC)'
            )
        )
        conn.execute(
            text(
                f"INSERT INTO \"{schema}\".schema_migrations(version,detail) VALUES (1,'PostgreSQL Workbench schema') ON CONFLICT(version) DO NOTHING"
            )
        )
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS executor_one_active_idx ON \"{schema}\".executor_jobs(environment_id) WHERE status IN ('queued','running','cancelling')"
            )
        )
        conn.execute(
            text(
                f"INSERT INTO \"{schema}\".schema_migrations(version,detail) VALUES (3,'Environment-scoped executor state API') ON CONFLICT(version) DO NOTHING"
            )
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS lab_records_listing_idx ON "{schema}".lab_records(project,kind,created DESC)'
            )
        )
        conn.execute(
            text(
                f"INSERT INTO \"{schema}\".schema_migrations(version,detail) VALUES (4,'Project-scoped Model Lab workflow') ON CONFLICT(version) DO NOTHING"
            )
        )
        conn.execute(
            text(
                f"INSERT INTO \"{schema}\".schema_migrations(version,detail) VALUES (5,'Bounded dashboard aggregate cache') ON CONFLICT(version) DO NOTHING"
            )
        )


if __name__ == "__main__":
    migrate()
    print("Workbench PostgreSQL schema ready")
