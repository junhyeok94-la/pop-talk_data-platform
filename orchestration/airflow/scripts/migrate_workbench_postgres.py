"""Migrate only Workbench data into the configured Airflow PostgreSQL metadata DB.

Run outside DAG tasks. Default: review counts. --apply: transactionally import an
empty destination, verify every row, and retain the read-only SQLite source.
Stop the API server before applying to prevent writes to the old database.
"""

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, "/opt/airflow/plugins")
from airflow_workbench import metadata


def migrate_sqlite(path, apply=False):
    path = Path(path).resolve(strict=True)
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as source:
        tables = {
            r[0]
            for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        records = {}
        for table in metadata.TABLES.keys() - {"schema_migrations"}:
            if table not in tables:
                continue
            cursor = source.execute(f'SELECT * FROM "{table}"')
            columns = [c[0] for c in cursor.description]
            records[table] = [dict(zip(columns, row)) for row in cursor.fetchall()]
        if any(r["status"] == "running" for r in records.get("experiments", [])):
            raise RuntimeError("Finish/reconcile active experiments before migration.")
        if any(r["expires"] > time.time() for r in records.get("leases", [])):
            raise RuntimeError("Wait for active experiment leases before migration.")
    digest = hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()
    report = {
        "backend": "Airflow metadata PostgreSQL",
        "schema": "workbench",
        "source": str(path),
        "counts": {k: len(v) for k, v in records.items()},
        "content_sha256": digest,
        "applied": False,
    }
    if not apply:
        return report
    metadata.migrate()
    with metadata.connection() as db:
        db.lock("sqlite-migration")
        previous = db.execute(
            "SELECT detail FROM schema_migrations WHERE version=2"
        ).fetchone()
        if previous:
            if previous[0] != digest:
                raise RuntimeError("A different SQLite source was already imported.")
            return {**report, "applied": True, "already_imported": True}
        for table in records:
            if db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]:
                raise RuntimeError(
                    "Destination must be empty before initial import: " + table
                )
        for table, rows in records.items():
            for row in rows:
                columns = list(row)
                db.execute(
                    f'INSERT INTO "{table}" ({",".join(columns)}) VALUES ({",".join("?" for _ in columns)})',
                    tuple(row.values()),
                )
            restored = [dict(r) for r in db.execute(f'SELECT * FROM "{table}"')]
            normalize = lambda values: sorted(
                json.dumps(r, sort_keys=True) for r in values
            )
            if normalize(restored) != normalize(rows):
                raise RuntimeError("Content verification failed: " + table)
        db.execute(
            "INSERT INTO schema_migrations(version,detail) VALUES (2,?)", (digest,)
        )
    return {**report, "applied": True, "every_row_verified": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            migrate_sqlite(args.sqlite, args.apply), ensure_ascii=False, indent=2
        )
    )
