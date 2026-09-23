"""One-time import of a STOPPED worker's SQLite queue into Airflow PostgreSQL.

Uses the existing Airflow connection; never provisions users or credentials.
The read-only source is retained as an archive. Default is a read-only plan.
"""

import argparse
import hashlib
import json
import sqlite3
import time
from pathlib import Path

from airflow_workbench import environments, metadata


def migrate(path, environment_id, apply=False):
    environments.resolve(environment_id)
    path = Path(path).resolve(strict=True)
    rows = {}
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as source:
        source.row_factory = sqlite3.Row
        for table in ("jobs", "reservations"):
            rows[table] = [
                dict(row) for row in source.execute(f"SELECT * FROM {table}")
            ]
    if any(
        r["status"] in {"queued", "running", "cancelling"} for r in rows["jobs"]
    ) or any(r["expires"] > time.time() for r in rows["reservations"]):
        raise RuntimeError(
            "Complete jobs and reservations, then stop the worker before importing"
        )
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
    report = {
        "environment_id": environment_id,
        "counts": {k: len(v) for k, v in rows.items()},
        "sha256": digest,
        "applied": False,
    }
    if not apply:
        return report
    metadata.migrate()
    with metadata.connection() as db:
        db.lock("executor:" + environment_id)
        marker = "executor-migration:" + environment_id
        previous = db.execute(
            "SELECT value FROM documents WHERE key=?", (marker,)
        ).fetchone()
        if previous:
            if previous[0] != json.dumps({"sha256": digest}):
                raise RuntimeError("A different worker source was already imported")
            return {**report, "applied": True, "already_imported": True}
        for old, records in rows.items():
            target = "executor_" + old
            if old == "jobs":
                records = [
                    {("request" if k == "spec" else k): v for k, v in record.items()}
                    for record in records
                ]
            if db.execute(
                f"SELECT count(*) FROM {target} WHERE environment_id=?",
                (environment_id,),
            ).fetchone()[0]:
                raise RuntimeError("Environment destination must be empty")
            for record in records:
                value = {"environment_id": environment_id, **record}
                db.execute(
                    f"INSERT INTO {target} ({','.join(value)}) VALUES ({','.join('?' for _ in value)})",
                    tuple(value.values()),
                )
            restored = [
                {k: v for k, v in dict(r).items() if k != "environment_id"}
                for r in db.execute(
                    f"SELECT * FROM {target} WHERE environment_id=?", (environment_id,)
                )
            ]
            normalize = lambda values: sorted(
                json.dumps(r, sort_keys=True) for r in values
            )
            if normalize(restored) != normalize(records):
                raise RuntimeError("Row verification failed: " + old)
        db.execute(
            "INSERT INTO documents VALUES (?,?,1)",
            (marker, json.dumps({"sha256": digest})),
        )
    return {**report, "applied": True, "every_row_verified": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(migrate(args.sqlite, args.environment, args.apply), indent=2))
