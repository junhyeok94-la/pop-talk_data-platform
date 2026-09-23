"""Check local maintenance preconditions without displaying credentials."""

import asyncio
from airflow_workbench.worker_client import call
from airflow.utils.session import create_session
from airflow.models.dagrun import DagRun

r = asyncio.run(call("GET", "/resources"))
print(
    {
        "active_worker_job": r.get("active_job"),
        "reservation": r.get("inference_reserved_until"),
    }
)
assert not r.get("active_job") and not r.get("inference_reserved_until")
with create_session() as db:
    active = db.query(DagRun).filter(DagRun.state.in_(["running", "queued"])).count()
print({"active_dag_runs": active})
assert active == 0
