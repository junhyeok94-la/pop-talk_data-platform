"""Read-only monitoring adapter over the persisted execution-environment contract."""

import json
from airflow_workbench.dashboard.database import Store
from airflow_workbench.shared.execution_contract import dag_ids


def environment_index():
    with Store().connection() as db:
        records = db.execute(
            "SELECT value FROM documents WHERE key LIKE 'environment:%'"
        ).fetchall()
    result = {}
    for row in records:
        spec = json.loads(row[0])
        for kind, dag_id in dag_ids(spec["dag_prefix"], spec["workloads"]).items():
            result[dag_id] = (spec["id"], kind)
    return result
