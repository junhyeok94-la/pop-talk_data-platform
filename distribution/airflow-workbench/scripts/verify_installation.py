"""Check a fresh or existing installation without submitting inference/training jobs."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys



def snapshot():
    from airflow_workbench.shared.metadata import engine
    from sqlalchemy import text
    with engine().connect() as db:
        return {
            "versions": list(db.execute(text("SELECT * FROM workbench.schema_migrations ORDER BY version"))),
            "records": list(db.execute(text("SELECT * FROM workbench.lab_records ORDER BY project,kind,id"))),
            "users": list(db.execute(text("SELECT id,username,password FROM ab_user ORDER BY id"))),
        }


def check_dags(dags, expected, existing=False):
    found = {d["dag_id"]: d for d in dags}
    missing = expected - found.keys()
    assert not missing, "Missing Workbench DAGs: " + ", ".join(sorted(missing))
    if not existing:
        assert found.keys() == expected, "Use --existing when other DAGs are installed"
        assert all(d["is_paused"] for d in dags), "Fresh installation DAGs should be paused; use --existing after enabling them"
    assert not any(found[key].get("has_import_errors") or found[key].get("is_stale") for key in expected), "Workbench DAG import or source error"


def main():
    from airflow_workbench.shared.metadata import engine
    from airflow_workbench.model_lab import environments
    from sqlalchemy import text
    import httpx
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://airflow-apiserver:8080")
    parser.add_argument("--worker", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--repeat-init", action="store_true", help="Reinitialize only a disposable fresh installation")
    mode.add_argument("--existing", action="store_true", help="Read-only checks; preserve projects, settings and DAG pause states")
    args = parser.parse_args()
    with engine().connect() as db:
        project_count = db.execute(text("SELECT count(*) FROM workbench.lab_records WHERE kind='project'")).scalar_one()
        if not args.existing:
            assert project_count == 0, "Use --existing to check an installation with projects"
    if args.repeat_init:
        before = snapshot()
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("initialize.py"))], capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError("Initialization failed:\n" + result.stdout[-3000:] + result.stderr[-3000:])
        assert snapshot() == before, "Reinitialization changed existing records or administrator password"
        print("Repeated initialization preserves schema versions, records and administrator password.")
    with engine().connect() as db:
        if not args.existing:
            assert db.execute(text("SELECT count(*) FROM import_error")).scalar_one() == 0
        versions = [r[0] for r in db.execute(text("SELECT version FROM workbench.schema_migrations ORDER BY version"))]
        # Migration numbers are identifiers; the current schema never used 2.
        assert versions == [1, 3, 4, 5, 6], versions
    with httpx.Client(base_url=args.url, timeout=60) as client:
        response = client.post("/auth/token", json={"username": os.environ["AIRFLOW_ADMIN_USERNAME"], "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})
        response.raise_for_status()
        client.headers["Authorization"] = "Bearer " + response.json()["access_token"]
        for path in ("/workbench/models", "/workbench/dashboard", "/workbench/monitoring", "/workbench/static/shared/host.js", "/workbench/static/dashboard/studio-vendor.js", "/workbench/api/lab/bootstrap"):
            result = client.get(path)
            assert result.status_code == 200, (path, result.status_code)
        bootstrap = client.get("/workbench/api/lab/bootstrap").json()
        if not args.existing:
            assert bootstrap["projects"] == []
        permissions = bootstrap["evaluation_permissions"]
        assert all(p["can_trigger"] for p in permissions.values()), "Administrator lacks evaluation DAG permission"
        expected = {p["dag_id"] for p in permissions.values()}
        if args.worker:
            expected.update(dag_id for spec in environments.configured() for dag_id in spec.dag_ids().values())
        if args.existing:
            dags = []
            for dag_id in sorted(expected):
                response = client.get("/api/v2/dags/" + dag_id)
                response.raise_for_status()
                dags.append(response.json())
        else:
            response = client.get("/api/v2/dags", params={"limit": 100})
            response.raise_for_status()
            dags = response.json()["dags"]
        check_dags(dags, expected, existing=args.existing)
        for endpoint_id in permissions:
            response = client.get("/workbench/api/lab/endpoints/" + endpoint_id + "/status")
            response.raise_for_status()
            status = response.json()
            assert status["settings"]["state"] == "ready", endpoint_id + ": " + status["settings"]["message"]
        response = client.get("/workbench/api/lab/connection-options")
        response.raise_for_status()
        allowed = {"connection_id", "source", "source_label", "resolved", "valid"}
        assert all(set(c) <= allowed for c in response.json()["connections"]), "Connection catalog contains unexpected fields"
        print("Authenticated Workbench pages/assets/API: OK. DAGs:", ", ".join(sorted(expected)))
    if args.worker:
        # Both admission and resources round-trip through the worker state API.
        token = json.loads(os.environ["AIRFLOW_CONN_MODEL_LAB_WORKER"])["password"]
        with httpx.Client(base_url="http://model-worker:8091", timeout=30) as client:
            assert client.get("/resources").status_code == 401
            response = client.get("/resources", headers={"Authorization": "Bearer " + token})
            response.raise_for_status()
            assert response.json()["state_backend"] == "Airflow API / PostgreSQL"
        print("Separate worker authentication and PostgreSQL state API round-trip: OK.")
    print("Installation smoke checks passed.")
    if args.existing:
        print("Existing projects and DAG pause states were preserved; no initialization or job submission was performed.")


if __name__ == "__main__":
    main()
