"""Real auth and mounted Airflow app, in a fresh process without server restart.

Only reads saved configuration and native DAG metadata. A synthetic circuit is
opened in this process, never in the live API process. No DAG actions submitted.
"""

import concurrent.futures
import json
import time
from pathlib import Path

from airflow.api_fastapi.app import create_app
from fastapi.testclient import TestClient

from airflow_workbench.dashboard import protection
from prepare_model_lab_workflow import client as authenticated_client


def main():
    signed_in = authenticated_client()
    try:
        headers = dict(signed_in.headers)
    finally:
        signed_in.close()
    root = create_app()
    protection.guard.reset()
    checks = {}
    with TestClient(root, base_url="http://127.0.0.1:8080", headers=headers) as client:

        def read(path):
            response = client.get(path)
            assert response.status_code == 200, (
                path,
                response.status_code,
                response.text[:500],
            )
            return response.json()

        result = read("/workbench/api/work/summary")
        checks["work_summary"] = {
            "dags": len(result["dags"]),
            "stale": result.get("stale", False),
        }
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            values = list(
                pool.map(
                    read,
                    [
                        "/workbench/api/studio/boards",
                        "/workbench/api/studio/catalog",
                        "/workbench/api/studio/preferences",
                    ],
                )
            )
        board = values[0][0]
        # The global budget deliberately spaces different cold aggregate keys.
        time.sleep(protection.policy.compute_gap + 0.1)
        response = client.post("/workbench/api/studio/boards/query", json=board)
        assert response.status_code == 200, (response.status_code, response.text[:500])
        frames = [f for group in response.json().values() for f in group]
        assert frames and not any(f.get("error") for f in frames), frames
        checks["chart_frames"] = len(frames)
        checks["parallel_studio_startup"] = "passed"
        protection.guard.trip("database_active")
        denied = client.get("/workbench/api/work/summary")
        assert denied.status_code == 503 and denied.headers.get("retry-after")
        checks["dashboard_during_pressure"] = denied.status_code
        # Existing stable API still authorizes normally and can inspect the run.
        native = client.get("/api/v2/dags", params={"limit": 1})
        assert native.status_code == 200, native.text[:500]
        checks["native_dag_api_during_pressure"] = native.status_code
        checks["scope"] = (
            "Mounted fresh Airflow app with real account JWT and PostgreSQL, in-process TestClient. Live API unchanged; no DAG mutations."
        )
    print(json.dumps(checks, indent=2))
    Path("/opt/airflow/workbench/dashboard-protection-auth.json").write_text(
        json.dumps(checks, indent=2)
    )


if __name__ == "__main__":
    main()
