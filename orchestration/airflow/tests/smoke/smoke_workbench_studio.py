"""Read-only local acceptance checks. No GPU job, account, grant, or DAG is created."""

import json
import os
import time
from pathlib import Path

import httpx


def main():
    base = "http://127.0.0.1:8080"
    report = {"checked_at": time.time(), "checks": {}, "boards": []}
    with httpx.Client(base_url=base, timeout=60, trust_env=False) as c:
        r = c.get("/workbench/api/studio/boards")
        assert r.status_code == 401, r.status_code
        report["checks"]["authentication_required"] = True
        token = c.post(
            "/auth/token",
            json={
                "username": os.environ["_AIRFLOW_WWW_USER_USERNAME"],
                "password": os.environ["_AIRFLOW_WWW_USER_PASSWORD"],
            },
        )
        token.raise_for_status()
        c.headers.update(
            {
                "Authorization": "Bearer " + token.json()["access_token"],
                "X-Workbench-Request": "1",
            }
        )
        r = c.get("/workbench/api/studio/boards")
        r.raise_for_status()
        boards = r.json()
        for b in boards:
            r = c.post("/workbench/api/studio/boards/query", json=b)
            r.raise_for_status()
            errors = [
                f["error"] for fs in r.json().values() for f in fs if "error" in f
            ]
            assert not errors, errors
            report["boards"].append(
                {
                    "title": b["title"],
                    "version": b["version"],
                    "panels": len(b["panels"]),
                    "result_rows": sum(
                        len(f["rows"]) for fs in r.json().values() for f in fs
                    ),
                }
            )
        report["checks"]["real_default_board_queries"] = True
        configuration = c.get("/workbench/api/config")
        configuration.raise_for_status()
        assert configuration.json()["dashboard_scope"] == "personal"
        assert configuration.json()["can_edit_dashboard"] is True
        preferences = c.get("/workbench/api/studio/preferences")
        preferences.raise_for_status()
        assert preferences.json()["last_board_id"] in {b["id"] for b in boards}
        report["checks"]["authenticated_personal_dashboard_scope"] = True
        sources = c.get("/workbench/api/studio/catalog")
        sources.raise_for_status()
        report["sources"] = [
            {k: s.get(k) for k in ["id", "dialect", "kind", "error"]}
            for s in sources.json()
        ]
        assert all(s["dialect"] != "SQLite" for s in report["sources"])
        report["checks"]["no_sqlite_query_datasource"] = True
        r = c.post("/workbench/api/query", json={"sql": "SELECT 1"})
        assert r.status_code == 410, r.text
        report["checks"]["legacy_sql_disabled"] = True
        r = c.get("/workbench/models")
        r.raise_for_status()
        assert "topbar" not in r.text and "nav-models" not in r.text
        r = c.get("/workbench/dashboard")
        r.raise_for_status()
        assert "studio-vendor.js" in r.text and "topbar" not in r.text
        report["checks"]["no_duplicate_workbench_navigation"] = True
        r = c.post(
            "/workbench/api/studio/query",
            headers={"Origin": "https://outside.example"},
            json={"query": {"builder": {"measures": [{"op": "count"}]}}},
        )
        assert r.status_code == 403, r.text
        report["checks"]["cross_origin_rejected"] = True
        for asset in [
            "theme.js",
            "theme.css",
            "studio.js",
            "studio-vendor.js",
            "studio-vendor.css",
            "studio.css",
            "THIRD-PARTY-NOTICES.txt",
        ]:
            r = c.get("/workbench/static/" + asset)
            r.raise_for_status()
        report["checks"]["bundled_assets_served"] = True
        r = c.get("/workbench/api/mlops/status")
        r.raise_for_status()
        status = r.json()
        assert all(d.get("contract_valid") for d in status["dags"]), status["dags"]
        assert status["pool"]["include_deferred"] is True
        report["checks"]["model_lab_dag_contract_and_pool"] = True
        report["worker_ram_mib"] = status["worker"].get("ram_limit_mib")
        report["legacy_boards_preserved"] = len(c.get("/workbench/api/boards").json())
        assert report["legacy_boards_preserved"] >= 3
    Path("/opt/airflow/workbench/studio-validation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
