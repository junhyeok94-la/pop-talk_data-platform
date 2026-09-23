"""로컬 Airflow Workbench 실제 API 검사. --run-models는 작은 GPU 실험 2건을 저장한다."""

import argparse
import json
import os
from datetime import datetime, timezone

import httpx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-models", action="store_true")
    args = parser.parse_args()
    origin = "http://127.0.0.1:8080"
    result = {"checked_at": datetime.now(timezone.utc).isoformat(), "checks": {}}
    with httpx.Client(base_url=origin, timeout=30, trust_env=False) as client:
        for route in (
            "/workbench/dashboard",
            "/workbench/api/models",
            "/workbench/static/workbench.js",
        ):
            response = client.get(route)
            assert response.status_code in {401, 403}, (route, response.status_code)
        result["checks"]["anonymous_denied"] = True
        auth = client.post(
            "/auth/token",
            json={
                "username": os.environ["_AIRFLOW_WWW_USER_USERNAME"],
                "password": os.environ["_AIRFLOW_WWW_USER_PASSWORD"],
            },
        )
        auth.raise_for_status()
        client.headers["Authorization"] = "Bearer " + auth.json()["access_token"]
        response = client.get("/workbench/api/config")
        response.raise_for_status()
        config = response.json()
        assert config["can_edit"], config
        result["checks"]["admin_edit_allowed"] = True
        response = client.get("/workbench/api/snapshot")
        response.raise_for_status()
        snapshot = response.json()
        assert not snapshot["errors"], snapshot["errors"]
        result["checks"]["dag_runs"] = len(snapshot["runs"])
        result["checks"]["models"] = [m["name"] for m in snapshot["models"]["models"]]
        dashboard = client.get("/workbench/api/dashboard").json()
        assert client.put("/workbench/api/dashboard", json=dashboard).status_code == 403
        result["checks"]["csrf_header_required"] = True
        response = client.get("/api/v2/plugins")
        response.raise_for_status()
        assert "airflow_workbench" in response.text
        result["checks"]["plugin_registered"] = True
        if args.run_models:
            client.headers["X-Workbench-Request"] = "1"
            experiments = [
                (
                    "embedding",
                    {
                        "name": "Workbench smoke · BGE-M3",
                        "models": ["bge-m3:latest"],
                        "query": "space and family",
                        "documents": [
                            "A parent explores space to return to their family.",
                            "A chef opens a restaurant.",
                        ],
                        "relevant_indices": [0],
                        "k": 1,
                    },
                ),
                (
                    "generation",
                    {
                        "name": "Workbench smoke · Qwen3",
                        "models": ["qwen3:8b"],
                        "prompt": "한국어로 '연결 테스트 성공'이라고만 답하세요.",
                        "options": {"num_predict": 32},
                    },
                ),
            ]
            result["experiments"] = []
            for kind, spec in experiments:
                response = client.post(
                    "/workbench/api/experiments/" + kind, json=spec, timeout=650
                )
                response.raise_for_status()
                record = response.json()
                assert record["status"] == "success", record.get("error")
                model = record["result"]["models"][0]
                result["experiments"].append(
                    {
                        "id": record["id"],
                        "kind": kind,
                        "model": model["model"],
                        "digest": model["digest"],
                        "latency_seconds": model["latency_seconds"],
                        "dimension": model.get("dimension"),
                        "metrics": model.get("metrics"),
                        "output": model.get("output"),
                    }
                )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
