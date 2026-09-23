"""실제 query API와 외부 GPU/deferral/cancel 경로를 검증한다. 학습은 실행하지 않는다."""

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, "/opt/airflow/plugins")
from airflow_workbench import worker_client


def worker(method, path, payload=None):
    return asyncio.run(worker_client.call(method, path, payload))


def main():
    result = {"checked_at": time.time(), "checks": {}}
    c = httpx.Client(base_url="http://127.0.0.1:8080", timeout=30, trust_env=False)
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

    def api(method, path, payload=None):
        r = c.request(method, path, json=payload)
        assert r.is_success, (path, r.status_code, r.text[:2000])
        return r.json()

    boards = api("GET", "/workbench/api/studio/boards")
    for board in boards:
        report = api("POST", "/workbench/api/studio/boards/query", board)
        assert all(
            "error" not in frame for panel in report.values() for frame in panel
        ), report
    result["checks"]["default_board_queries"] = True
    print("Default dashboard queries passed", flush=True)
    status = api("GET", "/workbench/api/mlops/status")
    assert all(d.get("contract_valid") for d in status["dags"]), status["dags"]
    assert status["pool"]["include_deferred"] is True, status["pool"]
    assert status["worker"]["ram_limit_mib"] == 6144, status["worker"]
    result["checks"]["dag_pool_resource_contract"] = True
    api("POST", "/workbench/api/mlops/dags/diagnostic/activate")
    bad = c.post(
        "/workbench/api/mlops/diagnostic",
        json={"request_id": str(uuid.uuid4()), "required_gpu_mib": 20000},
    )
    assert bad.status_code == 409, bad.text
    assert "GPU" in bad.text, bad.text
    result["checks"]["insufficient_vram_rejected"] = True
    request = {"request_id": str(uuid.uuid4()), "required_gpu_mib": 256}
    run = api("POST", "/workbench/api/mlops/diagnostic", request)
    again = api("POST", "/workbench/api/mlops/diagnostic", request)
    assert run["dag_run_id"] == again["dag_run_id"]
    result["checks"]["dag_submission_idempotent"] = True
    result["diagnostic_run"] = {k: run[k] for k in ("dag_id", "dag_run_id")}
    print("Diagnostic DAG queued", flush=True)
    path = f"/api/v2/dags/{run['dag_id']}/dagRuns/{run['dag_run_id']}"
    deadline = time.monotonic() + 150
    saw_deferred = False
    while time.monotonic() < deadline:
        ti = api("GET", path + "/taskInstances")["task_instances"]
        if any(t["state"] == "deferred" for t in ti):
            pool = api("GET", "/api/v2/pools/model_lab_gpu")
            assert pool["occupied_slots"] == 1, pool
            saw_deferred = True
        state = api("GET", path)["state"]
        if state in {"success", "failed"}:
            break
        time.sleep(1)
    assert state == "success", (state, ti)
    assert saw_deferred, "DAG did not free worker via deferral"
    jobs = worker("GET", "/jobs")
    matching = [
        j
        for j in jobs
        if j["status"] == "success" and j["created"] >= result["checked_at"]
    ]
    assert (
        matching and matching[0]["result"]["cuda_probe"]["allocated_mib"] == 64
    ), matching
    result["checks"]["external_cuda_and_airflow_deferral"] = True
    result["cuda_job"] = matching[0]
    print("External CUDA and Airflow deferral passed", flush=True)
    spec = {
        "key": "smoke_" + uuid.uuid4().hex,
        "kind": "diagnostic",
        "profile": "diagnostic",
    }
    job = worker("POST", "/jobs", spec)
    assert worker("POST", "/jobs", spec)["id"] == job["id"]
    for method, path2, payload in [
        ("POST", "/jobs", {**spec, "key": "other_" + uuid.uuid4().hex}),
        ("POST", "/reservations", None),
    ]:
        try:
            worker(method, path2, payload)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 409, str(exc)
        else:
            raise AssertionError("GPU contention allowed")
    time.sleep(2)
    cancelled = api("POST", "/workbench/api/mlops/jobs/" + job["id"] + "/cancel")
    assert cancelled["status"] == "cancelled", cancelled
    time.sleep(1)
    assert worker("GET", "/jobs/" + job["id"])["status"] == "cancelled"
    assert worker("GET", "/resources")["active_job"] is None
    result["checks"]["cancel_and_atomic_gpu_admission"] = True
    reservation = worker("POST", "/reservations")
    try:
        try:
            worker("POST", "/jobs", {**spec, "key": "blocked_" + uuid.uuid4().hex})
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 409
        else:
            raise AssertionError("training bypassed inference reservation")
    finally:
        worker("POST", "/reservations/" + reservation["id"] + "/release")
    result["checks"]["inference_reservation_blocks_training"] = True
    host, _ = worker_client.connection()
    assert httpx.get(host + "/resources", trust_env=False).status_code == 401
    result["checks"]["worker_authentication"] = True
    destination = Path("/opt/airflow/workbench/qa-v2.json")
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
