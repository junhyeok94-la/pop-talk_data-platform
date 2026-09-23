"""Exercise only the dedicated, side-effect-free workbench_retry_verification DAG."""

import json
import time
import uuid

from prepare_model_lab_workflow import client

DAG = "workbench_retry_verification"


def main():
    c = client()
    run_id = "workbench_retry_qa_" + uuid.uuid4().hex[:12]
    dag_path = "/api/v2/dags/" + DAG
    run_path = dag_path + "/dagRuns/" + run_id

    def call(method, path, data=None):
        response = c.request(method, path, json=data)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path}: {response.status_code} {response.text[:1200]}")
        return response.json()

    def wait(state):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            value = call("GET", run_path)
            if value["state"] == state:
                return value
            time.sleep(2)
        raise RuntimeError(f"DAG did not reach {state}")

    def tasks():
        values = call("GET", run_path + "/taskInstances")["task_instances"]
        return {t["task_id"]: {k: t[k] for k in ("state", "try_number", "start_date", "end_date")} for t in values}

    initial_pause = True
    activated = False
    try:
        for _ in range(60):
            response = c.get(dag_path)
            if response.is_success:
                initial_pause = response.json()["is_paused"]
                break
            if response.status_code != 404:
                response.raise_for_status()
            time.sleep(2)
        else:
            raise RuntimeError("Verification DAG has not been parsed")
        call("PATCH", dag_path, {"is_paused": False})
        activated = True
        call("POST", dag_path + "/dagRuns", {"dag_run_id": run_id, "logical_date": None, "conf": {}})
        wait("failed")
        before = tasks()
        preview = call("POST", "/workbench/api/work/retry-preview", {"dag_id": DAG, "run_id": run_id})
        assert {t["task_id"] for t in preview["tasks"]} == {"fail_once", "downstream"}, preview
        assert tasks() == before, "Preview modified task instances"
        receipt = call("POST", "/workbench/api/work/retry", {"preview_id": preview["preview_id"], "note": "Dedicated QA: failed and upstream_failed only"})
        wait("success")
        after = tasks()
        repeated = call("POST", "/workbench/api/work/retry", {"preview_id": preview["preview_id"]})
        assert repeated == receipt
        assert tasks() == after, "Repeated receipt changed the run"
        assert after["already_succeeded"] == before["already_succeeded"]
        assert after["fail_once"]["try_number"] == before["fail_once"]["try_number"] + 1
        assert all(t["state"] == "success" for t in after.values())
        print(json.dumps({"dag_id": DAG, "run_id": run_id, "before": before, "preview_tasks": [t["task_id"] for t in preview["tasks"]], "after": after, "repeat_same_receipt": True, "successful_task_unchanged": True}, ensure_ascii=False, indent=2), flush=True)
    finally:
        if activated:
            call("PATCH", dag_path, {"is_paused": initial_pause})
        c.close()


if __name__ == "__main__":
    main()
