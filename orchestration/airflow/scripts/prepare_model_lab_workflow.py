"""Import a project profile from stdin and prepare the existing local endpoint."""

import json
import os
import sys
import time
import httpx


def client():
    c = httpx.Client(base_url="http://127.0.0.1:8080", timeout=30, trust_env=False)
    for _ in range(30):
        try:
            if c.get("/api/v2/monitor/health").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(1)
    response = c.post(
        "/auth/token",
        json={
            "username": os.environ["_AIRFLOW_WWW_USER_USERNAME"],
            "password": os.environ["_AIRFLOW_WWW_USER_PASSWORD"],
        },
    )
    response.raise_for_status()
    c.headers.update(
        {
            "Authorization": "Bearer " + response.json()["access_token"],
            "X-Workbench-Request": "1",
        }
    )
    return c


def call(c, method, path, data=None):
    r = c.request(method, "/workbench/api/lab" + path, json=data)
    if r.status_code >= 400:
        raise RuntimeError(f"{path}: {r.status_code} {r.text[:1200]}")
    return r.json()


def main():
    profile = json.load(sys.stdin)
    c = client()
    initial = call(c, "GET", "/bootstrap")
    project = profile["project"]
    if not any(p["id"] == project["id"] for p in initial["projects"]):
        call(c, "PUT", "/projects/" + project["id"], project)
    endpoint = "default-ollama"
    if not any(e["id"] == endpoint for e in initial["endpoints"]):
        call(
            c,
            "PUT",
            "/endpoints/" + endpoint,
            {
                "id": endpoint,
                "name": "로컬 Ollama",
                "provider": "ollama",
                "connection_id": "",
                "pool": "model_lab_gpu",
                "reservation_environment": "workbench_model_worker",
            },
        )
    call(c, "POST", "/endpoints/" + endpoint + "/prepare")
    print("Evaluation DAG published; waiting for DAG processor", flush=True)
    for _ in range(80):
        r = c.post("/workbench/api/lab/endpoints/" + endpoint + "/activate")
        if r.is_success:
            break
        if r.status_code not in (404, 409):
            raise RuntimeError(f"Activation failed: {r.status_code}")
        time.sleep(3)
    else:
        raise RuntimeError("DAG processor has not loaded the evaluation DAG")
    base = "/projects/" + project["id"]
    existing = call(c, "GET", base + "/dataset")
    for dataset in profile["datasets"]:
        if not any(d["name"] == dataset["name"] for d in existing):
            call(c, "POST", base + "/dataset", dataset)
    experiments = call(c, "GET", base + "/experiment")
    for experiment in profile.get("experiments", []):
        if not any(e["name"] == experiment["name"] for e in experiments):
            call(c, "POST", base + "/experiment", experiment)
        elif experiment.get("default_system"):
            existing_experiment = next(
                e for e in experiments if e["name"] == experiment["name"]
            )
            if not existing_experiment.get("default_system"):
                call(
                    c,
                    "PATCH",
                    base
                    + "/experiment/"
                    + existing_experiment["id"]
                    + "?version="
                    + str(existing_experiment["version"]),
                    experiment,
                )
    call(c, "PUT", "/preferences", {"project_id": project["id"]})
    catalog = call(c, "GET", "/endpoints/" + endpoint + "/models")
    print(
        json.dumps(
            {
                "project": project["id"],
                "models": [m["name"] for m in catalog["models"]],
                "evaluation_dag": "ready",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
