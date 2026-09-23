"""Real Ollama → Airflow deferral → review/registry/config export. No training."""

import json
import time
import uuid
from pathlib import Path
from prepare_model_lab_workflow import client, call


def main():
    c = client()
    project = "model-lab-verification"
    existing = call(c, "GET", "/bootstrap")
    if not any(p["id"] == project for p in existing["projects"]):
        call(
            c,
            "PUT",
            "/projects/" + project,
            {
                "id": project,
                "name": "Model Lab 동작 검증",
                "description": "기능 검증용 실행입니다. 챗봇 모델 품질 벤치마크가 아닙니다.",
            },
        )
    base = "/projects/" + project
    exp = call(
        c,
        "POST",
        base + "/experiment",
        {
            "name": "로컬 생성·검색 실행 검증",
            "hypothesis": "Airflow DAG, 결과 기록, 기준선 비교, 등록과 구성 내보내기가 연결되는지 검증",
        },
    )
    dataset = call(
        c,
        "POST",
        base + "/dataset",
        {
            "name": "생성 계약 확인",
            "kind": "generation",
            "cases": [
                {
                    "id": "ready",
                    "input": "설명 없이 READY 한 단어만 출력하세요.",
                    "review_status": "reviewed",
                    "required_text": ["READY"],
                }
            ],
        },
    )
    runs = []

    def evaluate(dataset, model, system=""):
        payload = {
            "request_id": str(uuid.uuid4()),
            "experiment_id": exp["id"],
            "dataset_id": dataset["id"],
            "endpoint_id": "default-ollama",
            "model": model,
            "system": system,
            "max_tokens": 32,
            "k": 1,
        }
        r = call(c, "POST", base + "/evaluate", payload)
        again = call(c, "POST", base + "/evaluate", payload)
        assert r["id"] == again["id"], "duplicate run"
        print("Queued " + model + " " + r["id"], flush=True)
        saw_deferred = False
        last = None
        for _ in range(120):
            detail = call(c, "GET", base + "/run/" + r["id"])
            state = detail["status"]
            if state != last:
                print("State " + state, flush=True)
                last = state
            native = c.get(
                "/api/v2/dags/"
                + r["dag_id"]
                + "/dagRuns/"
                + r["dag_run_id"]
                + "/taskInstances"
            ).json()
            if any(t["state"] == "deferred" for t in native.get("task_instances", [])):
                saw_deferred = True
            if state in ("success", "failed", "result_missing", "cancelled"):
                break
            time.sleep(2)
        assert detail["status"] == "success", json.dumps(detail, ensure_ascii=False)[
            :5000
        ]
        assert detail["result"]["summary"]["failed"] == 0
        print("Completed " + model + " " + json.dumps(detail["summary"]), flush=True)
        runs.append(
            {
                "id": r["id"],
                "model": model,
                "summary": detail["summary"],
                "saw_deferred": saw_deferred,
            }
        )
        return detail

    baseline = evaluate(dataset, "qwen3:8b")
    call(c, "POST", base + "/run/" + baseline["id"] + "/baseline")
    candidate = evaluate(dataset, "qwen3:8b", "요청된 형식만 짧게 출력합니다.")
    compared = call(c, "GET", base + "/compare/" + candidate["id"])
    assert compared["baseline"]["id"] == baseline["id"]
    call(
        c,
        "POST",
        base + "/run/" + candidate["id"] + "/cases/ready/review",
        {
            "score": 1,
            "reason": "동작 확인용 출력 형식을 확인했습니다. 모델 품질 승격 근거가 아닙니다.",
        },
    )
    model = call(
        c,
        "POST",
        base + "/model",
        {
            "name": "동작 검증용 Qwen · 운영 적용 제외",
            "evaluation_run_id": candidate["id"],
            "notes": "플러그인 기능 검증용",
        },
    )
    call(
        c,
        "POST",
        base + "/model/" + model["id"] + "/review",
        {
            "decision": "approved",
            "reason": "기능 검증 전용 구성 생성을 확인합니다. 운영 품질 승인이 아닙니다.",
        },
    )
    release = call(
        c,
        "POST",
        base + "/release",
        {"model_version_id": model["id"], "target": "smoke-test-only"},
    )
    assert release["status"] == "configuration_ready"
    embeddings = call(
        c,
        "POST",
        base + "/dataset",
        {
            "name": "임베딩 계약 확인",
            "kind": "embedding",
            "cases": [
                {
                    "id": "password",
                    "input": "비밀번호 재설정 방법",
                    "review_status": "reviewed",
                    "relevance": {"account": 2, "notify": 0},
                }
            ],
            "corpus": [
                {
                    "id": "account",
                    "text": "비밀번호를 잊으면 로그인 화면에서 이메일 인증으로 비밀번호를 재설정합니다.",
                },
                {
                    "id": "notify",
                    "text": "알림 수신 여부는 알림 설정 메뉴에서 변경합니다.",
                },
            ],
            "corpus_version": "smoke-v1",
        },
    )
    embedded = evaluate(embeddings, "bge-m3:latest")
    assert embedded["result"]["rows"][0]["ranking"][0]["id"] == "account"
    assert embedded["summary"]["recall"] == 1
    result = {
        "project": project,
        "runs": runs,
        "release_status": release["status"],
        "checked_at": time.time(),
    }
    Path("/opt/airflow/workbench/model-lab-workflow-smoke.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2)
    )
    print(
        "PASS: real generation, embedding, idempotency, comparison, review, model version and configuration export",
        flush=True,
    )


if __name__ == "__main__":
    main()
