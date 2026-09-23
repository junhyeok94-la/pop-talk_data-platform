"""Bounded remote inference and reproducible scoring. Never loads model weights."""

import asyncio
import json
import math
import os
import re
import time
from pathlib import Path
from urllib.parse import quote

import httpx
from airflow_workbench.model_lab import models
from airflow_workbench.model_lab import worker_client
from airflow_workbench.model_lab.contracts import Endpoint, digest


class EvaluationError(Exception):
    pass


class RequestUncertain(EvaluationError):
    pass


class EvaluationCancelled(Exception):
    pass


def effective_options(spec, config):
    options = config.get("options", {})
    allowed = {
        "ollama": {"top_k", "top_p", "num_ctx", "repeat_penalty", "seed"},
        "openai": {"top_p", "seed", "presence_penalty", "frequency_penalty"},
        "gemini": {"topP", "topK", "seed"},
        "evaluation_api": set(),
    }[spec.provider]
    if not set(options) <= allowed:
        raise ValueError(
            "선택한 제공자가 지원하지 않는 고급 옵션입니다: "
            + ", ".join(sorted(set(options) - allowed))
        )
    if spec.provider == "ollama":
        from airflow_workbench.model_lab.schemas import GenerationOptions

        return GenerationOptions(
            **options,
            temperature=config["temperature"],
            num_predict=config["max_tokens"],
        ).model_dump()
    for key, value in options.items():
        if key in {"top_p", "topP"} and not 0 < value <= 1:
            raise ValueError("Top P는 0 초과 1 이하여야 합니다.")
        if key in {"seed", "topK"} and (
            not float(value).is_integer()
            or not 0 <= value <= (100 if key == "topK" else 2147483647)
        ):
            raise ValueError("Seed/Top K 범위를 확인하세요.")
        if key.endswith("penalty") and not -2 <= value <= 2:
            raise ValueError("Penalty는 -2~2 범위입니다.")
    return {k: int(v) if k in {"seed", "topK"} else v for k, v in options.items()}


def artifact_path(run_id):
    if not re.fullmatch(r"[a-f0-9]{32}", run_id):
        raise ValueError("Invalid run ID")
    root = Path(
        os.environ.get(
            "AIRFLOW_WORKBENCH_ARTIFACT_DIR", "/opt/airflow/workbench/lab-artifacts"
        )
    ).resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / (run_id + ".json")
    if path.is_symlink():
        raise ValueError("Artifact symlink is not allowed")
    return path


def write_artifact(run_id, value):
    import uuid

    path = artifact_path(run_id)
    temp = path.with_suffix("." + uuid.uuid4().hex + ".pending")
    temp.write_text(
        json.dumps(value, ensure_ascii=False, allow_nan=False), encoding="utf-8"
    )
    temp.replace(path)


def read_artifact(run_id):
    path = artifact_path(run_id)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def endpoint_connection(spec):
    if spec.connection_id:
        return worker_client.connection(spec.connection_id)
    return models.model_origin(), ""


async def request_json(client, method, path, payload=None):
    # Error bodies can contain upstream credentials or prompts; only return status.
    try:
        async with asyncio.timeout(client.timeout.read), client.stream(
            method, path, json=payload
        ) as response:
            if response.status_code >= 400:
                raise EvaluationError(
                    f"추론 서버 HTTP {response.status_code}. Connection·모델·지원 옵션을 확인하세요."
                )
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > 8 * 1024 * 1024:
                    raise EvaluationError("추론 응답이 허용 크기를 초과했습니다.")
            return json.loads(data)
    except (httpx.HTTPError, TimeoutError) as exc:
        raise RequestUncertain(
            "추론 연결이 중단되었습니다. 서버에서 요청이 계속 처리될 수 있어 자원 예약을 만료까지 유지합니다."
        ) from exc
    except (ValueError, KeyError) as exc:
        raise EvaluationError(
            "추론 응답을 확인할 수 없습니다. 연결·제한 시간·응답 계약을 확인하세요."
        ) from exc


def client_for(spec):
    host, token = endpoint_connection(spec)
    headers = {}
    if token:
        headers["x-goog-api-key" if spec.provider == "gemini" else "Authorization"] = (
            token if spec.provider == "gemini" else "Bearer " + token
        )
    return httpx.AsyncClient(
        base_url=host.rstrip("/") + "/",
        headers=headers,
        timeout=spec.timeout_seconds,
        trust_env=False,
        follow_redirects=False,
    )


async def catalog(spec):
    async with client_for(spec) as client:
        if spec.provider == "ollama":
            result = await request_json(client, "GET", "api/tags")
            return [
                {"name": m["name"], "digest": m.get("digest"), "size": m.get("size")}
                for m in result.get("models", [])
            ]
        if spec.provider == "openai":
            result = await request_json(client, "GET", "models")
            return [{"name": m["id"]} for m in result.get("data", [])]
        return [{"name": m} for m in spec.models]


async def generate(client, spec, config, case):
    options = effective_options(spec, config)
    messages = (
        [{"role": "system", "content": config["system"]}] if config["system"] else []
    ) + [{"role": "user", "content": case["input"]}]
    if spec.provider == "ollama":
        r = await request_json(
            client,
            "POST",
            "api/chat",
            {
                "model": config["model"],
                "messages": messages,
                "stream": False,
                "think": False,
                "keep_alive": 0,
                "options": options,
                **({"format": "json"} if config.get("json_mode") else {}),
            },
        )
        return {
            "output": r["message"]["content"],
            "tokens": r.get("eval_count"),
            "load_ms": r.get("load_duration", 0) / 1e6,
            "finish_reason": r.get("done_reason"),
        }
    if spec.provider == "openai":
        r = await request_json(
            client,
            "POST",
            "chat/completions",
            {
                "model": config["model"],
                "messages": messages,
                "temperature": config["temperature"],
                "max_tokens": config["max_tokens"],
                **options,
                **(
                    {"response_format": {"type": "json_object"}}
                    if config.get("json_mode")
                    else {}
                ),
            },
        )
        return {
            "output": r["choices"][0]["message"]["content"] or "",
            "tokens": (r.get("usage") or {}).get("completion_tokens"),
            "finish_reason": r["choices"][0].get("finish_reason"),
        }
    if spec.provider == "gemini":
        body = {
            "contents": [{"role": "user", "parts": [{"text": case["input"]}]}],
            "generationConfig": {
                "temperature": config["temperature"],
                "maxOutputTokens": config["max_tokens"],
            },
        }
        if config["system"]:
            body["systemInstruction"] = {"parts": [{"text": config["system"]}]}
        body["generationConfig"].update(options)
        if config.get("json_mode"):
            body["generationConfig"]["responseMimeType"] = "application/json"
        r = await request_json(
            client,
            "POST",
            "models/" + quote(config["model"], safe="") + ":generateContent",
            body,
        )
        candidate = r.get("candidates", [{}])[0]
        return {
            "output": "".join(
                p.get("text", "") for p in candidate.get("content", {}).get("parts", [])
            ),
            "tokens": r.get("usageMetadata", {}).get("candidatesTokenCount"),
            "finish_reason": candidate.get("finishReason"),
        }
    r = await request_json(
        client,
        "POST",
        "evaluate",
        {
            "contract_version": 1,
            "model": config["model"],
            "input": case["input"],
            "system": config["system"],
            "parameters": {
                "temperature": config["temperature"],
                "max_tokens": config["max_tokens"],
            },
            "read_only": True,
        },
    )
    if not isinstance(r.get("output"), str):
        raise EvaluationError("평가 API에는 문자열 output이 필요합니다.")
    return {"output": r["output"], "trace": r.get("trace"), "tokens": r.get("tokens")}


async def embed(client, spec, model, texts, cancelled=None):
    vectors = []
    for offset in range(0, len(texts), 16):
        if cancelled and cancelled():
            raise EvaluationCancelled()
        batch = texts[offset : offset + 16]
        if spec.provider == "ollama":
            r = await request_json(
                client,
                "POST",
                "api/embed",
                {"model": model, "input": batch, "truncate": False, "keep_alive": 0},
            )
            part = r["embeddings"]
        else:
            r = await request_json(
                client, "POST", "embeddings", {"model": model, "input": batch}
            )
            part = [v["embedding"] for v in sorted(r["data"], key=lambda v: v["index"])]
        if len(part) != len(batch) or any(
            not v
            or not all(isinstance(x, (float, int)) and math.isfinite(x) for x in v)
            for v in part
        ):
            raise EvaluationError("임베딩 개수·값이 입력 계약과 다릅니다.")
        vectors.extend(part)
    if len({len(v) for v in vectors}) != 1:
        raise EvaluationError("임베딩 차원이 서로 다릅니다.")
    return vectors


def json_subset(expected, actual):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            k in actual and json_subset(v, actual[k]) for k, v in expected.items()
        )
    # Arrays represent exact ordered values; projects can use an evaluation API
    # for domain-specific set semantics instead of claiming general equivalence.
    return type(expected) is type(actual) and expected == actual


def generation_score(case, output):
    if case["review_status"] != "reviewed":
        return None
    checks = [term.casefold() in output.casefold() for term in case["required_text"]]
    if case.get("expected_json") is not None:
        try:
            actual = json.loads(output)
            checks.append(json_subset(case["expected_json"], actual))
        except ValueError:
            checks.append(False)
    return float(all(checks)) if checks else None


def retrieval_score(case, ranked, k):
    relevance = case.get("relevance")
    if case["review_status"] != "reviewed" or relevance is None:
        return {}
    unknown = [key for key in ranked[:k] if key not in relevance]
    if unknown:
        return {
            "label_coverage": (len(ranked[:k]) - len(unknown)) / len(ranked[:k]),
            "label_note": "Top-K에 미판정 문서가 있어 검색 지표 산정에서 제외했습니다.",
        }
    positives = {key for key, grade in relevance.items() if grade > 0}
    if not positives:
        return {"label_note": "정답 없음: Recall/NDCG/MRR 산정 제외"}
    grades = [relevance.get(key, 0) for key in ranked[:k]]
    dcg = sum((2**grade - 1) / math.log2(i + 2) for i, grade in enumerate(grades))
    ideal = sum(
        (2**grade - 1) / math.log2(i + 2)
        for i, grade in enumerate(sorted(relevance.values(), reverse=True)[:k])
    )
    return {
        "recall": len(set(ranked[:k]) & positives) / len(positives),
        "ndcg": dcg / ideal,
        "mrr": next((1 / (i + 1) for i, grade in enumerate(grades) if grade > 0), 0),
    }


def summarize(rows, total):
    scores = [r["score"] for r in rows if r.get("score") is not None]
    metrics = {
        key: [r["metrics"][key] for r in rows if key in r.get("metrics", {})]
        for key in ("recall", "ndcg", "mrr")
    }
    ok = [r for r in rows if not r.get("error")]
    return {
        "total": total,
        "completed": len(rows),
        "failed": len(rows) - len(ok),
        "scored": len(scores),
        "pass_rate": sum(scores) / len(scores) if scores else None,
        "mean_latency_ms": sum(r["latency_ms"] for r in ok) / len(ok) if ok else None,
        **{
            key: sum(values) / len(values) if values else None
            for key, values in metrics.items()
        },
        "retrieval_scored": len(metrics["recall"]),
    }


async def evaluate(run_id, endpoint, config, dataset):
    spec = Endpoint.model_validate(endpoint)
    previous = read_artifact(run_id)
    signature = digest({"endpoint": endpoint, "config": config, "dataset": dataset})
    if previous and previous.get("signature") != signature:
        raise EvaluationError("저장된 실행 입력의 checksum이 다릅니다.")
    if previous and previous.get("status") in {"success", "failed", "cancelled"}:
        return previous
    result = previous or {
        "signature": signature,
        "status": "running",
        "rows": [],
        "started": time.time(),
        "scorer_version": "builtin-v1",
        "model_revision": None,
    }
    reservation = None
    release = True
    cancelled = lambda: artifact_path(run_id).with_suffix(".cancel").exists()
    try:
        if cancelled():
            raise EvaluationCancelled()
        if dataset["kind"] != "embedding":
            result["effective_options"] = effective_options(spec, config)
        if spec.reservation_environment:
            # Connection is embedded by the trusted publisher, not looked up in
            # Workbench's metadata database from the task/triggerer.
            if result.get("reservation"):
                try:
                    await worker_client.call(
                        "POST",
                        "/reservations/" + result["reservation"]["id"] + "/renew",
                        connection_id=spec.reservation_environment,
                    )
                    reservation = result["reservation"]
                except Exception:
                    reservation = None
            if reservation is None:
                reservation = await worker_client.call(
                    "POST", "/reservations", connection_id=spec.reservation_environment
                )
            result["reservation"] = reservation
            await asyncio.to_thread(write_artifact, run_id, result)
        async with client_for(spec) as client:
            if reservation:

                async def renew_before_request(request):
                    await worker_client.call(
                        "POST",
                        "/reservations/" + reservation["id"] + "/renew",
                        connection_id=spec.reservation_environment,
                    )

                client.event_hooks["request"].append(renew_before_request)
            if spec.provider == "ollama":
                r = await request_json(client, "GET", "api/tags")
                result["model_revision"] = next(
                    (
                        m.get("digest")
                        for m in r.get("models", [])
                        if m.get("name") == config["model"]
                    ),
                    None,
                )
            corpus_vectors = None
            if dataset["kind"] == "embedding":
                corpus_vectors = await embed(
                    client,
                    spec,
                    config["model"],
                    [d["text"] for d in dataset["corpus"]],
                    cancelled=cancelled,
                )
                result["embedding_dimension"] = len(corpus_vectors[0])
            completed = {r["id"] for r in result["rows"]}
            for case in dataset["cases"]:
                if case["id"] in completed:
                    continue
                if artifact_path(run_id).with_suffix(".cancel").exists():
                    result["status"] = "cancelled"
                    break
                if reservation:
                    await worker_client.call(
                        "POST",
                        "/reservations/" + reservation["id"] + "/renew",
                        connection_id=spec.reservation_environment,
                    )
                started = time.monotonic()
                row = {
                    "id": case["id"],
                    "input": case["input"],
                    "category": case["category"],
                    "review_status": case["review_status"],
                    "reference": case.get("reference", {}),
                    "score": None,
                }
                try:
                    if dataset["kind"] == "embedding":
                        vector = (
                            await embed(client, spec, config["model"], [case["input"]])
                        )[0]
                        if len(vector) != len(corpus_vectors[0]):
                            raise EvaluationError(
                                "질문과 코퍼스의 임베딩 차원이 다릅니다."
                            )
                        ranking = (
                            await asyncio.to_thread(
                                models.rank_embeddings,
                                [vector, *corpus_vectors],
                                [d["text"] for d in dataset["corpus"]],
                                [],
                                config["k"],
                            )
                        )["ranking"]
                        ids = [dataset["corpus"][r["index"]]["id"] for r in ranking]
                        row["ranking"] = [
                            {
                                "id": dataset["corpus"][r["index"]]["id"],
                                "similarity": r["score"],
                                "text": r["text"],
                            }
                            for r in ranking[: config["k"]]
                        ]
                        row["metrics"] = retrieval_score(case, ids, config["k"])
                    else:
                        row.update(await generate(client, spec, config, case))
                        row["output"] = row["output"][:24000]
                        row["score"] = generation_score(case, row["output"])
                except RequestUncertain:
                    release = False
                    raise
                except (
                    EvaluationError,
                    models.ModelError,
                    KeyError,
                    TypeError,
                    ValueError,
                ) as exc:
                    row["error"] = (
                        str(exc)
                        if isinstance(exc, EvaluationError)
                        else "추론 응답 계약을 확인하세요."
                    )
                row["latency_ms"] = round((time.monotonic() - started) * 1000, 2)
                result["rows"].append(row)
                result["summary"] = summarize(result["rows"], len(dataset["cases"]))
                result["cancel_requested"] = (
                    artifact_path(run_id).with_suffix(".cancel").exists()
                )
                if result["cancel_requested"]:
                    result["status"] = "cancelled"
                await asyncio.to_thread(write_artifact, run_id, result)
                if result["status"] == "cancelled":
                    break
            else:
                result["status"] = (
                    "failed"
                    if any(r.get("error") for r in result["rows"])
                    else "success"
                )
    except asyncio.CancelledError:
        # Do not release an admission lease while an upstream request may remain
        # active. A restarted trigger resumes from completed case artifacts.
        release = False
        raise
    except EvaluationCancelled:
        result["status"] = "cancelled"
    except Exception as exc:
        if isinstance(exc, RequestUncertain):
            release = False
        result["status"] = "failed"
        result["error"] = (
            str(exc)
            if isinstance(exc, EvaluationError)
            else "평가 실행 연결 또는 자원 예약을 확인하세요."
        )
    finally:
        if reservation and release:
            try:
                await worker_client.call(
                    "POST",
                    "/reservations/" + reservation["id"] + "/release",
                    connection_id=spec.reservation_environment,
                )
            except Exception:
                result["resource_notice"] = (
                    "추론 예약 해제 응답을 확인하지 못했습니다. 유한 예약 만료까지 대기합니다."
                )
    result["summary"] = summarize(result["rows"], len(dataset["cases"]))
    result["finished"] = time.time()
    await asyncio.to_thread(write_artifact, run_id, result)
    return result
