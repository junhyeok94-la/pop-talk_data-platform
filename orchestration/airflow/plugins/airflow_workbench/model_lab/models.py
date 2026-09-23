"""Ollama 추론과 임베딩 평가. 서버 주소는 배포 설정에서만 받는다."""

import asyncio
import math
import os
import time

import httpx


class ModelError(Exception):
    pass


class ModelRequestInterrupted(ModelError):
    """서버에서 작업이 계속 실행 중일 수 있어 lease를 즉시 해제하면 안 된다."""


def model_origin():
    return os.environ.get(
        "AIRFLOW_WORKBENCH_OLLAMA_URL", "http://localhost:11434"
    ).rstrip("/")


async def ollama(path, payload=None, timeout=10):
    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            response = await client.request(
                "POST" if payload is not None else "GET",
                model_origin() + path,
                json=payload,
            )
            response.raise_for_status()
            value = response.json()
            if "error" in value:
                raise ModelError(
                    "모델 실행에 실패했습니다. Ollama 서버 로그를 확인하세요."
                )
            return value
    except httpx.TransportError as exc:
        raise ModelRequestInterrupted(
            "Ollama 통신이 중단됐습니다. 서버 연결 상태를 확인하세요."
        ) from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise ModelError(
            "Ollama 응답을 받지 못했습니다. 서버 연결·모델 종류·메모리를 확인하세요."
        ) from exc


async def catalog():
    try:
        tags = await ollama("/api/tags")
        models = [
            {
                key: model.get(key)
                for key in ("name", "digest", "size", "modified_at", "details")
            }
            for model in tags.get("models", [])
        ]
        return {"online": True, "models": models, "error": None}
    except ModelError as exc:
        return {"online": False, "models": [], "error": str(exc)}


async def installed_models(names):
    data = await catalog()
    if not data["online"]:
        raise ModelError(data["error"])
    installed = {m["name"]: m for m in data["models"]}
    selected = []
    for name in names:
        key = name if name in installed else f"{name}:latest"
        if key not in installed:
            raise ModelError(f"설치되지 않은 모델입니다: {name}")
        selected.append(installed[key])
    return selected


def rank_embeddings(vectors, documents, relevant, k):
    if len(vectors) != len(documents) + 1 or not vectors[0]:
        raise ModelError("임베딩 응답의 문서 수 또는 차원이 올바르지 않습니다.")
    dimension = len(vectors[0])
    norms = []
    for vector in vectors:
        if len(vector) != dimension or any(
            not isinstance(x, (int, float)) or not math.isfinite(x) for x in vector
        ):
            raise ModelError(
                "임베딩 차원이 다르거나 유효하지 않은 값이 포함되어 있습니다."
            )
        norm = math.sqrt(sum(x * x for x in vector))
        if norm == 0:
            raise ModelError("길이가 0인 임베딩 벡터를 받았습니다.")
        norms.append(norm)
    scores = [
        sum(a * b for a, b in zip(vectors[0], vector)) / (norms[0] * norms[i + 1])
        for i, vector in enumerate(vectors[1:])
    ]
    order = sorted(range(len(documents)), key=lambda i: (-scores[i], i))
    relevant = set(relevant)
    metrics = None
    if relevant:
        hits = [int(i in relevant) for i in order[:k]]
        dcg = sum(hit / math.log2(rank + 2) for rank, hit in enumerate(hits))
        ideal = sum(1 / math.log2(rank + 2) for rank in range(min(k, len(relevant))))
        metrics = {
            "recall_at_k": sum(hits) / len(relevant),
            "ndcg_at_k": dcg / ideal,
            "mrr_at_k": next(
                (1 / (rank + 1) for rank, hit in enumerate(hits) if hit), 0
            ),
        }
    return {
        "dimension": dimension,
        "k": k,
        "metrics": metrics,
        "ranking": [
            {
                "index": i,
                "text": documents[i],
                "score": max(-1, min(1, scores[i])),
                "relevant": i in relevant if relevant else None,
            }
            for i in order
        ],
    }


async def run_generation(spec):
    models = await installed_models(spec.models)
    results = []
    for model in models:
        payload = {
            "model": model["name"],
            "messages": [
                {"role": "system", "content": spec.system},
                {"role": "user", "content": spec.prompt},
            ],
            "options": spec.options.model_dump(),
            "stream": False,
            "think": False,
            "keep_alive": 0,
        }
        if spec.json_mode:
            payload["format"] = "json"
        started = time.monotonic()
        response = await asyncio.wait_for(
            ollama("/api/chat", payload, timeout=600), timeout=610
        )
        tokens = response.get("eval_count", 0)
        duration = response.get("eval_duration", 0) / 1e9
        results.append(
            {
                "model": model["name"],
                "digest": model["digest"],
                "output": response.get("message", {}).get("content", ""),
                "latency_seconds": round(time.monotonic() - started, 3),
                "tokens": tokens,
                "tokens_per_second": round(tokens / duration, 2) if duration else None,
                "load_seconds": response.get("load_duration", 0) / 1e9,
                "done_reason": response.get("done_reason"),
                "quality_score": None,
            }
        )
    return {
        "models": results,
        "note": "같은 입력·파라미터로 순차 실행. 출력 품질은 사람이 검토해야 합니다. GPU 공유 부하와 모델 로딩이 지연시간에 포함됩니다.",
    }


async def run_embedding(spec):
    models = await installed_models(spec.models)
    results = []
    for model in models:
        started = time.monotonic()
        response = await asyncio.wait_for(
            ollama(
                "/api/embed",
                {
                    "model": model["name"],
                    "input": [spec.query, *spec.documents],
                    "truncate": False,
                    "keep_alive": 0,
                },
                timeout=600,
            ),
            timeout=610,
        )
        ranking = rank_embeddings(
            response.get("embeddings", []),
            spec.documents,
            spec.relevant_indices,
            spec.k,
        )
        results.append(
            {
                "model": model["name"],
                "digest": model["digest"],
                "latency_seconds": round(time.monotonic() - started, 3),
                **ranking,
            }
        )
    return {
        "models": results,
        "note": "입력한 문서 집합 안에서의 검색 평가입니다. 서로 다른 모델의 벡터를 혼합하지 않으며, 정답 미지정 시 품질 점수를 계산하지 않습니다.",
    }
