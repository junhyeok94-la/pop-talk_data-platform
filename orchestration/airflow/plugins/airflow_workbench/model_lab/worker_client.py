"""Airflow Connection의 자격 증명으로 외부 실행기와 통신한다."""

import os
import httpx


CONNECTION_ID = "workbench_model_worker"


def connection(connection_id=CONNECTION_ID):
    # API 서버/triggerer는 Connection secrets backend, task는 Task SDK를 사용한다.
    try:
        from airflow.sdk import get_current_context

        get_current_context()
    except Exception:
        from airflow.models.connection import Connection

        conn = Connection.get_connection_from_secrets(connection_id)
    else:
        from airflow.sdk.bases.hook import BaseHook

        conn = BaseHook.get_connection(connection_id)
    from urllib.parse import urlsplit

    host = (conn.host or "").rstrip("/")
    parsed = urlsplit(host)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "Executor Connection host must be an HTTP(S) endpoint without credentials."
        )
    return host, conn.password or ""


async def call(method, path, payload=None, *, connection_id=CONNECTION_ID):
    from fastapi import HTTPException

    try:
        host, token = connection(connection_id)
        async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
            response = await client.request(
                method,
                host + path,
                json=payload,
                headers={"Authorization": "Bearer " + token},
            )
    except Exception as exc:
        raise HTTPException(
            503,
            "선택한 실행 환경의 Airflow Connection과 외부 Job API 연결을 확인하세요.",
        ) from exc
    if response.status_code >= 400:
        value = response.json().get("detail", "worker request failed")
        raise HTTPException(response.status_code, value)
    return response.json()


def profile(recipe):
    return (
        "embedding_small"
        if recipe.task == "embedding_contrastive"
        else "qlora_8b" if recipe.method == "qlora" else "lora_small"
    )
