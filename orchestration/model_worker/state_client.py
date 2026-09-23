"""Worker state lives behind the Airflow plugin API; no database credentials."""

import os
from urllib.parse import quote, urlsplit

import httpx
from fastapi import HTTPException


def call(method, path, payload=None):
    url = os.environ.get("MODEL_WORKER_STATE_API_URL", "").rstrip("/")
    env = os.environ.get("MODEL_WORKER_ENVIRONMENT_ID", "")
    token = os.environ.get("MODEL_WORKER_TOKEN", "")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not env
        or len(token) < 32
    ):
        raise RuntimeError(
            "Configure executor state API URL, environment ID and existing worker token"
        )
    try:
        with httpx.Client(timeout=15, trust_env=False) as client:
            response = client.request(
                method,
                url + path,
                json=payload,
                headers={
                    "Authorization": "Bearer " + token,
                    "X-Workbench-Executor": env,
                },
            )
        if response.status_code >= 400:
            raise HTTPException(
                response.status_code,
                response.json().get("detail", "Executor state request failed"),
            )
        return response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(503, "Executor state API unavailable") from exc


def job_path(job_id):
    return "/jobs/" + quote(job_id, safe="")
