"""Authenticated calls to the native Airflow API."""

import os
import httpx
from fastapi import HTTPException


async def airflow_api(request, method, path, *, params=None, payload=None):
    """고정된 내부 주소에 사용자의 자격 증명을 전달해 native API RBAC를 유지한다."""
    headers = {
        key: request.headers[key]
        for key in ("authorization", "cookie")
        if key in request.headers
    }
    origin = os.environ.get(
        "AIRFLOW_WORKBENCH_API_URL", "http://127.0.0.1:8080"
    ).rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
            response = await client.request(
                method,
                origin + "/api/v2" + path,
                headers=headers,
                params=params,
                json=payload,
            )
        if response.status_code >= 400:
            code = (
                response.status_code
                if response.status_code in {401, 403, 404, 409, 422}
                else 502
            )
            raise HTTPException(
                code,
                f"Airflow API 요청 실패 ({response.status_code}). 권한과 DAG 상태를 확인하세요.",
            )
        return response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(502, "Airflow API에 연결할 수 없습니다.") from exc
