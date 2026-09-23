"""Airflow session authorization and same-origin mutation protection."""

import re
from contextvars import ContextVar
from urllib.parse import urlsplit
from fastapi import Depends, HTTPException, Request
from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.core_api.security import get_user
from airflow.api_fastapi.auth.managers.models.resource_details import AccessView

authorization_runner = ContextVar("workbench_authorization_runner", default=None)


async def access(request: Request, user=Depends(get_user)):
    """Personal writes use Plugins access; DAG actions also enforce native permissions."""
    manager = get_auth_manager()
    querying = request.url.path.endswith(
        (
            "/api/query",
            "/api/boards/query",
            "/api/studio/query",
            "/api/studio/boards/query",
        )
    )
    writing = request.method not in {"GET", "HEAD", "OPTIONS"} and not querying
    # Enumerate configuration routes, never grant write access to all Studio APIs.
    route = request.url.path.split("/api/", 1)[-1]
    personal_write = (
        request.method == "POST"
        and route
        in {"studio/validate", "studio/templates", "work/retry-preview", "work/retry"}
    ) or (
        request.method == "PUT"
        and (
            route in {"studio/preferences", "work/profile", "work/dag-state"}
            or re.fullmatch(r"studio/boards/[a-zA-Z0-9_-]{1,64}", route)
        )
    )

    def authorize():
        if writing and not personal_write and not route.startswith("lab/"):
            return manager.is_authorized_configuration(
                method="GET", user=user
            ) or manager.is_authorized_custom_view(
                method="PUT", resource_name="Airflow Workbench", user=user
            )
        return manager.is_authorized_view(access_view=AccessView.PLUGINS, user=user)

    runner = authorization_runner.get()
    allowed = await runner(authorize) if runner else authorize()
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        origin = request.headers.get("origin")
        expected = urlsplit(str(request.base_url))
        if origin:
            supplied = urlsplit(origin)
            if (supplied.scheme, supplied.netloc) != (expected.scheme, expected.netloc):
                raise HTTPException(
                    403, "동일한 Airflow 사이트에서만 변경할 수 있습니다."
                )
        if request.headers.get("x-workbench-request") != "1":
            raise HTTPException(403, "Workbench 요청 헤더가 필요합니다.")
    if not allowed:
        raise HTTPException(403, "Workbench 접근 권한이 없습니다.")
    return user


def can_edit(user):
    manager = get_auth_manager()
    return manager.is_authorized_configuration(
        method="GET", user=user
    ) or manager.is_authorized_custom_view(
        method="PUT", resource_name="Airflow Workbench", user=user
    )
