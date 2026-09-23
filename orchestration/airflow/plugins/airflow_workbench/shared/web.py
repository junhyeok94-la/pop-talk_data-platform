"""Response limits and error handling shared by feature routers."""

from fastapi import FastAPI, Depends
from fastapi.responses import JSONResponse
from airflow_workbench.shared.auth import access
from airflow_workbench.shared.database import Conflict, NotFound


async def response_headers(request, call_next):
    try:
        content_length = int(request.headers.get("content-length", "0"))
    except ValueError:
        return JSONResponse({"detail": "잘못된 Content-Length입니다."}, status_code=400)
    if content_length > 262144:
        return JSONResponse(
            {"detail": "요청은 256KB 이하여야 합니다."}, status_code=413
        )
    if request.method in {"POST", "PUT", "PATCH"}:
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > 262144:
                return JSONResponse(
                    {"detail": "요청은 256KB 이하여야 합니다."}, status_code=413
                )
            chunks.append(chunk)
        # Starlette BaseHTTPMiddleware의 cached request를 downstream에 재전달한다.
        request._body = b"".join(chunks)
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'self'; object-src 'none'; base-uri 'self'"
    )
    origins = getattr(request.state, "monitor_frame_sources", [])
    if origins:
        response.headers["Content-Security-Policy"] += "; frame-src 'self' " + " ".join(
            origins
        )
    return response


async def conflict_handler(request, exc):
    return JSONResponse({"detail": str(exc)}, status_code=409)


async def not_found_handler(request, exc):
    return JSONResponse({"detail": str(exc)}, status_code=404)


def base_app():
    app = FastAPI(
        title="Airflow Workbench",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        dependencies=[Depends(access)],
    )
    app.middleware("http")(response_headers)
    app.add_exception_handler(Conflict, conflict_handler)
    app.add_exception_handler(NotFound, not_found_handler)
    return app
