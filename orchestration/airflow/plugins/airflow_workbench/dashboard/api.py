"""Dashboard pages and APIs. No Model Lab runtime dependency."""

import json
import re
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from airflow_workbench.dashboard import protection
from airflow_workbench.dashboard import boards, postgres, studio, performance
from airflow_workbench.dashboard.schemas import Dashboard
from airflow_workbench.dashboard.legacy_store import Store
from airflow_workbench.shared.auth import access, can_edit
from airflow_workbench.shared.airflow_api import airflow_api

router = APIRouter()
STATIC = Path(__file__).parent / "static"


async def dashboard_owner(user=Depends(access)):
    """Identity comes exclusively from Airflow's authenticated user, never the client."""
    from airflow_workbench.shared.identity import owner_id

    return owner_id(user)


@router.get("/dashboard")
async def dashboard_page(view: str = "work"):
    return FileResponse(STATIC / ("index.html" if view == "charts" else "work.html"))


@router.get("/api/dashboard")
async def dashboard(user=Depends(access)):
    if not await protection.run(can_edit, user):
        raise HTTPException(
            403, "이전 공용 구성은 Workbench 관리자만 조회할 수 있습니다."
        )
    return await protection.run(Store().dashboard)


@router.put("/api/dashboard")
async def save_dashboard(spec: Dashboard):
    return await protection.run(Store().save_dashboard, spec.model_dump())


@router.get("/api/boards")
async def board_list(user=Depends(access)):
    if not await protection.run(can_edit, user):
        raise HTTPException(
            403, "이전 공용 구성은 Workbench 관리자만 조회할 수 있습니다."
        )
    return await protection.run(boards.list_boards)


@router.put("/api/boards/{board_id}")
async def board_save(board_id: str, spec: boards.Board):
    if board_id != spec.id:
        raise HTTPException(422, "보드 ID가 일치하지 않습니다.")
    return await protection.run(boards.save_board, spec)


@router.post("/api/boards/validate")
async def board_validate(spec: boards.Board):
    return spec.model_dump()


@router.get("/api/datasources")
@router.get("/api/studio/catalog")
async def datasources():
    sources = [dict(s) for s in studio.CATALOG]
    registered = await protection.run(postgres.configured)
    if not any(s.id == "pg_airflow" for s in registered):
        registered.insert(0, studio.PG_METADATA)
    for source in registered:
        try:
            sources.append(await protection.run(postgres.inspect, source))
        except ValueError as exc:
            sources.append(
                {
                    "id": source.id,
                    "name": source.name,
                    "category": source.category,
                    "dialect": "PostgreSQL · 연결 확인 필요",
                    "kind": "sql",
                    "tables": {},
                    "error": str(exc),
                }
            )
    return sources


@router.post("/api/datasources")
async def datasource_save(spec: postgres.Source):
    try:
        return await protection.run(postgres.save, spec)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/api/query")
async def preview_query(spec: boards.QuerySpec, request: Request):
    raise HTTPException(
        410,
        "SQLite 임시 SQL 조회는 종료되었습니다. Dashboard Studio의 API 빌더 또는 PostgreSQL을 사용하세요.",
    )


@router.post("/api/boards/query")
async def board_query(spec: boards.Board, request: Request):
    raise HTTPException(
        410,
        "이전 보드는 JSON으로 보존되어 있습니다. Dashboard Studio로 새로 구성하세요.",
    )


@router.get("/api/studio/boards")
async def studio_boards(owner=Depends(dashboard_owner)):
    return await protection.run(studio.list_boards, owner)


@router.put("/api/studio/boards/{board_id}")
async def studio_save(
    board_id: str, spec: studio.Board, owner=Depends(dashboard_owner)
):
    if board_id != spec.id:
        raise HTTPException(422, "보드 ID가 일치하지 않습니다.")
    return await protection.run(studio.save_board, owner, spec)


@router.post("/api/studio/validate")
async def studio_validate(spec: studio.Board):
    return spec


@router.get("/api/studio/boards/{board_id}/revisions")
async def studio_revisions(board_id: str, owner=Depends(dashboard_owner)):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", board_id):
        raise HTTPException(422, "보드 ID가 올바르지 않습니다.")
    return await protection.run(studio.revisions, owner, board_id)


@router.get("/api/studio/templates")
async def studio_templates(owner=Depends(dashboard_owner)):
    return await protection.run(studio.templates, owner)


@router.post("/api/studio/templates")
async def studio_template_save(spec: studio.Panel, owner=Depends(dashboard_owner)):
    return await protection.run(studio.save_template, owner, spec)


@router.get("/api/studio/preferences")
async def studio_preferences(owner=Depends(dashboard_owner)):
    return await protection.run(studio.preferences, owner)


@router.put("/api/studio/preferences")
async def studio_preferences_save(
    spec: studio.Preferences, owner=Depends(dashboard_owner)
):
    return await protection.run(studio.save_preferences, owner, spec)


@router.get("/api/studio/legacy")
async def studio_legacy(user=Depends(access)):
    if not await protection.run(can_edit, user):
        raise HTTPException(
            403, "기존 공용 구성은 Workbench 관리자만 조회할 수 있습니다."
        )
    return await protection.run(studio.legacy_archive)


@router.post("/api/studio/legacy/import")
async def studio_legacy_import(owner=Depends(dashboard_owner)):
    return await protection.run(studio.import_legacy, owner)


@router.post("/api/studio/query")
async def studio_query(
    spec: studio.QueryRequest, request: Request, user=Depends(access)
):
    async def fetch(path, params):
        return await airflow_api(request, "GET", path, params=params)

    try:
        panel = studio.Panel(queries=[spec.query])
        board = studio.Board(**spec.model_dump(exclude={"query"}), panels=[panel])
        return (
            await performance.board(
                board, fetch, user, allow_postgres=await protection.run(can_edit, user)
            )
        )[panel.id][0]
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/api/studio/boards/query")
async def studio_board_query(
    spec: studio.Board, request: Request, user=Depends(access)
):
    async def fetch(path, params):
        return await airflow_api(request, "GET", path, params=params)

    return await performance.board(
        spec, fetch, user, allow_postgres=await protection.run(can_edit, user)
    )


@router.get("/api/dashboard/config")
async def config(user=Depends(access)):
    return {
        "project": "Airflow Workbench",
        "can_edit": await protection.run(can_edit, user),
        "can_edit_dashboard": True,
        "dashboard_scope": "personal",
        "storage_backend": "Airflow PostgreSQL / workbench",
    }
