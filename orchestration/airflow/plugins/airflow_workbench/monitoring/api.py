"""Administrator-configured iframe destinations with per-page frame policy."""

import asyncio
import json
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit, parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import AnyHttpUrl, Field, TypeAdapter, field_validator, model_validator
from airflow_workbench.shared.contracts import Contract, ShortName
from airflow_workbench.shared.database import Store, Conflict
from airflow_workbench.shared.auth import access, can_edit

router = APIRouter()
KEY = "operations-monitoring"


class Screen(Contract):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    name: ShortName
    url: str = Field(max_length=3000)
    description: str = Field(default="", max_length=500)
    theme: Literal["none", "grafana"] = "none"
    enabled: bool = True

    @field_validator("url")
    @classmethod
    def validate_url(cls, value):
        value = str(TypeAdapter(AnyHttpUrl).validate_python(value))
        parsed = urlsplit(value)
        if parsed.path.rstrip("/").endswith(
            ("/workbench/monitoring", "/plugin/workbench-monitoring")
        ):
            raise ValueError("운영 모니터링 메뉴 자체를 연결할 수 없습니다.")
        if parsed.username or parsed.password:
            raise ValueError("URL에 계정이나 비밀번호를 포함할 수 없습니다.")
        if any(
            k.lower()
            in {
                "token",
                "access_token",
                "api_key",
                "apikey",
                "password",
                "authorization",
            }
            for k, _ in parse_qsl(parsed.query)
        ):
            raise ValueError("로그인 토큰 대신 외부 서비스의 로그인 세션을 사용하세요.")
        return value

    def origin(self):
        parsed = urlsplit(self.url)
        return parsed.scheme + "://" + parsed.netloc


class Settings(Contract):
    version: int = Field(default=0, ge=0)
    screens: list[Screen] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def unique(self):
        if len({s.id for s in self.screens}) != len(self.screens):
            raise ValueError("화면 ID가 중복됩니다.")
        return self


def load():
    with Store().connection() as db:
        row = db.execute(
            "SELECT value,version FROM documents WHERE key=?", (KEY,)
        ).fetchone()
    return (
        Settings.model_validate({**json.loads(row["value"]), "version": row["version"]})
        if row
        else Settings()
    )


def save(spec):
    with Store().connection() as db:
        db.lock(KEY)
        current = db.execute(
            "SELECT version FROM documents WHERE key=?", (KEY,)
        ).fetchone()
        if (current[0] if current else 0) != spec.version:
            raise Conflict("다른 관리자가 변경했습니다. 새로고침 후 다시 저장하세요.")
        spec = spec.model_copy(update={"version": spec.version + 1})
        db.execute(
            "INSERT INTO documents(key,value,version) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,version=EXCLUDED.version",
            (KEY, spec.model_dump_json(), spec.version),
        )
    return spec


@router.get("/monitoring")
async def page(request: Request, user=Depends(access)):
    spec = await asyncio.to_thread(load)
    request.state.monitor_frame_sources = sorted(
        {s.origin() for s in spec.screens if s.enabled}
    )
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@router.get("/api/monitoring")
async def config(user=Depends(access)):
    spec = await asyncio.to_thread(load)
    editable = await asyncio.to_thread(can_edit, user)
    return {
        **spec.model_dump(),
        "screens": [s.model_dump() for s in spec.screens if s.enabled or editable],
        "can_edit": editable,
    }


@router.put("/api/monitoring")
async def update(spec: Settings, user=Depends(access)):
    if not await asyncio.to_thread(can_edit, user):
        raise HTTPException(403, "운영 모니터링 연결은 관리자만 설정할 수 있습니다.")
    return await asyncio.to_thread(save, spec)
