"""DAG pause control and single-run recovery through authenticated Airflow APIs."""

import asyncio
import json
import time
import uuid
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import Field
from airflow_workbench.dashboard.cache import digest
from airflow_workbench.shared.auth import access
from airflow_workbench.shared.contracts import Contract
from airflow_workbench.shared.database import Store
from airflow_workbench.shared.identity import owner_id
from airflow_workbench.shared.airflow_api import airflow_api
from airflow_workbench.shared import auth
from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails

router = APIRouter()


class Target(Contract):
    dag_id: str = Field(min_length=1, max_length=250, pattern=r"^[\w.-]+$")
    run_id: str = Field(min_length=1, max_length=250)


class Confirmation(Contract):
    preview_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    note: str = Field(default="내 대시보드에서 실패 작업 재실행", max_length=500)


class DagState(Contract):
    dag_id: str = Field(min_length=1, max_length=250, pattern=r"^[\w.-]+$")
    is_paused: bool
    expected_is_paused: bool


@router.get("/api/work/dag-control")
async def dag_control(request: Request, dag_id: str, user=Depends(access)):
    dag = await airflow_api(request, "GET", "/dags/" + quote(dag_id, safe=""))
    editable = await asyncio.to_thread(
        auth.get_auth_manager().is_authorized_dag,
        method="PUT",
        details=DagDetails(id=dag_id),
        user=user,
    )
    managed = any(
        (t.get("name") if isinstance(t, dict) else t) == "model-lab"
        for t in dag.get("tags", [])
    )
    return {
        "dag_id": dag_id,
        "is_paused": dag["is_paused"],
        "is_stale": dag.get("is_stale", False),
        "has_import_errors": dag.get("has_import_errors", False),
        "can_edit": bool(editable),
        "model_lab": managed,
        "timetable": dag.get("timetable_description") or dag.get("timetable_summary"),
        "next_run": dag.get("next_dagrun_run_after"),
    }


@router.put("/api/work/dag-state")
async def dag_state(spec: DagState, request: Request, user=Depends(access)):
    current = await dag_control(request, spec.dag_id, user)
    if not current["can_edit"]:
        raise HTTPException(403, "이 DAG를 활성화하거나 일시중지할 권한이 없습니다.")
    if current["is_stale"] or (not spec.is_paused and current["has_import_errors"]):
        raise HTTPException(
            409, "DAG 파일이 없거나 파싱 오류가 있습니다. DAG 상세에서 먼저 확인하세요."
        )
    if current["is_paused"] != spec.expected_is_paused:
        raise HTTPException(409, "DAG 상태가 변경되었습니다. 상태를 다시 확인하세요.")
    # Airflow also checks the caller's DAG PUT permission and writes its audit log.
    changed = await airflow_api(
        request,
        "PATCH",
        "/dags/" + quote(spec.dag_id, safe=""),
        payload={"is_paused": spec.is_paused},
    )
    return {"dag_id": spec.dag_id, "is_paused": changed["is_paused"]}


@router.get("/api/work/run-status")
async def run_status(request: Request, dag_id: str, run_id: str, user=Depends(access)):
    try:
        target = Target(dag_id=dag_id, run_id=run_id)
    except ValueError as exc:
        raise HTTPException(422, "DAG와 실행 ID를 확인하세요.") from exc
    result = await airflow_api(
        request, "GET", path(target) + "/dagRuns/" + quote(target.run_id, safe="")
    )
    return {"dag_id": target.dag_id, "run_id": target.run_id, "state": result["state"]}


def path(target):
    return "/dags/" + quote(target.dag_id, safe="")


def clear_body(target, dry_run=True, tasks=None):
    return {
        "dag_run_id": target.run_id,
        "dry_run": dry_run,
        "only_failed": True,
        "only_running": False,
        "reset_dag_runs": True,
        "include_past": False,
        "include_future": False,
        "include_upstream": False,
        "include_downstream": False,
        "run_on_latest_version": False,
        "prevent_running_task": True,
        **(
            {"task_ids": [[t["task_id"], t["map_index"]] for t in tasks]}
            if tasks is not None
            else {}
        ),
    }


async def inspect_target(request, target):
    dag = await airflow_api(request, "GET", path(target))
    if dag.get("is_paused"):
        raise HTTPException(
            409, "일시중지된 DAG입니다. DAG 화면에서 실행 가능 상태를 확인하세요."
        )
    tags = {t["name"] if isinstance(t, dict) else t for t in dag.get("tags", [])}
    if "model-lab" in tags:
        raise HTTPException(
            409,
            "Model Lab 실행은 Model Lab에서 다시 요청하세요. 모델 실행의 중복 방지 계약을 유지합니다.",
        )
    run = await airflow_api(
        request, "GET", path(target) + "/dagRuns/" + quote(target.run_id, safe="")
    )
    if run.get("state") != "failed":
        raise HTTPException(
            409,
            "현재 실패 상태인 DAG Run만 재실행할 수 있습니다. 목록을 새로고침하세요.",
        )
    preview = await airflow_api(
        request,
        "POST",
        path(target) + "/clearTaskInstances",
        payload=clear_body(target),
    )
    tasks = []
    for t in preview.get("task_instances", []):
        if (
            t.get("dag_id") != target.dag_id
            or t.get("dag_run_id") != target.run_id
            or t.get("state") not in {"failed", "upstream_failed"}
        ):
            raise HTTPException(
                409,
                "재실행 대상 범위를 확인할 수 없습니다. Airflow 실행 화면에서 확인하세요.",
            )
        tasks.append(
            {
                k: t.get(k)
                for k in ("task_id", "map_index", "state", "try_number", "dag_version")
            }
        )
    if (
        not tasks
        or len(tasks) > 100
        or preview.get("total_entries", len(tasks)) != len(tasks)
    ):
        raise HTTPException(
            409,
            "대상 태스크가 없거나 100개를 초과합니다. Airflow 실행 화면에서 확인하세요.",
        )
    tasks.sort(key=lambda t: (t["task_id"], t["map_index"]))
    stamp = {
        k: run.get(k)
        for k in (
            "state",
            "start_date",
            "end_date",
            "queued_at",
            "bundle_version",
            "dag_versions",
        )
    }
    return {
        **target.model_dump(),
        "tasks": tasks,
        "fingerprint": digest([target.model_dump(), stamp, tasks]),
        "logical_date": run.get("logical_date"),
        "data_interval_start": run.get("data_interval_start"),
        "data_interval_end": run.get("data_interval_end"),
    }


def remember(owner, value):
    key, now = uuid.uuid4().hex, time.time()
    with Store().connection() as db:
        db.execute(
            "INSERT INTO dashboard_actions(id,owner,created,expires,status,value) VALUES (?,?,?,?,?,?)",
            (key, owner, now, now + 300, "preview", json.dumps(value)),
        )
    return {"preview_id": key, "expires": now + 300, **value}


def claim(owner, key):
    with Store().connection() as db:
        row = db.execute(
            "SELECT * FROM dashboard_actions WHERE id=? AND owner=? FOR UPDATE",
            (key, owner),
        ).fetchone()
        if not row:
            raise HTTPException(404, "재실행 미리보기를 찾을 수 없습니다.")
        value = json.loads(row["value"])
        if row["status"] == "submitted":
            return value, None
        if row["status"] != "preview" or row["expires"] < time.time():
            raise HTTPException(
                409,
                "미리보기가 만료되었거나 이미 요청되었습니다. 실행 상태를 확인하세요.",
            )
        lease = "work-retry:" + digest([value["dag_id"], value["run_id"]])
        now = time.time()
        claimed = db.execute(
            "INSERT INTO leases(name,owner,expires) VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET owner=EXCLUDED.owner,expires=EXCLUDED.expires WHERE leases.expires<=? RETURNING name",
            (lease, key, now + 120, now),
        ).fetchone()
        if not claimed:
            raise HTTPException(
                409, "이 실행의 재실행 요청이 진행 중입니다. 잠시 후 상태를 확인하세요."
            )
        db.execute(
            "UPDATE dashboard_actions SET status='submitting' WHERE id=?", (key,)
        )
    return value, lease


def finish(key, lease, status):
    with Store().connection() as db:
        db.execute("UPDATE dashboard_actions SET status=? WHERE id=?", (status, key))
        if status != "unknown":
            db.execute("DELETE FROM leases WHERE name=? AND owner=?", (lease, key))
        if status == "submitted":
            # Force subsequent work/metric requests to sample fresh state. An in-flight
            # old aggregate may finish later; actions always use the native live API.
            db.execute("UPDATE dashboard_cache SET expires=0")


@router.post("/api/work/retry-preview")
async def preview(target: Target, request: Request, user=Depends(access)):
    value = await inspect_target(request, target)
    return await asyncio.to_thread(remember, owner_id(user), value)


@router.post("/api/work/retry")
async def retry(spec: Confirmation, request: Request, user=Depends(access)):
    value, lease = await asyncio.to_thread(claim, owner_id(user), spec.preview_id)
    if lease is None:
        return {
            "status": "submitted",
            "dag_id": value["dag_id"],
            "run_id": value["run_id"],
        }
    attempted = False
    try:
        target = Target(dag_id=value["dag_id"], run_id=value["run_id"])
        fresh = await inspect_target(request, target)
        if fresh["fingerprint"] != value["fingerprint"]:
            raise HTTPException(
                409, "미리보기 이후 태스크 상태가 바뀌었습니다. 대상을 다시 확인하세요."
            )
        attempted = True
        await airflow_api(
            request,
            "POST",
            path(target) + "/clearTaskInstances",
            payload={**clear_body(target, False, value["tasks"]), "note": spec.note},
        )
        await asyncio.to_thread(finish, spec.preview_id, lease, "submitted")
    except BaseException:
        await asyncio.to_thread(
            finish, spec.preview_id, lease, "unknown" if attempted else "failed"
        )
        raise
    return {"status": "submitted", "dag_id": value["dag_id"], "run_id": value["run_id"]}
