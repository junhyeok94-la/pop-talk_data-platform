"""Authenticated Model Lab workflow API; native Airflow RBAC remains in force."""

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from airflow_workbench.model_lab import environments
from airflow_workbench.model_lab import worker_client
from airflow_workbench.model_lab import connections
from airflow_workbench.model_lab.contracts import (
    Project,
    Endpoint,
    Dataset,
    Experiment,
    Evaluation,
    LabTraining,
    StorageProfile,
    RegisterModel,
    Review,
    CaseReview,
    Release,
    digest,
)
from airflow_workbench.model_lab.store import LabStore
from airflow_workbench.shared.database import Conflict, NotFound
from airflow_workbench.model_lab.schemas import TrainingRecipe, Preset
from airflow_workbench.model_lab import evaluation as evaluation

router = APIRouter(prefix="/api/lab")


def admin(user):
    from airflow_workbench.shared.auth import can_edit

    return can_edit(user)


from airflow_workbench.shared.auth import access
from airflow_workbench.shared import auth
from airflow_workbench.shared.airflow_api import airflow_api
from airflow_workbench.model_lab.operations_api import readiness
from airflow.api_fastapi.auth.managers.models.resource_details import (
    DagAccessEntity,
    DagDetails,
    ConnectionDetails,
)


async def can_trigger(dag_id, user):
    return bool(
        await asyncio.to_thread(
            auth.get_auth_manager().is_authorized_dag,
            method="POST",
            access_entity=DagAccessEntity.RUN,
            details=DagDetails(id=dag_id),
            user=user,
        )
    )


def trigger_denied_message(dag_id):
    return (
        f"평가 DAG '{dag_id}' 실행 권한이 없습니다. Airflow Viewer는 조회만 가능합니다. "
        "관리자에게 해당 DAG 실행 권한을 요청하세요. Model Lab 프로젝트 권한과는 별도입니다."
    )


def authorized(project_id, user, role="viewer"):
    project = LabStore().get("_global", "project", project_id)
    level = (
        "manager"
        if admin(user) or project["owner"] == str(user.get_id())
        else project["members"].get(str(user.get_id()))
    )
    if {"viewer": 0, "developer": 1, "manager": 2}.get(level, -1) < {
        "viewer": 0,
        "developer": 1,
        "manager": 2,
    }[role]:
        raise HTTPException(403, "이 프로젝트의 작업 권한이 없습니다.")
    return project


def endpoint(key):
    record = LabStore().get("_global", "endpoint", key)
    return Endpoint.model_validate(record["spec"])


@router.get("/bootstrap")
async def bootstrap(user=Depends(access)):
    store = LabStore()
    projects = [
        p
        for p in store.list("_global", "project")
        if admin(user)
        or p["owner"] == str(user.get_id())
        or str(user.get_id()) in p["members"]
    ]
    try:
        selected = store.get("_users", "preference", digest(str(user.get_id())))[
            "project_id"
        ]
    except NotFound:
        selected = ""
    if selected not in {p["id"] for p in projects}:
        selected = ""
    endpoints = store.list("_global", "endpoint")
    specs = [Endpoint.model_validate(e["spec"]) for e in endpoints]
    permissions = await asyncio.gather(*(can_trigger(e.dag_id(), user) for e in specs))
    return {
        "projects": projects,
        "selected_project": selected,
        "endpoints": endpoints,
        "evaluation_permissions": {
            e.id: {
                "dag_id": e.dag_id(),
                "can_trigger": allowed,
                "message": "" if allowed else trigger_denied_message(e.dag_id()),
            }
            for e, allowed in zip(specs, permissions)
        },
        "can_admin": admin(user),
        "subject": str(user.get_id()),
        "storage": "Airflow PostgreSQL · workbench",
    }


class Preference(BaseModel):
    project_id: str = Field(max_length=80)


@router.put("/preferences")
async def preferences(body: Preference, user=Depends(access)):
    authorized(body.project_id, user)
    return LabStore().put(
        "_users", "preference", body.model_dump(), key=digest(str(user.get_id()))
    )


class Draft(BaseModel):
    values: dict = Field(default_factory=dict, max_length=100)


@router.get("/projects/{project_id}/draft/{tab}")
async def get_draft(project_id: str, tab: str, user=Depends(access)):
    authorized(project_id, user)
    try:
        return LabStore().get(project_id, "draft", digest([str(user.get_id()), tab]))
    except NotFound:
        return {"values": {}}


@router.put("/projects/{project_id}/draft/{tab}")
async def save_draft(project_id: str, tab: str, body: Draft, user=Depends(access)):
    authorized(project_id, user, "developer")
    if tab not in {"dataset", "experiment", "training"}:
        raise HTTPException(422, "초안 종류가 올바르지 않습니다.")
    return LabStore().put(
        project_id, "draft", body.model_dump(), key=digest([str(user.get_id()), tab])
    )


@router.put("/projects/{project_id}")
async def save_project(project_id: str, body: Project, user=Depends(access)):
    if project_id != body.id:
        raise HTTPException(422, "프로젝트 ID가 다릅니다.")
    store = LabStore()
    try:
        old = authorized(project_id, user, "manager")
        owner = old["owner"]
    except NotFound:
        owner = str(user.get_id())
    return store.put(
        "_global",
        "project",
        {**body.model_dump(), "owner": owner},
        key=project_id,
        expected=body.version,
    )


@router.put("/endpoints/{endpoint_id}")
async def save_endpoint(endpoint_id: str, body: Endpoint, user=Depends(access)):
    if not admin(user):
        raise HTTPException(403, "추론 연결 등록은 Workbench 관리자 권한이 필요합니다.")
    if endpoint_id != body.id:
        raise HTTPException(422, "연결 ID가 다릅니다.")
    if any(
        e["id"] != body.id
        and Endpoint.model_validate(e["spec"]).dag_id() == body.dag_id()
        for e in LabStore().list("_global", "endpoint")
    ):
        raise HTTPException(
            422, "다른 연결과 DAG ID가 충돌합니다. 다른 ID를 사용하세요."
        )
    # Resolve credentials without returning them or storing them in records.
    try:
        evaluation.endpoint_connection(body)
        if body.reservation_environment:
            worker_client.connection(body.reservation_environment)
    except Exception:
        raise HTTPException(
            422, "Airflow Connection의 HTTP(S) host와 password를 확인하세요."
        ) from None
    previous = body.version
    body.version += 1
    return LabStore().put(
        "_global",
        "endpoint",
        {"name": body.name, "spec": body.model_dump()},
        key=body.id,
        expected=previous,
    )


@router.get("/endpoints/{endpoint_id}/models")
async def endpoint_models(endpoint_id: str):
    try:
        return {"models": await evaluation.catalog(endpoint(endpoint_id))}
    except Exception:
        raise HTTPException(
            503, "추론 서버에 연결할 수 없습니다. Connection과 모델 목록을 확인하세요."
        ) from None


@router.get("/endpoints/{endpoint_id}/bundle")
async def bundle(endpoint_id: str, user=Depends(access)):
    if not admin(user):
        raise HTTPException(403, "관리자 권한이 필요합니다.")
    from airflow_workbench.model_lab.evaluation_dag import source

    return PlainTextResponse(source(endpoint(endpoint_id)), media_type="text/x-python")


@router.post("/endpoints/{endpoint_id}/prepare")
async def prepare(endpoint_id: str, request: Request, user=Depends(access)):
    if not admin(user):
        raise HTTPException(403, "환경 준비는 관리자 권한이 필요합니다.")
    from airflow.configuration import conf
    from airflow_workbench.model_lab.evaluation_dag import source

    spec = endpoint(endpoint_id)
    folder = Path(conf.get("core", "dags_folder")).resolve() / "workbench_managed"
    if folder.is_symlink():
        raise HTTPException(409, "관리 DAG 폴더를 확인하세요.")
    folder.mkdir(exist_ok=True)
    path = folder / ("evaluation_" + spec.id + ".py")
    if path.is_symlink():
        raise HTTPException(409, "관리 DAG 파일을 확인하세요.")
    temp = path.with_suffix("." + uuid.uuid4().hex + ".pending")
    try:
        temp.write_text(source(spec), encoding="utf-8")
        temp.replace(path)
    except OSError:
        raise HTTPException(
            409, "DAG 폴더가 읽기 전용입니다. DAG bundle을 내려받아 배포하세요."
        ) from None
    try:
        pool = await airflow_api(request, "GET", "/pools/" + spec.pool)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        pool = await airflow_api(
            request,
            "POST",
            "/pools",
            payload={
                "name": spec.pool,
                "slots": spec.capacity,
                "description": "Model Lab remote inference",
                "include_deferred": True,
            },
        )
    if pool.get("slots") != spec.capacity or pool.get("include_deferred") is not True:
        raise HTTPException(
            409,
            "기존 Pool 용량 또는 deferred 점유가 설정과 다릅니다. 기존 작업을 확인한 뒤 Pool 설정을 맞추세요.",
        )
    return {
        "dag_id": spec.dag_id(),
        "state": "awaiting_dag_processor",
        "message": "DAG 파일 게시 완료. 반영 확인·활성화를 진행하세요.",
    }


async def evaluation_ready(request, spec, dag=None):
    if dag is None:
        dag = await airflow_api(request, "GET", "/dags/" + spec.dag_id() + "/details")
    tags = {t.get("name") if isinstance(t, dict) else t for t in dag.get("tags", [])}
    tasks = await airflow_api(request, "GET", "/dags/" + spec.dag_id() + "/tasks")
    pool = await airflow_api(request, "GET", "/pools/" + spec.pool)
    task_list = tasks.get("tasks", [])
    if (
        not {"model-lab", "contract:lab-v1", "config:" + spec.fingerprint()} <= tags
        or dag.get("has_import_errors")
        or len(task_list) != 1
        or task_list[0].get("operator_name") != "RemoteEvaluationOperator"
        or task_list[0].get("pool") != spec.pool
        or pool.get("include_deferred") is not True
        or pool.get("slots") != spec.capacity
    ):
        raise HTTPException(409, "최신 평가 DAG와 Pool 반영을 확인하세요.")
    return dag


@router.get("/connection-options")
async def connection_options(request: Request, user=Depends(access)):
    if not admin(user):
        raise HTTPException(403, "연결 선택은 Workbench 관리자 권한이 필요합니다.")
    candidates = set(connections.environment_ids())
    candidates.update(r["spec"]["connection_id"] for r in LabStore().list("_global", "endpoint") if r["spec"].get("connection_id"))
    note = "환경변수와 Airflow Connections에 등록된 HTTP 접속 정보를 선택할 수 있습니다. 외부 Secrets Backend의 ID는 직접 입력할 수 있습니다."
    # Native API applies connection visibility permissions. Values stay server-side.
    try:
        rows = await airflow_api(request, "GET", "/connections", params={"limit": 100})
        candidates.update(row["connection_id"] for row in rows.get("connections", []) if row.get("connection_id"))
        if rows.get("total_entries", 0) > 100:
            note += " 목록은 처음 100개까지 표시합니다. 나머지는 ID를 입력하세요."
    except HTTPException:
        note += " Airflow Connections 목록을 읽지 못했습니다. 알고 있는 ID를 입력해 확인할 수 있습니다."
    manager = auth.get_auth_manager()
    result = []
    for connection_id in sorted(candidates)[:200]:
        allowed = await asyncio.to_thread(manager.is_authorized_connection, method="GET", details=ConnectionDetails(conn_id=connection_id), user=user)
        if not allowed:
            continue
        info = await asyncio.to_thread(connections.describe, connection_id)
        if info["valid"]:
            result.append(info)
    return {"connections": result, "message": note}


@router.get("/endpoints/{endpoint_id}/status")
async def endpoint_status(endpoint_id: str, request: Request, user=Depends(access)):
    spec = endpoint(endpoint_id)
    connection = await asyncio.to_thread(connections.describe, spec.connection_id)
    can_edit = bool(await asyncio.to_thread(auth.get_auth_manager().is_authorized_dag, method="PUT", details=DagDetails(id=spec.dag_id()), user=user))
    result = {
        "endpoint_id": spec.id, "connection": connection,
        "dag": {"id": spec.dag_id(), "url": "/dags/" + quote(spec.dag_id(), safe=""), "filename": "evaluation_" + spec.id + ".py", "exists": None, "is_paused": None},
        "can_edit": can_edit, "can_change_state": False,
        "settings": {"state": "unknown", "message": "설정을 확인하지 못했습니다."},
        "checked_at": time.time(),
    }
    try:
        dag = await airflow_api(request, "GET", "/dags/" + spec.dag_id() + "/details")
    except HTTPException as exc:
        if exc.status_code == 404:
            result["dag"]["exists"] = False
        result["settings"] = {"state": "blocked" if exc.status_code == 404 else "unknown", "message": "DAG가 아직 목록에 없습니다. DAG 생성·갱신 후 반영을 기다려주세요." if exc.status_code == 404 else "DAG 상태를 조회하지 못했습니다. 조회 권한과 Airflow 연결을 확인하세요."}
        return result
    result["dag"].update(exists=True, is_paused=dag["is_paused"])
    result["can_change_state"] = can_edit and not dag.get("is_stale", False) and not (dag["is_paused"] and dag.get("has_import_errors", False))
    try:
        await evaluation_ready(request, spec, dag=dag)
        result["settings"] = {"state": "ready", "message": "현재 DAG·Pool 설정이 일치합니다. 추론 서버 응답은 모델 목록 확인이나 평가 실행에서 확인하세요."}
    except HTTPException as exc:
        result["settings"] = {"state": "blocked" if exc.status_code in {404, 409} else "unknown", "message": "DAG·Pool 설정 반영이 필요합니다. DAG 생성·갱신과 Pool 설정을 확인하세요." if exc.status_code in {404, 409} else "설정 확인에 필요한 조회 권한 또는 Airflow 연결을 확인하세요."}
    if not connection["valid"]:
        result["settings"] = {"state": "blocked", "message": "접속 정보를 찾을 수 없거나 HTTP(S) 서버 주소가 올바르지 않습니다. Airflow Connection ID와 등록 위치를 확인하세요."}
    return result


@router.post("/endpoints/{endpoint_id}/activate")
async def activate(endpoint_id: str, request: Request, user=Depends(access)):
    # Compatibility route. Pause state uses native DAG permission; evaluation
    # readiness remains enforced when an evaluation is actually submitted.
    from airflow_workbench.dashboard.actions import DagState, dag_control, dag_state
    spec = endpoint(endpoint_id)
    current = await dag_control(request, spec.dag_id(), user)
    return await dag_state(DagState(dag_id=spec.dag_id(), is_paused=False, expected_is_paused=current["is_paused"]), request, user)


@router.get("/projects/{project_id}/{kind}")
async def records(project_id: str, kind: str, request: Request, user=Depends(access)):
    authorized(project_id, user)
    if kind not in {
        "dataset",
        "experiment",
        "run",
        "model",
        "release",
        "preset",
        "storage",
    }:
        raise HTTPException(404)
    values = LabStore().list(project_id, kind)
    if kind == "run":
        active = [
            r
            for r in values
            if r["status"] in {"queued", "running", "submitting", "submission_unknown"}
        ][:8]
        refreshed = await asyncio.gather(
            *(run_detail(project_id, r["id"], request, user) for r in active),
            return_exceptions=True
        )
        by_id = {r["id"]: r for r in refreshed if isinstance(r, dict)}
        values = [by_id.get(r["id"], r) for r in values]
    return [{k: v for k, v in item.items() if k != "dag_conf"} for item in values]


def storage_environment(profile):
    target = environments.resolve(profile.environment_id)
    expected = "mounted" if target.backend == "http" else "object"
    if profile.locations.mode != expected:
        raise HTTPException(
            422,
            "HTTP 참조 실행기는 마운트 경로, Kubernetes는 객체 저장소 프로필을 사용하세요.",
        )
    return target


@router.put("/projects/{project_id}/storage/{storage_id}")
async def save_storage(
    project_id: str, storage_id: str, body: StorageProfile, user=Depends(access)
):
    authorized(project_id, user, "manager")
    if body.id != storage_id:
        raise HTTPException(422, "저장소 ID가 다릅니다.")
    storage_environment(body)
    spec = body.model_dump()
    spec["locations"].update(profile_id=body.id, profile_version=body.version + 1)
    return LabStore().put(
        project_id,
        "storage",
        {**spec, "actor": str(user.get_id())},
        key=body.id,
        expected=body.version,
    )


@router.get("/projects/{project_id}/storage-options/{environment_id}")
async def storage_options(project_id: str, environment_id: str, user=Depends(access)):
    authorized(project_id, user, "developer")
    target = environments.resolve(environment_id)
    if target.backend == "kubernetes":
        return {
            "contract": "object-storage-v1",
            "defaults": {
                "mode": "object",
                "data_root": "",
                "model_root": "",
                "artifact_root": "",
            },
            "message": "Pod의 Service Account로 데이터·결과 저장소에 접근합니다. 원본 모델은 Hugging Face ID와 고정 revision으로 받습니다.",
        }
    return await worker_client.call(
        "GET", "/storage", connection_id=target.connection_id
    )


@router.post("/projects/{project_id}/storage/{storage_id}/check")
async def check_storage(project_id: str, storage_id: str, user=Depends(access)):
    authorized(project_id, user, "developer")
    record = LabStore().get(project_id, "storage", storage_id)
    profile = StorageProfile.model_validate(
        {k: v for k, v in record.items() if k in StorageProfile.model_fields}
    )
    target = storage_environment(profile)
    if target.backend == "kubernetes":
        result = {
            "ready": None,
            "status": "execution_check_required",
            "checks": {},
            "blockers": [],
            "scope": "URI 형식만 검증했습니다. 실제 읽기·쓰기 권한과 모델 다운로드는 학습 Pod에서 검증합니다.",
        }
    else:
        result = await worker_client.call(
            "POST",
            "/storage/check",
            profile.locations.model_dump(),
            connection_id=target.connection_id,
        )
    # Checks do not change the profile version or invalidate in-flight recipes.
    return {
        **result,
        "profile_id": storage_id,
        "profile_version": record["version"],
        "checked_at": time.time(),
    }


def validate_storage_snapshot(project_id, recipe):
    if recipe.storage is None:
        return
    record = LabStore().get(project_id, "storage", recipe.storage.profile_id)
    if record["environment_id"] != recipe.environment_id:
        raise HTTPException(422, "저장소 프로필과 학습 실행 환경이 다릅니다.")
    if recipe.storage.model_dump() != record["locations"]:
        raise Conflict(
            "저장소 설정이 변경되었습니다. 프로필을 다시 선택하고 검증하세요."
        )


@router.post("/projects/{project_id}/dataset")
async def save_dataset(project_id: str, body: Dataset, user=Depends(access)):
    authorized(project_id, user, "developer")
    value = body.model_dump()
    return LabStore().put(
        project_id,
        "dataset",
        {
            "name": body.name,
            "sha256": digest(value),
            "spec": value,
            "actor": str(user.get_id()),
        },
    )


@router.post("/projects/{project_id}/experiment")
async def save_experiment(project_id: str, body: Experiment, user=Depends(access)):
    authorized(project_id, user, "developer")
    if body.baseline_run_id:
        raise HTTPException(422, "실험 생성 후 완료된 실행을 기준선으로 지정하세요.")
    return LabStore().put(
        project_id, "experiment", {**body.model_dump(), "actor": str(user.get_id())}
    )


@router.patch("/projects/{project_id}/experiment/{experiment_id}")
async def edit_experiment(
    project_id: str,
    experiment_id: str,
    body: Experiment,
    version: int,
    user=Depends(access),
):
    authorized(project_id, user, "developer")
    store = LabStore()
    previous = store.get(project_id, "experiment", experiment_id)
    return store.put(
        project_id,
        "experiment",
        {**previous, **body.model_dump(exclude={"baseline_run_id"})},
        key=experiment_id,
        expected=version,
    )


async def submit(request, project_id, run, conf):
    store = LabStore()
    run = store.get(project_id, "run", run["id"])
    if run.get("cancel_requested") or run["status"] == "cancelled":
        return run
    if run["status"] == "submission_rejected":
        # Claim the retry before POST so cancellation cannot mistake an in-flight
        # submission for the previous definite refusal.
        run = store.put(
            project_id, "run", {**run, "status": "submitting"},
            key=run["id"], expected=run["version"],
        )
    path = "/dags/" + quote(run["dag_id"], safe="") + "/dagRuns"
    try:
        native = await airflow_api(
            request,
            "POST",
            path,
            payload={
                "dag_run_id": run["dag_run_id"],
                "logical_date": None,
                "conf": conf,
            },
        )
    except HTTPException as exc:
        if exc.status_code != 409:
            # A refusal is definitive for a new submission. A prior transport
            # failure can still have created a DAG run, so retain uncertainty.
            rejected = (
                exc.status_code in {401, 403}
                and run["status"] != "submission_unknown"
            )
            LabStore().patch(
                project_id,
                "run",
                run["id"],
                {
                    "status": "submission_rejected" if rejected else "submission_unknown",
                    "message": (
                        "Airflow 인증 또는 DAG 실행 권한이 없어 제출이 거부되었습니다. "
                        "권한을 확인한 뒤 같은 입력으로 다시 실행하세요."
                        if rejected
                        else "동일 요청으로 제출 상태를 재확인하세요."
                    ),
                },
            )
            raise
        native = await airflow_api(request, "GET", path + "/" + run["dag_run_id"])
        if native.get("conf") != conf:
            raise HTTPException(409, "기존 DAG 실행의 입력이 다릅니다.")
    return LabStore().patch(
        project_id, "run", run["id"], {"status": native["state"], "message": ""}
    )


async def existing_request(project_id, body, request):
    try:
        run = LabStore().get(project_id, "run", uuid.UUID(body.request_id).hex)
    except NotFound:
        return None
    if run.get("request_sha256") != digest(body.model_dump()):
        raise Conflict("같은 요청 ID에 다른 입력이 있습니다. 새 실행으로 제출하세요.")
    if run.get("cancel_requested"):
        return run
    if run["status"] in {"submitting", "submission_unknown", "submission_rejected"}:
        return await submit(request, project_id, run, run["dag_conf"])
    return run


@router.post("/projects/{project_id}/evaluate")
async def launch_evaluation(
    project_id: str, body: Evaluation, request: Request, user=Depends(access)
):
    authorized(project_id, user, "developer")
    spec = endpoint(body.endpoint_id)
    if not await can_trigger(spec.dag_id(), user):
        raise HTTPException(403, trigger_denied_message(spec.dag_id()))
    previous = await existing_request(project_id, body, request)
    if previous:
        return previous
    store = LabStore()
    experiment = store.get(project_id, "experiment", body.experiment_id)
    dataset = store.get(project_id, "dataset", body.dataset_id)
    if dataset["spec"]["kind"] not in spec.capabilities:
        raise HTTPException(422, "선택한 추론 연결은 이 평가를 지원하지 않습니다.")
    if (
        body.k > len(dataset["spec"]["corpus"])
        and dataset["spec"]["kind"] == "embedding"
    ):
        raise HTTPException(422, "K는 코퍼스 문서 수 이하여야 합니다.")
    if body.model_version_id:
        model = store.get(project_id, "model", body.model_version_id)
        if (model["endpoint_id"], model["model"]) != (body.endpoint_id, body.model):
            raise HTTPException(422, "모델 버전과 추론 연결/모델 이름이 다릅니다.")
    dag = await evaluation_ready(request, spec)
    if dag.get("is_paused"):
        raise HTTPException(409, "환경·연결에서 평가 DAG를 활성화하세요.")
    config = body.model_dump()
    try:
        evaluation.effective_options(spec, config)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    payload = {
        "config": config,
        "dataset": dataset["spec"],
        "fingerprint": spec.fingerprint(),
        "run_id": uuid.UUID(body.request_id).hex,
    }
    run = store.request(
        project_id,
        {
            "request_id": body.request_id,
            "input_sha256": digest(payload),
            "name": experiment["name"] + " · " + body.model,
            "kind": "evaluation",
            "experiment_id": body.experiment_id,
            "dataset_id": body.dataset_id,
            "dataset_sha256": dataset["sha256"],
            "endpoint": spec.model_dump(),
            "config": config,
            "status": "submitting",
            "dag_id": spec.dag_id(),
            "dag_run_id": "model_lab__" + body.request_id,
            "actor": str(user.get_id()),
            "quality": "not_reviewed",
        },
        request_spec=body.model_dump(),
        dag_conf={"model_lab": payload},
    )
    return await submit(request, project_id, run, run["dag_conf"])


@router.post("/projects/{project_id}/train")
async def train(
    project_id: str, body: LabTraining, request: Request, user=Depends(access)
):
    authorized(project_id, user, "developer")
    previous = await existing_request(project_id, body, request)
    if previous:
        return previous
    store = LabStore()
    experiment = store.get(project_id, "experiment", body.experiment_id)
    validate_storage_snapshot(project_id, body.recipe)
    report = await readiness(request, body.recipe)
    if not report["ready"]:
        raise HTTPException(409, report)
    run = store.request(
        project_id,
        {
            "request_id": body.request_id,
            "input_sha256": digest(
                {
                    "recipe": report["recipe"],
                    "datasets": report["datasets"],
                    "environment_fingerprint": report["environment_fingerprint"],
                }
            ),
            "name": body.recipe.name,
            "kind": "training",
            "experiment_id": body.experiment_id,
            "config": body.recipe.model_dump(),
            "status": "submitting",
            "dag_id": report["dag_id"],
            "dag_run_id": "workbench__" + body.request_id,
            "actor": str(user.get_id()),
            "quality": "not_evaluated",
        },
        request_spec=body.model_dump(),
        dag_conf={"workbench": report},
    )
    return await submit(request, project_id, run, run["dag_conf"])


@router.post("/projects/{project_id}/training/validate")
async def training_ready(
    project_id: str, body: TrainingRecipe, request: Request, user=Depends(access)
):
    authorized(project_id, user, "developer")
    validate_storage_snapshot(project_id, body)
    return await readiness(request, body)


@router.post("/projects/{project_id}/preset")
async def save_preset(project_id: str, body: Preset, user=Depends(access)):
    authorized(project_id, user, "developer")
    return LabStore().put(project_id, "preset", body.model_dump())


def finish_unsubmitted_cancellation(project_id, run):
    return LabStore().put(
        project_id,
        "run",
        {
            **run,
            "status": "cancelled",
            "cancel_requested": True,
            "cancel_completed_at": time.time(),
            "dag_run_created": False,
            "message": "제출이 거부되어 DAG가 생성되지 않은 요청입니다. 실행 전 취소가 완료되었습니다.",
        },
        key=run["id"],
        expected=run["version"],
    )


@router.get("/projects/{project_id}/run/{run_id}")
async def run_detail(
    project_id: str, run_id: str, request: Request, user=Depends(access)
):
    authorized(project_id, user)
    store = LabStore()
    run = store.get(project_id, "run", run_id)
    if run["status"] == "submission_rejected" and run.get("cancel_requested"):
        run = finish_unsubmitted_cancellation(project_id, run)
    if run["status"] == "submission_rejected" or (
        run["status"] == "cancelled" and run.get("dag_run_created") is False
    ):
        return {k: v for k, v in run.items() if k != "dag_conf"}
    artifact = None
    if run["kind"] == "evaluation":
        artifact = evaluation.read_artifact(run_id)
        expected = digest(
            {
                "endpoint": run["endpoint"],
                "config": run["config"],
                "dataset": run["dag_conf"]["model_lab"]["dataset"],
            }
        )
        if artifact and artifact.get("signature") != expected:
            artifact = None
    try:
        native = await airflow_api(
            request, "GET", "/dags/" + run["dag_id"] + "/dagRuns/" + run["dag_run_id"]
        )
        status = native["state"]
        changes = {"status": status, "last_checked": time.time(), "message": ""}
        if run["kind"] == "evaluation":
            if artifact:
                changes.update(
                    {
                        "summary": artifact.get("summary", {}),
                        "artifact_status": artifact.get("status"),
                        "model_revision": artifact.get("model_revision"),
                    }
                )
                if (
                    status in {"success", "failed"}
                    and artifact.get("status") == "cancelled"
                ):
                    changes["status"] = "cancelled"
                if status == "success" and artifact.get("status") != "success":
                    changes.update(
                        status="result_missing",
                        message="DAG 성공 상태와 평가 결과가 일치하지 않습니다.",
                    )
            elif status == "success":
                changes.update(
                    status="result_missing",
                    message="공유 artifact 경로와 평가 결과를 확인하세요.",
                )
        elif status == "success":
            # Read the small final reference through the authenticated Airflow API.
            try:
                xcom = await airflow_api(
                    request,
                    "GET",
                    "/dags/"
                    + run["dag_id"]
                    + "/dagRuns/"
                    + run["dag_run_id"]
                    + "/taskInstances/run_external_job/xcomEntries/return_value",
                )
                result = xcom.get("value")
                if isinstance(result, str):
                    result = json.loads(result)
                if isinstance(result, dict):
                    changes["training_result"] = result
            except (HTTPException, ValueError):
                changes["message"] = "학습 완료. 산출물 참조는 DAG 실행에서 확인하세요."
        try:
            run = store.patch(project_id, "run", run_id, changes)
        except Conflict:
            run = store.get(project_id, "run", run_id)
    except HTTPException as exc:
        if exc.status_code in {401, 403}:
            raise
        run = {
            **run,
            "message": (
                "취소 요청을 기록했지만 DAG 생성 여부를 아직 확인하지 못했습니다. "
                "같은 요청은 다시 제출하지 않습니다."
                if exc.status_code == 404 and run.get("cancel_requested")
                else "Airflow 상태 확인이 지연되었습니다. 마지막 확인 상태입니다."
            ),
        }
    if run["kind"] == "evaluation":
        run = {**run, "result": artifact}
    return {k: v for k, v in run.items() if k != "dag_conf"}


@router.post("/projects/{project_id}/run/{run_id}/cancel")
async def cancel(project_id: str, run_id: str, request: Request, user=Depends(access)):
    authorized(project_id, user, "developer")
    store = LabStore()
    run = store.get(project_id, "run", run_id)
    if run["status"] in {"cancelled", "success", "failed", "result_missing"}:
        return {k: v for k, v in run.items() if k != "dag_conf"}
    if run["status"] == "submission_rejected":
        # A definite native refusal has no task/worker that can acknowledge it.
        run = finish_unsubmitted_cancellation(project_id, run)
        return {k: v for k, v in run.items() if k != "dag_conf"}
    if run["kind"] != "evaluation":
        environment = environments.resolve(run["config"]["environment_id"])
        if environment.backend != "http":
            raise HTTPException(
                409, "Kubernetes 학습 취소는 연결된 DAG 실행에서 Pod 종료를 확인하세요."
            )
        import hashlib

        key = (
            "af_"
            + hashlib.sha256(
                (run["dag_id"] + "|" + run["dag_run_id"] + "|run_external_job").encode()
            ).hexdigest()[:48]
        )
        job = await worker_client.call(
            "GET", "/jobs/" + key, connection_id=environment.connection_id
        )
        await worker_client.call(
            "POST",
            "/jobs/" + job["id"] + "/cancel",
            connection_id=environment.connection_id,
        )
    else:
        evaluation.artifact_path(run_id).with_suffix(".cancel").write_text(
            "cancel requested", encoding="utf-8"
        )
    return LabStore().patch(
        project_id,
        "run",
        run_id,
        {
            "message": "취소 요청됨. 진행 중 추론 요청 종료 또는 외부 실행기 중단을 확인하는 중입니다.",
            "cancel_requested": True,
        },
    )


@router.post("/projects/{project_id}/run/{run_id}/baseline")
async def baseline(
    project_id: str, run_id: str, request: Request, user=Depends(access)
):
    authorized(project_id, user, "developer")
    run = await run_detail(project_id, run_id, request, user)
    if run["kind"] != "evaluation" or run["status"] != "success":
        raise HTTPException(409, "완료된 평가 실행을 기준선으로 지정하세요.")
    return LabStore().patch(
        project_id, "experiment", run["experiment_id"], {"baseline_run_id": run_id}
    )


@router.post("/projects/{project_id}/run/{run_id}/cases/{case_id}/review")
async def case_review(
    project_id: str,
    run_id: str,
    case_id: str,
    body: CaseReview,
    request: Request,
    user=Depends(access),
):
    authorized(project_id, user, "developer")
    run = await run_detail(project_id, run_id, request, user)
    if run["kind"] != "evaluation" or not any(
        r["id"] == case_id and not r.get("error")
        for r in (run.get("result") or {}).get("rows", [])
    ):
        raise HTTPException(409, "응답을 생성한 사례만 검토할 수 있습니다.")
    reviews = {
        **run.get("case_reviews", {}),
        case_id: {**body.model_dump(), "actor": str(user.get_id()), "at": time.time()},
    }
    return LabStore().patch(project_id, "run", run_id, {"case_reviews": reviews})


@router.get("/projects/{project_id}/compare/{candidate_id}")
async def compare(
    project_id: str, candidate_id: str, request: Request, user=Depends(access)
):
    candidate = await run_detail(project_id, candidate_id, request, user)
    if candidate["kind"] != "evaluation" or candidate["status"] != "success":
        raise HTTPException(409, "완료된 후보 평가를 선택하세요.")
    experiment = LabStore().get(project_id, "experiment", candidate["experiment_id"])
    base_id = experiment.get("baseline_run_id")
    if not base_id:
        raise HTTPException(409, "먼저 이 실험의 기준선을 지정하세요.")
    base = await run_detail(project_id, base_id, request, user)
    if base["status"] != "success":
        raise HTTPException(409, "기준선의 완료 상태를 확인하세요.")
    if candidate.get("dataset_sha256") != base.get("dataset_sha256"):
        raise HTTPException(
            409, "평가셋 버전이 다릅니다. 같은 평가셋으로 다시 실행하세요."
        )
    return {
        "baseline": base,
        "candidate": candidate,
        "same_system": base["config"]["system"] == candidate["config"]["system"],
    }


@router.post("/projects/{project_id}/model")
async def register_model(
    project_id: str, body: RegisterModel, request: Request, user=Depends(access)
):
    authorized(project_id, user, "developer")
    run = await run_detail(project_id, body.evaluation_run_id, request, user)
    if run["kind"] != "evaluation" or run["status"] != "success":
        raise HTTPException(409, "추론 평가를 완료한 실행이 필요합니다.")
    training = None
    if body.training_run_id:
        training = await run_detail(project_id, body.training_run_id, request, user)
        if (
            training["kind"] != "training"
            or training["status"] != "success"
            or training["experiment_id"] != run["experiment_id"]
        ):
            raise HTTPException(409, "같은 실험의 완료된 학습 실행을 연결하세요.")
    return LabStore().put(
        project_id,
        "model",
        {
            **body.model_dump(),
            "endpoint_id": run["config"]["endpoint_id"],
            "model": run["config"]["model"],
            "model_revision": run.get("model_revision"),
            "dataset_sha256": run["dataset_sha256"],
            "experiment_id": run["experiment_id"],
            "kind": LabStore().get(project_id, "dataset", run["dataset_id"])["spec"][
                "kind"
            ],
            "quality": "pending_review",
            "actor": str(user.get_id()),
            "training_result": training.get("training_result") if training else None,
        },
    )


@router.post("/projects/{project_id}/model/{model_id}/review")
async def review_model(
    project_id: str, model_id: str, body: Review, user=Depends(access)
):
    authorized(project_id, user, "manager")
    return LabStore().patch(
        project_id,
        "model",
        model_id,
        {
            "quality": body.decision,
            "review": {
                **body.model_dump(),
                "actor": str(user.get_id()),
                "at": time.time(),
            },
        },
    )


@router.post("/projects/{project_id}/release")
async def release(project_id: str, body: Release, user=Depends(access)):
    authorized(project_id, user, "manager")
    store = LabStore()
    model = store.get(project_id, "model", body.model_version_id)
    if model["quality"] != "approved":
        raise HTTPException(409, "모델 검토를 승인한 뒤 적용 구성을 만드세요.")
    if model["kind"] == "embedding" and not all(
        body.search_config.get(k)
        for k in (
            "index_version",
            "corpus_version",
            "preprocessing",
            "distance",
            "dimension",
        )
    ):
        raise HTTPException(
            422, "임베딩 적용에는 인덱스·코퍼스·전처리·거리함수·차원이 필요합니다."
        )
    run = store.get(project_id, "run", model["evaluation_run_id"])
    previous = next(
        (r for r in store.list(project_id, "release") if r["target"] == body.target),
        None,
    )
    return store.put(
        project_id,
        "release",
        {
            **body.model_dump(),
            "status": "configuration_ready",
            "previous_release_id": previous["id"] if previous else None,
            "manifest": {
                "contract_version": 1,
                "project_id": project_id,
                "model_version_id": model["id"],
                "endpoint_id": model["endpoint_id"],
                "model": model["model"],
                "model_revision": model["model_revision"],
                "system": run["config"]["system"],
                "parameters": {
                    k: run["config"][k] for k in ("temperature", "max_tokens")
                },
                "provider": run["endpoint"]["provider"],
                "options": run["config"].get("options", {}),
                "json_mode": run["config"].get("json_mode", False),
                "evaluation_run_id": model["evaluation_run_id"],
                "dataset_sha256": model["dataset_sha256"],
                "search": body.search_config,
            },
            "actor": str(user.get_id()),
        },
    )
