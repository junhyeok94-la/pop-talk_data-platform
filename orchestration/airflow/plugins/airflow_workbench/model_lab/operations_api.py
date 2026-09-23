"""Execution environments, DAG readiness and external training requests."""

import asyncio
from urllib.parse import quote
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from airflow_workbench.model_lab import (
    mlops,
    worker_client,
    environments,
    portable_training,
)
from airflow_workbench.model_lab.schemas import TrainingRecipe, TrainingLaunch
from airflow_workbench.shared.airflow_api import airflow_api

router = APIRouter()


async def dag_contract(request, dag_id, environment=None):
    path = "/dags/" + quote(dag_id, safe="")
    dag, tasks = await asyncio.gather(
        airflow_api(request, "GET", path + "/details"),
        airflow_api(request, "GET", path + "/tasks"),
    )
    failures = mlops.validate_dag_metadata(dag, tasks["tasks"], environment)
    if dag.get("has_import_errors"):
        failures.append("DAG에 import 오류가 있습니다.")
    return {
        "dag_id": dag_id,
        "is_paused": dag["is_paused"],
        "tags": dag["tags"],
        "contract_valid": not failures,
        "blockers": failures,
    }


async def pool_contract(request, environment=None):
    name = environment.pool if environment else mlops.POOL
    pool = await airflow_api(request, "GET", "/pools/" + quote(name, safe=""))
    if (
        pool.get("slots") != (environment.capacity if environment else 1)
        or pool.get("include_deferred") is not True
    ):
        raise HTTPException(
            409,
            "실행 환경의 pool 용량과 일치해야 하며 include_deferred=true가 필요합니다.",
        )
    return pool


@router.get("/api/mlops/status")
async def mlops_status(request: Request, environment_id: str = ""):
    environment = environments.resolve(environment_id)

    async def safely(coro):
        try:
            return await coro
        except HTTPException as exc:
            return {"error": exc.detail}

    pool, *dags = await asyncio.gather(
        safely(pool_contract(request, environment)),
        *[
            safely(dag_contract(request, dag_id, environment))
            for dag_id in environment.dag_ids().values()
        ],
    )
    for kind, dag in zip(environment.workloads, dags):
        dag["kind"] = kind
    if environment.backend == "http":
        worker, jobs = await asyncio.gather(
            safely(
                worker_client.call(
                    "GET", "/resources", connection_id=environment.connection_id
                )
            ),
            safely(
                worker_client.call(
                    "GET", "/jobs", connection_id=environment.connection_id
                )
            ),
        )
    else:
        worker = {
            "backend": "kubernetes",
            "namespace": environment.namespace,
            "image": environment.image,
            "resources": {
                "cpu": environment.cpu,
                "memory": environment.memory,
                "gpu": environment.gpu,
            },
            "admission": "Kubernetes scheduler",
        }
        jobs = []
    return {
        "environment": environment.model_dump(),
        "worker": worker,
        "pool": pool,
        "jobs": jobs,
        "dags": dags,
    }


@router.get("/api/mlops/environments")
async def environment_list():
    return [e.model_dump() for e in environments.configured()]


@router.put("/api/mlops/environments/{environment_id}")
async def environment_save(environment_id: str, spec: environments.Environment):
    if environment_id != spec.id:
        raise HTTPException(422, "실행 환경 ID가 일치하지 않습니다.")
    return environments.save(spec)


@router.get("/api/mlops/environments/{environment_id}/bundle")
async def environment_bundle(environment_id: str):
    target = environments.resolve(environment_id)
    return {
        "filename": target.id + ".py",
        "source": environments.dag_source(target),
        "dag_ids": target.dag_ids(),
        "pool": {
            "name": target.pool,
            "slots": target.capacity,
            "include_deferred": True,
        },
    }


@router.post("/api/mlops/environments/{environment_id}/prepare")
async def environment_prepare(environment_id: str, request: Request):
    target = environments.resolve(environment_id)
    for dag_id in target.dag_ids().values():
        try:
            existing = await airflow_api(
                request, "GET", "/dags/" + quote(dag_id, safe="") + "/details"
            )
        except HTTPException as exc:
            if exc.status_code == 404:
                continue
            raise
        tags = {
            t.get("name") if isinstance(t, dict) else t
            for t in existing.get("tags", [])
        }
        if "environment:" + target.id not in tags:
            raise HTTPException(
                409,
                "같은 ID의 다른 DAG가 있습니다. 실행 환경의 DAG prefix를 변경하세요.",
            )
    try:
        await pool_contract(request, target)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        await airflow_api(
            request,
            "POST",
            "/pools",
            payload={
                "name": target.pool,
                "slots": target.capacity,
                "description": "Model Lab external execution: " + target.id,
                "include_deferred": True,
            },
        )
    try:
        return environments.publish(target)
    except (OSError, ValueError) as exc:
        raise HTTPException(
            409,
            "DAG 폴더에 게시할 수 없습니다. DAG bundle을 내려받아 배포 파이프라인으로 배포하세요.",
        ) from exc


@router.post("/api/mlops/dags/{kind}/activate")
async def activate_model_dag(kind: str, request: Request, environment_id: str = ""):
    environment = environments.resolve(environment_id)
    if kind not in environment.dag_ids():
        raise HTTPException(404)
    report = await dag_contract(request, environment.dag_ids()[kind], environment)
    if not report["contract_valid"]:
        raise HTTPException(409, report)
    await pool_contract(request, environment)
    return await airflow_api(
        request,
        "PATCH",
        "/dags/" + quote(environment.dag_ids()[kind], safe=""),
        payload={"is_paused": False},
    )


class DiagnosticRequest(BaseModel):
    environment_id: str = ""
    request_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{8,100}$")
    required_gpu_mib: int = Field(default=256, ge=256, le=100000)


@router.post("/api/mlops/diagnostic")
async def diagnostic(spec: DiagnosticRequest, request: Request):
    environment = environments.resolve(spec.environment_id)
    if "diagnostic" not in environment.dag_ids():
        raise HTTPException(422, "이 환경은 diagnostic workload를 제공하지 않습니다.")
    dag = await dag_contract(request, environment.dag_ids()["diagnostic"], environment)
    if dag["is_paused"] or not dag["contract_valid"]:
        raise HTTPException(409, "자원 진단 DAG를 활성화하고 계약 오류를 해결하세요.")
    await pool_contract(request, environment)
    report = (
        await worker_client.call(
            "POST",
            "/preflight",
            {
                "key": spec.request_id,
                "kind": "diagnostic",
                "profile": "diagnostic",
                "required_gpu_mib": spec.required_gpu_mib,
            },
            connection_id=environment.connection_id,
        )
        if environment.backend == "http"
        else await asyncio.to_thread(portable_training.check_kubernetes, environment)
    )
    if not report["ready"]:
        raise HTTPException(409, report)
    path = "/dags/" + dag["dag_id"] + "/dagRuns"
    run_id = "workbench__" + spec.request_id
    try:
        return await airflow_api(
            request,
            "POST",
            path,
            payload={
                "dag_run_id": run_id,
                "logical_date": None,
                "conf": {
                    "required_gpu_mib": spec.required_gpu_mib,
                    "workbench": {
                        "environment_id": environment.id,
                        "environment_fingerprint": environment.fingerprint(),
                    },
                },
            },
        )
    except HTTPException as exc:
        if exc.status_code != 409:
            raise
        previous = await airflow_api(request, "GET", path + "/" + run_id)
        if (
            previous.get("conf", {}).get("required_gpu_mib") != spec.required_gpu_mib
            or previous.get("conf", {})
            .get("workbench", {})
            .get("environment_fingerprint")
            != environment.fingerprint()
        ):
            raise HTTPException(
                409, "동일 요청 ID에 다른 진단 설정이 등록되어 있습니다."
            )
        return previous


@router.post("/api/mlops/jobs/{job_id}/cancel")
async def cancel_model_job(job_id: str, environment_id: str = ""):
    environment = environments.resolve(environment_id)
    if environment.backend != "http":
        raise HTTPException(
            422, "Kubernetes 작업은 해당 Airflow DAG 실행에서 관리하세요."
        )
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", job_id):
        raise HTTPException(422, "잘못된 작업 ID입니다.")
    return await worker_client.call(
        "POST", "/jobs/" + job_id + "/cancel", connection_id=environment.connection_id
    )


async def readiness(request, recipe):
    environment = environments.resolve(recipe.environment_id)
    report = portable_training.recipe_report(recipe, environment)
    if not report["dag_id"]:
        report["blockers"].append("실행 환경에서 이 학습 종류를 지원하지 않습니다.")
    if report["dag_id"]:
        try:
            report["dag"] = await dag_contract(request, report["dag_id"], environment)
            report["blockers"].extend(report["dag"]["blockers"])
            if report["dag"]["is_paused"]:
                report["blockers"].append(
                    "학습 DAG가 일시 정지 상태입니다. 실행·리소스 메뉴에서 활성화하세요."
                )
            report["pool"] = await pool_contract(request, environment)
        except HTTPException as exc:
            report["blockers"].append(str(exc.detail))
    try:
        payload = {
            k: report[k]
            for k in ("recipe", "recipe_sha256", "datasets", "contract_version")
        }
        if environment.backend == "kubernetes":
            portable_training.validate_remote_recipe(recipe.model_dump(), recipe.task)
            report["datasets"] = [
                {
                    "uri": recipe.train_dataset,
                    "sha256": recipe.train_sha256,
                    "split": "train",
                },
                {
                    "uri": recipe.validation_dataset,
                    "sha256": recipe.validation_sha256,
                    "split": "validation",
                },
            ]
            report["worker"] = await asyncio.to_thread(
                portable_training.check_kubernetes, environment
            )
        else:
            report["worker"] = await worker_client.call(
                "POST",
                "/preflight",
                {
                    "key": "preflight",
                    "kind": recipe.task,
                    "profile": environment.resource_profiles.get(
                        recipe.method, recipe.method
                    ),
                    "payload": payload,
                },
                connection_id=environment.connection_id,
            )
            report["datasets"] = report["worker"].get("datasets", [])
            if report["worker"].get("ready") and len(report["datasets"]) != 2:
                report["blockers"].append(
                    "외부 실행기가 검증한 학습/검증 데이터 manifest 2개를 반환해야 합니다."
                )
        report["blockers"].extend(str(b) for b in report["worker"].get("blockers", []))
        if not report["worker"].get("ready") and not report["worker"].get("blockers"):
            report["blockers"].append("실행기가 준비 완료 상태를 반환하지 않았습니다.")
    except HTTPException as exc:
        report["blockers"].append(str(exc.detail))
    except ValueError as exc:
        report["blockers"].append(str(exc))
    except Exception:
        report["blockers"].append(
            "선택한 실행 환경에 연결할 수 없습니다. Connection과 실행 계약을 확인하세요."
        )
    report["blockers"] = list(dict.fromkeys(report["blockers"]))
    report["ready"] = not report["blockers"]
    return report


@router.post("/api/training/validate")
async def validate_training(spec: TrainingRecipe, request: Request):
    return await readiness(request, spec)


@router.post("/api/training/launch")
async def launch_training(spec: TrainingLaunch, request: Request):
    report = await readiness(request, spec.recipe)
    if not report["ready"]:
        raise HTTPException(409, report)
    run_id = "workbench__" + spec.request_id
    path = "/dags/" + quote(report["dag_id"], safe="") + "/dagRuns"
    payload = {
        "dag_run_id": run_id,
        "logical_date": None,
        "conf": {"workbench": report},
    }
    try:
        result = await airflow_api(request, "POST", path, payload=payload)
    except HTTPException as exc:
        if exc.status_code != 409:
            raise
        result = await airflow_api(request, "GET", path + "/" + quote(run_id, safe=""))
        previous = result.get("conf", {}).get("workbench", {})
        if (
            previous.get("recipe_sha256") != report["recipe_sha256"]
            or previous.get("datasets") != report["datasets"]
            or previous.get("environment_fingerprint")
            != report["environment_fingerprint"]
        ):
            raise HTTPException(
                409, "동일 요청 ID에 다른 설정이 이미 등록되어 있습니다."
            )
    return {
        "dag_id": report["dag_id"],
        "dag_run_id": run_id,
        "state": result.get("state"),
        "recipe_sha256": report["recipe_sha256"],
    }
