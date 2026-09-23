"""Compatibility evaluation APIs; not loaded by the dashboard page."""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from airflow_workbench.model_lab import models, worker_client
from airflow_workbench.model_lab.schemas import (
    ChatExperiment,
    EmbeddingExperiment,
    Preset,
)
from airflow_workbench.model_lab.legacy_store import Store
from airflow_workbench.shared.auth import access
from airflow_workbench.shared.airflow_api import airflow_api

router = APIRouter()


@router.get("/api/snapshot")
async def snapshot(
    request: Request,
    hours: int = Query(24, ge=1, le=720),
    dag_filter: str = Query("", max_length=100),
):
    errors = []
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    try:
        data = await airflow_api(
            request,
            "GET",
            "/dags/~/dagRuns",
            params={
                "limit": 200,
                "order_by": "-run_after",
                "run_after_gte": since.isoformat(),
            },
        )
        rows = data.get("dag_runs", [])
        total = data.get("total_entries", len(rows))
        rows = [row for row in rows if dag_filter.lower() in row["dag_id"].lower()]
        runs = [
            {
                key: row.get(key)
                for key in (
                    "dag_id",
                    "dag_run_id",
                    "state",
                    "start_date",
                    "end_date",
                    "run_after",
                )
            }
            for row in rows
        ]
    except HTTPException as exc:
        errors.append(exc.detail)
        runs, total = [], None
    model_catalog = await models.catalog()
    if not model_catalog["online"]:
        errors.append(model_catalog["error"])
    return {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "runs": runs,
        "total_runs_before_filter": total,
        "sample_limit": 200,
        "truncated": total is not None and total > 200,
        "models": model_catalog,
        "experiments": Store().experiments(hours),
        "errors": errors,
    }


@router.get("/api/models")
async def list_models():
    return await models.catalog()


@router.get("/api/presets")
async def presets():
    return Store().presets()


@router.post("/api/presets", status_code=201)
async def save_preset(spec: Preset):
    return Store().save_preset(spec.model_dump())


@router.get("/api/experiments")
async def experiments():
    return Store().experiments()


@router.get("/api/experiments/{experiment_id}")
async def experiment(experiment_id: str):
    value = Store().experiment(experiment_id)
    if value is None:
        raise HTTPException(404, "실험을 찾을 수 없습니다.")
    return value


async def execute_experiment(kind, spec, user, runner):
    store = Store()
    actor = str(user.get_id())
    experiment_id = store.start_experiment(kind, spec.model_dump(), actor)
    reservation = None
    release = True
    try:
        if (
            os.environ.get("AIRFLOW_WORKBENCH_REQUIRE_WORKER", "false").lower()
            == "true"
        ):
            reservation = await worker_client.call("POST", "/reservations")
        result = await asyncio.wait_for(runner(spec), timeout=1230)
        store.finish_experiment(experiment_id, result=result)
    except (models.ModelRequestInterrupted, TimeoutError) as exc:
        release = False
        message = (
            str(exc) or "모델 실행 시간이 초과되었습니다."
        ) + " GPU 중복 실행 방지를 위해 실험 잠금을 만료 시점까지 유지합니다."
        store.finish_experiment(experiment_id, error=message, release=False)
    except models.ModelError as exc:
        store.finish_experiment(
            experiment_id, error=str(exc) or "모델 실행 시간이 초과되었습니다."
        )
    except asyncio.CancelledError:
        release = False
        store.finish_experiment(
            experiment_id,
            error="API 요청이 중단되었습니다. Ollama 서버 상태를 확인하세요.",
            release=False,
        )
        raise
    except HTTPException as exc:
        store.finish_experiment(experiment_id, error=str(exc.detail))
        raise
    except Exception:
        store.finish_experiment(
            experiment_id,
            error="실험 처리 중 오류가 발생했습니다. 서버 로그를 확인하세요.",
        )
        raise
    finally:
        if reservation and release:
            try:
                await worker_client.call(
                    "POST", "/reservations/" + reservation["id"] + "/release"
                )
            except HTTPException:
                # worker의 유한 lease가 만료될 때까지 다음 GPU 작업을 차단한다.
                pass
    return store.experiment(experiment_id)


@router.post("/api/experiments/generation")
async def generation(spec: ChatExperiment, user=Depends(access)):
    return await execute_experiment("generation", spec, user, models.run_generation)


@router.post("/api/experiments/embedding")
async def embedding(spec: EmbeddingExperiment, user=Depends(access)):
    return await execute_experiment("embedding", spec, user, models.run_embedding)
