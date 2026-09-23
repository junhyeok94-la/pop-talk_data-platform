"""Airflow 프로세스와 분리된 단일 GPU 작업 실행기. 신뢰된 driver만 subprocess로 실행한다."""

import asyncio
import csv
import hashlib
import hmac
import importlib.util
import json
import math
import os
import secrets
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

ROOT = Path(os.environ.get("MODEL_WORKER_STATE_DIR", "/state"))
DATA = Path(os.environ.get("MODEL_WORKER_DATA_DIR", "/datasets/training"))
WEIGHTS = Path(os.environ.get("MODEL_WORKER_WEIGHTS_DIR", "/models"))
DRIVER = Path(__file__).with_name("driver.py")
PROFILES = {
    "diagnostic": {"gpu_mib": 256, "ram_mib": 128, "disk_mib": 20, "max_seconds": 120},
    "qlora_8b": {
        "gpu_mib": 10500,
        "ram_mib": 10000,
        "disk_mib": 10000,
        "max_seconds": 21600,
    },
    "lora_small": {
        "gpu_mib": 8000,
        "ram_mib": 6000,
        "disk_mib": 6000,
        "max_seconds": 21600,
    },
    "embedding_small": {
        "gpu_mib": 8000,
        "ram_mib": 6000,
        "disk_mib": 6000,
        "max_seconds": 21600,
    },
}
TASKS = set()
TASK_BY_JOB = {}
PROCESSES = {}


class JobSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    key: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,100}$")
    kind: str = Field(pattern=r"^(diagnostic|llm_sft|embedding_contrastive)$")
    profile: str = Field(pattern=r"^(diagnostic|qlora_8b|lora_small|embedding_small)$")
    payload: dict = Field(default_factory=dict)
    required_gpu_mib: int = Field(default=0, ge=0, le=100000)


import state_client as state


def resources():
    import shutil

    gpus = []
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,memory.total,memory.free,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        for row in csv.reader(result.stdout.splitlines()):
            gpus.append(
                {
                    "index": int(row[0]),
                    "name": row[1].strip(),
                    "total_mib": int(row[2]),
                    "free_mib": int(row[3]),
                    "utilization": int(row[4]),
                }
            )
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    mem = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        mem[key] = int(value.strip().split()[0]) // 1024
    limit = mem["MemTotal"]
    available = mem["MemAvailable"]
    cgroup_limit = Path("/sys/fs/cgroup/memory.max")
    cgroup_used = Path("/sys/fs/cgroup/memory.current")
    if cgroup_limit.is_file() and cgroup_limit.read_text().strip() != "max":
        limit = min(limit, int(cgroup_limit.read_text()) // 1048576)
        available = min(
            available, max(0, limit - int(cgroup_used.read_text()) // 1048576)
        )
    elif Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").is_file():
        limit = min(
            limit,
            int(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").read_text())
            // 1048576,
        )
        used = (
            int(Path("/sys/fs/cgroup/memory/memory.usage_in_bytes").read_text())
            // 1048576
        )
        available = min(available, max(0, limit - used))
    ROOT.mkdir(parents=True, exist_ok=True)
    return {
        "gpus": gpus,
        "ram_limit_mib": limit,
        "ram_available_mib": available,
        "disk_free_mib": shutil.disk_usage(ROOT).free // 1048576,
        "cpu_limit": os.environ.get("MODEL_WORKER_CPU_LIMIT", "2"),
        "training_packages": {
            name: bool(importlib.util.find_spec(name))
            for name in ("torch", "transformers", "peft", "sentence_transformers")
        },
        "checked_at": time.time(),
    }


def preflight(spec, *, discover=False):
    facts = resources()
    datasets = []
    budget = PROFILES[spec.profile]
    blockers = []
    if spec.kind == "diagnostic" and spec.profile != "diagnostic":
        blockers.append("진단 작업은 diagnostic profile을 사용해야 합니다.")
    if spec.kind != "diagnostic" and spec.profile == "diagnostic":
        blockers.append("학습은 학습용 resource profile을 사용해야 합니다.")
    required = max(budget["gpu_mib"], spec.required_gpu_mib)
    if not facts["gpus"] or facts["gpus"][0]["free_mib"] < required:
        blockers.append(f"GPU 여유 메모리 부족: 최소 {required} MiB 필요")
    if facts["ram_available_mib"] < budget["ram_mib"]:
        blockers.append(
            f"워커 RAM 부족: 최소 {budget['ram_mib']} MiB 필요 (컨테이너 제한 포함)"
        )
    if facts["disk_free_mib"] < budget["disk_mib"]:
        blockers.append(f"artifact 디스크 부족: 최소 {budget['disk_mib']} MiB 필요")
    if spec.kind != "diagnostic":
        recipe = spec.payload.get("recipe", {})
        # 공유되는 입력 계약을 다시 적용한다. Airflow나 UI의 검증 결과만 신뢰하지 않는다.
        from airflow_workbench.model_lab.schemas import TrainingRecipe
        from airflow_workbench.model_lab.training import inspect_recipe
        from airflow_workbench.model_lab.storage import inspect_storage, model_directory

        try:
            parsed = TrainingRecipe.model_validate(recipe)
            if parsed.artifact_uri:
                blockers.append(
                    "이 HTTP 참조 실행기는 결과 URI 업로드를 지원하지 않습니다. 마운트 저장소 프로필을 선택하세요."
                )
            if parsed.storage:
                storage_report = inspect_storage(parsed.storage)
                blockers.extend(storage_report["blockers"])
                artifact_facts = storage_report["checks"].get("artifact_root", {})
                if artifact_facts.get("free_mib", 0) < budget["disk_mib"]:
                    blockers.append("선택한 결과 저장소의 여유 디스크가 부족합니다.")
            if parsed.task != spec.kind:
                blockers.append("작업 종류와 recipe task가 다릅니다.")
            expected = (
                "embedding_small"
                if parsed.task == "embedding_contrastive"
                else "qlora_8b" if parsed.method == "qlora" else "lora_small"
            )
            if spec.profile != expected:
                blockers.append("학습 방식과 resource profile이 다릅니다.")
            if (
                parsed.max_seq_length > 2048
                or parsed.batch_size > 2
                or parsed.lora_rank > 32
            ):
                blockers.append(
                    "현재 단일 GPU profile은 seq<=2048, batch<=2, rank<=32 범위입니다. 더 큰 실험은 별도 GPU profile이 필요합니다."
                )
            report = inspect_recipe(parsed)
            datasets = report["datasets"]
            blockers.extend(report["blockers"])
            if report["recipe_sha256"] != spec.payload.get("recipe_sha256") or (
                not discover and report["datasets"] != spec.payload.get("datasets")
            ):
                blockers.append("제출 시점과 현재 학습 데이터/recipe hash가 다릅니다.")
            model_dir = model_directory(parsed)
            if (
                not model_dir.is_dir()
                or not (model_dir / "config.json").is_file()
                or not list(model_dir.glob("*.safetensors"))
            ):
                blockers.append("고정 revision의 로컬 Hugging Face 가중치가 없습니다.")
            else:
                try:
                    parameters = model_parameters(model_dir)
                    required = max(required, estimated_gpu_mib(parsed, parameters))
                    if not facts["gpus"] or facts["gpus"][0]["free_mib"] < required:
                        blockers.append(
                            f"모델 크기/학습 방식 기준 GPU 메모리 부족: 약 {required} MiB 이상 필요"
                        )
                except (OSError, ValueError, KeyError, TypeError):
                    blockers.append("로컬 safetensors 메타데이터를 확인할 수 없습니다.")
            required_packages = (
                ["torch", "transformers", "peft"]
                if parsed.task == "llm_sft"
                else ["torch", "sentence_transformers"]
            )
            if any(not facts["training_packages"][p] for p in required_packages):
                blockers.append(
                    "GPU 학습 이미지의 Python 패키지가 준비되지 않았습니다."
                )
        except ValueError as exc:
            blockers.append("학습 recipe 계약이 올바르지 않습니다.")
    return {
        "ready": not blockers,
        "blockers": blockers,
        "resources": facts,
        "budget": {**budget, "gpu_mib": required},
        "profile": spec.profile,
        "datasets": datasets,
    }


def model_parameters(model_dir):
    """가중치를 메모리에 올리지 않고 safetensors의 제한된 JSON 헤더만 읽는다."""
    total = 0
    for path in model_dir.glob("*.safetensors"):
        with path.open("rb") as stream:
            length = int.from_bytes(stream.read(8), "little")
            if not 2 <= length <= 16 * 1024 * 1024:
                raise ValueError("invalid safetensors header")
            header = json.loads(stream.read(length))
        for name, tensor in header.items():
            if name == "__metadata__":
                continue
            shape = tensor["shape"]
            if any(not isinstance(n, int) or n < 0 for n in shape):
                raise ValueError("invalid shape")
            total += math.prod(shape)
    if not 0 < total < 10**12:
        raise ValueError("invalid parameter count")
    return total


def estimated_gpu_mib(recipe, parameters):
    # 최소 예산 추정치. activation/optimizer의 실제 피크는 별도 작은 학습으로 측정한다.
    activation = (
        1536 + math.ceil(recipe.batch_size * recipe.max_seq_length / 1024) * 512
    )
    bytes_per_parameter = (
        16
        if recipe.task == "embedding_contrastive"
        else 0.7 if recipe.method == "qlora" else 2
    )
    return math.ceil(parameters * bytes_per_parameter / 1048576) + activation


def snapshot_training_data(spec, folder):
    """검증된 파일 사본을 job에 고정해 검증 이후 host 파일 수정의 영향을 막는다."""
    target = folder / "inputs"
    target.mkdir(exist_ok=True)
    from airflow_workbench.model_lab.storage import resolve_locations

    source_root = resolve_locations(spec.payload["recipe"].get("storage"))["data_root"]
    for record in spec.payload["datasets"]:
        path = (source_root / record["name"]).resolve()
        if path.parent != source_root or path.stat().st_size > 20 * 1024 * 1024:
            raise ValueError("dataset boundary/size")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != record["sha256"]:
            raise ValueError("dataset changed after validation")
        (target / path.name).write_bytes(data)


def get_job(job_id):
    return state.call("GET", state.job_path(job_id))


def update_job(job_id, status, result=None, error=None):
    return state.call(
        "POST",
        state.job_path(job_id) + "/transition",
        {"status": status, "result": result, "error": error},
    )["changed"]


async def execute(job_id, spec):
    process = None
    folder = ROOT / job_id
    try:
        folder.mkdir(parents=True, exist_ok=True)
        report = await asyncio.to_thread(preflight, spec)
        if not report["ready"]:
            update_job(job_id, "rejected", error="; ".join(report["blockers"]))
            return
        changed = update_job(job_id, "running")
        if not changed:
            return
        if spec.kind != "diagnostic":
            await asyncio.to_thread(snapshot_training_data, spec, folder)
        if get_job(job_id)["status"] != "running":
            return
        (folder / "request.json").write_text(spec.model_dump_json(), encoding="utf-8")
        with (folder / "worker.log").open("wb") as log:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(DRIVER),
                str(folder),
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
            PROCESSES[job_id] = process
            if get_job(job_id)["status"] in {"cancelled", "cancelling"}:
                os.killpg(process.pid, signal.SIGKILL)
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=PROFILES[spec.profile]["max_seconds"]
                )
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
                update_job(job_id, "timeout", error="작업 제한 시간을 초과했습니다.")
                return
        if get_job(job_id)["status"] in {"cancelled", "cancelling"}:
            return
        if process.returncode != 0:
            update_job(
                job_id,
                "failed",
                error="격리 실행기에서 작업이 실패했습니다. worker artifact 로그를 확인하세요.",
            )
        else:
            result = json.loads((folder / "result.json").read_text())
            update_job(job_id, "success", result={**result, "artifact_id": job_id})
    except asyncio.CancelledError:
        if process and process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        update_job(job_id, "interrupted", error="워커가 중단됐습니다.")
        raise
    except Exception:
        update_job(job_id, "failed", error="워커 작업 처리 오류")
    finally:
        PROCESSES.pop(job_id, None)


async def authenticate(request: Request):
    expected = os.environ.get("MODEL_WORKER_TOKEN", "")
    if len(expected) < 32 or not hmac.compare_digest(
        request.headers.get("authorization", ""), "Bearer " + expected
    ):
        raise HTTPException(401, "worker 인증 실패")


@asynccontextmanager
async def lifespan(app):
    await asyncio.to_thread(state.call, "POST", "/recover")
    yield
    for task in list(TASKS):
        task.cancel()
    await asyncio.gather(*TASKS, return_exceptions=True)


app = FastAPI(
    title="Isolated Model Worker",
    dependencies=[Depends(authenticate)],
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/resources")
async def resource_status():
    facts = await asyncio.to_thread(resources)
    admission = await asyncio.to_thread(state.call, "GET", "/admission")
    return {
        **facts,
        **admission,
        "state_backend": "Airflow API / PostgreSQL",
        "profiles": PROFILES,
    }


@app.post("/preflight")
async def inspect(spec: JobSpec):
    return await asyncio.to_thread(preflight, spec, discover=True)


@app.get("/storage")
async def storage_options():
    from airflow_workbench.model_lab.storage import capabilities

    return capabilities()


from airflow_workbench.model_lab.schemas import TrainingStorage


@app.post("/storage/check")
async def storage_check(spec: TrainingStorage):
    from airflow_workbench.model_lab.storage import inspect_storage

    return await asyncio.to_thread(inspect_storage, spec)


@app.post("/jobs")
async def submit(spec: JobSpec):
    # The API atomically enforces idempotency and admission. Only queued jobs
    # are scheduled; a repeated response after a network failure is recoverable.
    reply = state.call(
        "POST", "/jobs", {"key": spec.key, "request": spec.model_dump_json()}
    )
    value = reply["job"]
    job_id = value["id"]
    if value["status"] == "queued" and job_id not in TASK_BY_JOB:
        task = asyncio.create_task(execute(job_id, spec))
        TASKS.add(task)
        TASK_BY_JOB[job_id] = task
        task.add_done_callback(TASKS.discard)
        task.add_done_callback(lambda _: TASK_BY_JOB.pop(job_id, None))
    return value


@app.get("/jobs")
async def history():
    return await asyncio.to_thread(state.call, "GET", "/jobs")


@app.post("/reservations")
async def reserve_inference():
    return state.call("POST", "/reservations")


@app.post("/reservations/{reservation_id}/release")
async def release_inference(reservation_id: str):
    from urllib.parse import quote

    return state.call(
        "POST", "/reservations/" + quote(reservation_id, safe="") + "/release"
    )


@app.post("/reservations/{reservation_id}/renew")
async def renew_inference(reservation_id: str):
    from urllib.parse import quote

    return await asyncio.to_thread(
        state.call, "POST", "/reservations/" + quote(reservation_id, safe="") + "/renew"
    )


@app.get("/jobs/{job_id}")
async def status(job_id: str):
    return get_job(job_id)


@app.post("/jobs/{job_id}/cancel")
async def cancel(job_id: str):
    value = get_job(job_id)
    if value["status"] in {"queued", "running", "cancelling"}:
        update_job(value["id"], "cancelling", error="취소 처리 중")
        process = PROCESSES.get(value["id"])
        if process and process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        task = TASK_BY_JOB.get(value["id"])
        if task and task is not asyncio.current_task():
            await asyncio.shield(task)
        update_job(value["id"], "cancelled", error="사용자가 실행을 취소했습니다.")
    return get_job(job_id)
