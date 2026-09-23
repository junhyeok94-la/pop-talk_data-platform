"""Mounted storage contract evaluated by the external worker, never by Airflow."""

import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

from airflow_workbench.model_lab.schemas import TrainingStorage, storage_location


def defaults():
    return {
        "mode": "mounted",
        "data_root": os.environ.get("MODEL_WORKER_DATA_DIR", "/datasets/training"),
        "model_root": os.environ.get("MODEL_WORKER_WEIGHTS_DIR", "/models"),
        "artifact_root": os.environ.get(
            "MODEL_WORKER_ARTIFACT_DIR",
            os.environ.get("MODEL_WORKER_STATE_DIR", "/state") + "/artifacts",
        ),
    }


def capabilities():
    locations = defaults()
    # Operators supply mounted roots. UI users can select subdirectories, not widen them.
    allowed = json.loads(os.environ.get("MODEL_WORKER_STORAGE_ROOTS", "{}"))
    roots = {}
    for key in ("data_root", "model_root", "artifact_root"):
        values = allowed.get(key, [locations[key]])
        if not isinstance(values, list) or not values or len(values) > 20:
            raise ValueError("MODEL_WORKER_STORAGE_ROOTS 설정을 확인하세요.")
        roots[key] = [storage_location(v, mounted=True) for v in values]
    return {
        "contract": "mounted-storage-v1",
        "defaults": locations,
        "allowed_roots": roots,
    }


def approved_path(value, key):
    path = Path(storage_location(value, mounted=True))
    resolved = path.resolve()
    for item in capabilities()["allowed_roots"][key]:
        base = Path(item)
        if path.is_relative_to(base) and resolved.is_relative_to(base.resolve()):
            return resolved
    raise ValueError(f"{key}: 실행기가 허용한 마운트 경로 밖입니다.")


def resolve_locations(value):
    spec = (
        value
        if isinstance(value, TrainingStorage)
        else TrainingStorage.model_validate(value or defaults())
    )
    if spec.mode != "mounted":
        raise ValueError(
            "이 HTTP 참조 실행기는 마운트 저장소를 지원합니다. 원격 URI는 객체 저장소를 지원하는 실행 환경을 선택하세요."
        )
    paths = {
        k: approved_path(getattr(spec, k), k)
        for k in ("data_root", "model_root", "artifact_root")
    }
    for key in ("data_root", "model_root"):
        if paths[key].is_relative_to(paths["artifact_root"]) or paths[
            "artifact_root"
        ].is_relative_to(paths[key]):
            raise ValueError("실제 결과 경로는 원본 경로와 분리해야 합니다.")
    return paths


def inspect_storage(value):
    blockers, checks = [], {}
    try:
        paths = resolve_locations(value)
    except (ValueError, OSError):
        return {
            "ready": False,
            "blockers": ["저장소 경로 또는 실행기 허용 경로 설정을 확인하세요."],
            "checks": {},
            "checked_at": time.time(),
        }
    for key, path in paths.items():
        try:
            if key == "artifact_root":
                # Bounded write/delete probe; no directories or user files are created/removed.
                probe_root = path
                while not probe_root.exists():
                    probe_root = probe_root.parent
                with tempfile.TemporaryFile(dir=probe_root) as stream:
                    stream.write(b"model-lab-storage-check")
                    stream.flush()
                checks[key] = {
                    "path": str(path),
                    "writable": True,
                    "exists": path.is_dir(),
                    "free_mib": shutil.disk_usage(probe_root).free // 1048576,
                }
            else:
                if not path.is_dir() or not os.access(path, os.R_OK | os.X_OK):
                    raise OSError("directory inaccessible")
                checks[key] = {"path": str(path), "readable": True}
        except OSError:
            blockers.append(
                f"{key}: {'쓰기' if key == 'artifact_root' else '읽기'} 접근을 확인할 수 없습니다."
            )
            checks[key] = {"path": str(path), "accessible": False}
    return {
        "ready": not blockers,
        "blockers": blockers,
        "checks": checks,
        "checked_at": time.time(),
        "scope": "경로·디렉터리 접근 확인. 개별 데이터 내용·모델 가중치는 학습 설정 검증에서 검사합니다.",
    }


def model_directory(recipe):
    root = resolve_locations(recipe.storage)["model_root"]
    path = (root / recipe.base_model.replace("/", "--") / recipe.revision).resolve()
    if not path.is_relative_to(root):
        raise ValueError("model path boundary")
    return path


def artifact_directory(recipe, job_id):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", job_id):
        raise ValueError("invalid job id")
    root = resolve_locations(recipe.storage)["artifact_root"]
    path = root / job_id
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError("artifact path boundary")
    return path
