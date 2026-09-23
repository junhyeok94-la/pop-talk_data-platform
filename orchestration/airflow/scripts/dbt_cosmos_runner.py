#!/usr/bin/env python3
"""Cosmos가 선택한 dbt 태스크를 DAG run에 고정된 배포본으로 실행한다.

LocalDagBundle은 실행 도중 로컬 DAG 파일이 바뀌면 최신 파일을 다시 읽을 수 있다.
이 실행기는 ``identify_gold_build``가 XCom에 고정한 deployment ID와 digest를 매
dbt 태스크에서 검증하고, Cosmos의 임시 project 내용을 해당 불변 배포본으로
교체한다. 따라서 current pointer가 다음 배포로 이동해도 진행 중인 run은 기존
SQL/manifest를 계속 사용한다.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Iterable

REAL_DBT_EXECUTABLE = "/opt/dbt-venv/bin/dbt"
DEFAULT_PROJECT_ROOT = "/opt/airflow/modules"


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"필수 dbt 배포 환경변수가 없습니다: {name}")
    return value


def _require_same_identity(
    locked_identity: dict[str, str], graph_identity: dict[str, str]
) -> None:
    """실행 source와 Airflow graph가 같은 불변 배포를 가리키는지 확인한다."""
    if locked_identity != graph_identity:
        raise RuntimeError("DAG graph와 run에 고정된 dbt deployment가 다릅니다")


def _option_values(arguments: list[str], options: Iterable[str]) -> list[str]:
    """동일 의미의 dbt option 값을 찾아 값 누락과 우회성 중복을 거부한다."""
    names = set(options)
    values: list[str] = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in names:
            if index + 1 >= len(arguments) or arguments[index + 1].startswith("-"):
                raise RuntimeError(f"dbt 인자에 {argument} 값이 없습니다")
            values.append(arguments[index + 1])
            index += 2
            continue
        matched = next(
            (name for name in names if argument.startswith(name + "=")), None
        )
        if matched:
            values.append(argument.split("=", 1)[1])
        index += 1
    return values


def _single_option(arguments: list[str], *options: str) -> str:
    values = _option_values(arguments, options)
    if len(values) != 1:
        raise RuntimeError(f"dbt 인자 {options}는 정확히 한 번 필요합니다")
    return values[0]


def _replace_single_option(arguments: list[str], option: str, value: str) -> None:
    """이미 유일성이 검증된 option 값을 변경한다."""
    for index, argument in enumerate(arguments):
        if argument == option:
            arguments[index + 1] = value
            return
        if argument.startswith(option + "="):
            arguments[index] = f"{option}={value}"
            return
    raise RuntimeError(f"dbt 필수 인자가 없습니다: {option}")


def _prepare_temporary_project(project_dir: Path, source: Path) -> None:
    """Cosmos의 태스크별 임시 project를 고정 배포 source와 정확히 같게 만든다."""
    temporary_root = Path(tempfile.gettempdir()).resolve()
    resolved = project_dir.resolve()
    if resolved == temporary_root or temporary_root not in resolved.parents:
        raise RuntimeError("Cosmos project 경로가 시스템 임시 디렉터리 밖에 있습니다")
    if not resolved.is_dir():
        raise RuntimeError("Cosmos 임시 project 디렉터리가 없습니다")

    for child in resolved.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()
    shutil.copytree(source, resolved, dirs_exist_ok=True)


def prepare_invocation(
    arguments: list[str],
    *,
    deployment: object,
    airflow_task_id: str,
) -> list[str]:
    """Cosmos 명령을 고정 manifest의 단일 model 또는 전체 test 계약과 대사한다."""
    prepared = list(arguments)
    project_dir_text = _single_option(prepared, "--project-dir")
    _single_option(prepared, "--profiles-dir")
    profile_name = _single_option(prepared, "--profile")
    target_name = _single_option(prepared, "--target")
    if profile_name != deployment.profile_name or target_name != deployment.target_name:
        raise RuntimeError("dbt profile/target이 고정 deployment 계약과 다릅니다")

    forbidden = _option_values(
        prepared,
        ("--target-path", "--log-path", "--state", "--defer-state"),
    )
    if forbidden:
        raise RuntimeError("Cosmos 임시 디렉터리를 우회하는 dbt 경로 인자는 금지됩니다")

    commands = [item for item in prepared if item in {"run", "test"}]
    if len(commands) != 1:
        raise RuntimeError("Cosmos Gold 태스크는 dbt run 또는 test 하나만 실행할 수 있습니다")
    command = commands[0]
    selectors = _option_values(prepared, ("--select", "--models", "-s"))
    excludes = _option_values(prepared, ("--exclude", "--selector"))
    short_task_id = airflow_task_id.rsplit(".", 1)[-1]

    if short_task_id == "project_test":
        if command != "test" or selectors or excludes:
            raise RuntimeError("전체 dbt test gate는 selection 없이 test만 실행해야 합니다")
    elif short_task_id.endswith("_run"):
        expected_model_name = short_task_id[: -len("_run")]
        if command != "run" or len(selectors) != 1 or excludes:
            raise RuntimeError("dbt model 태스크는 자기 모델 하나만 run해야 합니다")
        selector = selectors[0]
        if selector not in deployment.model_selectors:
            raise RuntimeError("dbt selector가 고정 manifest model 집합에 없습니다")
        if selector.rsplit(".", 1)[-1] != expected_model_name:
            raise RuntimeError("Airflow task ID와 dbt selector model이 다릅니다")
    else:
        raise RuntimeError("허용되지 않은 Cosmos Gold task ID입니다")

    temporary_project = Path(project_dir_text)
    _prepare_temporary_project(temporary_project, deployment.project_path)
    fixed_temporary_path = str(temporary_project.resolve())
    _replace_single_option(prepared, "--project-dir", fixed_temporary_path)
    _replace_single_option(prepared, "--profiles-dir", fixed_temporary_path)
    return prepared


def main() -> None:
    project_root = Path(os.environ.get("POP_TALK_PROJECT_ROOT", DEFAULT_PROJECT_ROOT))
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from pipelines.paths import DBT_DEPLOYMENT_ROOT

    from pipelines.orchestration.dbt_deployment import validate_deployment

    deployment_root = DBT_DEPLOYMENT_ROOT
    locked_identity = {
        "deployment_id": _required_environment("POP_TALK_DBT_DEPLOYMENT_ID"),
        "manifest_sha256": _required_environment("POP_TALK_DBT_MANIFEST_SHA256"),
        "model_version": _required_environment("POP_TALK_DBT_MODEL_VERSION"),
    }
    graph_identity = {
        "deployment_id": _required_environment("POP_TALK_DBT_GRAPH_DEPLOYMENT_ID"),
        "manifest_sha256": _required_environment("POP_TALK_DBT_GRAPH_MANIFEST_SHA256"),
        "model_version": _required_environment("POP_TALK_DBT_GRAPH_MODEL_VERSION"),
    }
    _require_same_identity(locked_identity, graph_identity)

    deployment = validate_deployment(
        deployment_root,
        locked_identity["deployment_id"],
        expected_manifest_sha256=locked_identity["manifest_sha256"],
        expected_model_version=locked_identity["model_version"],
    )

    arguments = prepare_invocation(
        list(sys.argv[1:]),
        deployment=deployment,
        airflow_task_id=_required_environment("POP_TALK_DBT_AIRFLOW_TASK_ID"),
    )
    os.execv(REAL_DBT_EXECUTABLE, [REAL_DBT_EXECUTABLE, *arguments])


if __name__ == "__main__":
    main()
