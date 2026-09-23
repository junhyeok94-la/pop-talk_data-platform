"""현재 dbt project를 Cosmos용 content-addressed 불변 배포본으로 만든다."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_DBT_EXECUTABLE = Path("/opt/dbt-venv/bin/dbt")
DEFAULT_DBT_PYTHON = Path("/opt/dbt-venv/bin/python")
PROFILE_NAME = "pop_talk_dw"
TARGET_NAME = "dev"
PROJECT_NAME = "pop_talk_dw"


def _project_import(source: Path) -> None:
    project_root = source.parent.parent
    root_text = str(project_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)


def _copy_sources(source: Path, destination: Path) -> None:
    from pipelines.orchestration.dbt_deployment import source_files

    for source_path in source_files(source):
        relative = source_path.relative_to(source)
        destination_path = destination / relative
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, destination_path)


def _dbt_versions(dbt_python: Path) -> dict[str, str]:
    code = (
        "import importlib.metadata,json; "
        "print(json.dumps({p:importlib.metadata.version(p) "
        "for p in ('dbt-core','dbt-snowflake')}))"
    )
    completed = subprocess.run(
        [str(dbt_python), "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _run_dbt(
    dbt_executable: Path,
    project_path: Path,
    working_root: Path,
    command: list[str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            str(dbt_executable),
            "--quiet",
            "--no-use-colors",
            *command,
            "--project-dir",
            str(project_path),
            "--profiles-dir",
            str(project_path),
            "--target-path",
            str(working_root / "target"),
            "--log-path",
            str(working_root / "logs"),
            "--no-partial-parse",
        ],
        # Airflow scripts의 dbt_* 파일을 dbt plugin으로 오인하지 않도록 분리한다.
        env={key: value for key, value in os.environ.items() if key != "PYTHONPATH"},
        check=True,
        capture_output=True,
        text=True,
    )


def _normalize_manifest(path: Path) -> bytes:
    from pipelines.orchestration.dbt_deployment import canonical_json

    manifest = json.loads(path.read_bytes())

    # dbt는 parse할 때 모든 node/macro/source에 현재 epoch 기반 created_at을 쓴다.
    # 그래프 의미와 무관한 이 값은 같은 source에서도 manifest digest를 매번 바꾸므로
    # 재귀적으로 0으로 정규화해 content-address를 결정적으로 유지한다.
    def normalize_created_at(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "created_at":
                    value[key] = 0
                else:
                    normalize_created_at(child)
        elif isinstance(value, list):
            for child in value:
                normalize_created_at(child)

    normalize_created_at(manifest)
    metadata = manifest.setdefault("metadata", {})
    metadata["generated_at"] = "1970-01-01T00:00:00Z"
    if "user_id" in metadata:
        metadata["user_id"] = "00000000-0000-0000-0000-000000000000"
    if "invocation_id" in metadata:
        metadata["invocation_id"] = "00000000-0000-0000-0000-000000000000"
    if "invocation_started_at" in metadata:
        metadata["invocation_started_at"] = "1970-01-01T00:00:00Z"
    return canonical_json(manifest)


def _listed_tests(completed: subprocess.CompletedProcess[str]) -> list[str]:
    unique_ids: list[str] = []
    for line in completed.stdout.splitlines():
        if line.strip():
            unique_ids.append(str(json.loads(line)["unique_id"]))
    return sorted(unique_ids)


def deploy(
    *,
    source: Path,
    output: Path,
    dbt_executable: Path,
    dbt_python: Path,
    expected_test_count: int,
) -> str:
    """임시 배포를 완성·검증하고 불변 경로와 current pointer를 원자 게시한다."""
    _project_import(source)
    from pipelines.orchestration.dbt_deployment import (
        canonical_json,
        model_version,
        source_file_digests,
        validate_deployment,
    )

    output.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".building-", dir=output))
    try:
        project_path = temporary / "project"
        _copy_sources(source, project_path)
        parse_result = _run_dbt(
            dbt_executable,
            project_path,
            temporary / "parse",
            ["parse"],
        )
        del parse_result
        manifest_body = _normalize_manifest(temporary / "parse" / "target" / "manifest.json")
        (temporary / "manifest.json").write_bytes(manifest_body)

        listed = _listed_tests(
            _run_dbt(
                dbt_executable,
                project_path,
                temporary / "list",
                ["ls", "--resource-type", "test", "--output", "json", "--output-keys", "unique_id"],
            )
        )
        manifest = json.loads(manifest_body)
        manifest_tests = sorted(
            unique_id
            for unique_id, node in manifest["nodes"].items()
            if node.get("resource_type") == "test"
        )
        if listed != manifest_tests:
            raise RuntimeError("dbt ls와 manifest의 test unique_id 집합이 다릅니다")
        if len(listed) != expected_test_count:
            raise RuntimeError(
                f"dbt test baseline은 {expected_test_count}개지만 현재 {len(listed)}개입니다"
            )

        models = sorted(
            unique_id
            for unique_id, node in manifest["nodes"].items()
            if node.get("resource_type") == "model"
        )
        contract = {
            "schema_version": 1,
            "project_name": PROJECT_NAME,
            "profile_name": PROFILE_NAME,
            "target_name": TARGET_NAME,
            "model_version": model_version(project_path),
            "manifest_sha256": hashlib.sha256(manifest_body).hexdigest(),
            "dbt_versions": _dbt_versions(dbt_python),
            "source_files": source_file_digests(project_path),
            "model_unique_ids": models,
            "test_unique_ids": listed,
        }
        deployment_id = hashlib.sha256(canonical_json(contract)).hexdigest()
        descriptor = {**contract, "deployment_id": deployment_id}
        (temporary / "deployment.json").write_bytes(canonical_json(descriptor))

        final_path = output / deployment_id
        if final_path.exists():
            validate_deployment(output, deployment_id)
        else:
            os.replace(temporary, final_path)
            temporary = final_path
            validate_deployment(output, deployment_id)

        pointer_temp = output / f".current-{deployment_id}.json"
        pointer_temp.write_bytes(canonical_json({"deployment_id": deployment_id}))
        os.replace(pointer_temp, output / "current.json")
        return deployment_id
    finally:
        if temporary.exists() and temporary.name.startswith(".building-"):
            shutil.rmtree(temporary)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dbt-executable", type=Path, default=DEFAULT_DBT_EXECUTABLE)
    parser.add_argument("--dbt-python", type=Path, default=DEFAULT_DBT_PYTHON)
    parser.add_argument("--expected-test-count", type=int, default=43)
    args = parser.parse_args()
    deployment_id = deploy(
        source=args.source,
        output=args.output,
        dbt_executable=args.dbt_executable,
        dbt_python=args.dbt_python,
        expected_test_count=args.expected_test_count,
    )
    print(deployment_id)


if __name__ == "__main__":
    main()
