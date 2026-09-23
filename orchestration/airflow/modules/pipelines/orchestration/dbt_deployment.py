"""Cosmos가 읽고 실행할 content-addressed dbt 배포본의 계약을 검증한다."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

SOURCE_SUFFIXES = {".sql", ".yml", ".yaml"}
EXCLUDED_DIRECTORIES = {"target", "logs", "dbt_packages", "dbt_deployments"}
EXCLUDED_FILE_NAMES = {".user.yml"}


class DbtDeploymentError(RuntimeError):
    """불변 dbt 배포 계약이 일치하지 않을 때 발생한다."""


@dataclass(frozen=True)
class DbtDeployment:
    """하나의 serialized DAG가 끝까지 사용해야 하는 고정 dbt 배포본."""

    deployment_id: str
    root: Path
    project_path: Path
    manifest_path: Path
    model_version: str
    manifest_sha256: str
    profile_name: str
    target_name: str
    model_unique_ids: tuple[str, ...]
    model_selectors: tuple[str, ...]
    test_unique_ids: tuple[str, ...]


def canonical_json(value: Any) -> bytes:
    """계약 hash에 사용할 결정적 UTF-8 JSON을 만든다."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def source_files(project_path: Path) -> Iterator[Path]:
    """dbt 실행 결과에 영향을 주는 SQL/YAML 파일만 안정된 순서로 찾는다."""
    yield from sorted(
        path
        for path in project_path.rglob("*")
        if path.is_file()
        and path.suffix.lower() in SOURCE_SUFFIXES
        and path.name not in EXCLUDED_FILE_NAMES
        and not EXCLUDED_DIRECTORIES.intersection(path.relative_to(project_path).parts)
    )


def source_file_digests(project_path: Path) -> dict[str, str]:
    """상대 경로별 source SHA-256을 계산한다."""
    return {
        path.relative_to(project_path).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in source_files(project_path)
    }


def model_version(project_path: Path) -> str:
    """기존 Gold publication identity와 같은 SQL/YAML model version을 계산한다."""
    digest_input = b"".join(
        path.relative_to(project_path).as_posix().encode("utf-8")
        + b"\0"
        + path.read_bytes()
        + b"\0"
        for path in source_files(project_path)
    )
    return hashlib.sha256(digest_input).hexdigest()


def _deployment_id(contract: dict[str, Any]) -> str:
    identity_contract = dict(contract)
    identity_contract.pop("deployment_id", None)
    return hashlib.sha256(canonical_json(identity_contract)).hexdigest()


def _manifest_nodes(manifest: dict[str, Any], resource_type: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            unique_id
            for unique_id, node in manifest.get("nodes", {}).items()
            if node.get("resource_type") == resource_type
        )
    )


def _model_selectors(manifest: dict[str, Any]) -> tuple[str, ...]:
    """manifest model마다 Cosmos가 실행에 사용하는 완전한 fqn selector를 만든다."""
    return tuple(
        sorted(
            "fqn:" + ".".join(node["fqn"])
            for node in manifest.get("nodes", {}).values()
            if node.get("resource_type") == "model" and node.get("fqn")
        )
    )


def validate_deployment(
    deployment_root: Path,
    deployment_id: str,
    *,
    expected_manifest_sha256: str | None = None,
    expected_model_version: str | None = None,
) -> DbtDeployment:
    """고정 descriptor, manifest와 source가 서로 같은 배포를 나타내는지 대사한다."""
    if not re.fullmatch(r"[0-9a-f]{64}", deployment_id):
        raise DbtDeploymentError("dbt deployment ID 형식이 올바르지 않습니다")
    root = deployment_root / deployment_id
    descriptor_path = root / "deployment.json"
    manifest_path = root / "manifest.json"
    project_path = root / "project"
    try:
        descriptor = json.loads(descriptor_path.read_bytes())
        manifest_body = manifest_path.read_bytes()
        manifest = json.loads(manifest_body)
    except (OSError, UnicodeError, ValueError) as error:
        raise DbtDeploymentError("dbt 배포 descriptor 또는 manifest를 읽을 수 없습니다") from error

    if descriptor.get("schema_version") != 1:
        raise DbtDeploymentError("지원하지 않는 dbt 배포 계약입니다")
    if descriptor.get("deployment_id") != deployment_id or root.name != deployment_id:
        raise DbtDeploymentError("dbt 배포 경로와 deployment ID가 다릅니다")
    if _deployment_id(descriptor) != deployment_id:
        raise DbtDeploymentError("dbt deployment descriptor digest가 다릅니다")

    manifest_sha256 = hashlib.sha256(manifest_body).hexdigest()
    if descriptor.get("manifest_sha256") != manifest_sha256:
        raise DbtDeploymentError("dbt manifest digest가 descriptor와 다릅니다")
    if expected_manifest_sha256 and manifest_sha256 != expected_manifest_sha256:
        raise DbtDeploymentError("DAG가 고정한 dbt manifest digest와 다릅니다")

    actual_model_version = model_version(project_path)
    if descriptor.get("model_version") != actual_model_version:
        raise DbtDeploymentError("dbt source model version이 descriptor와 다릅니다")
    if expected_model_version and actual_model_version != expected_model_version:
        raise DbtDeploymentError("DAG가 고정한 dbt model version과 다릅니다")
    if descriptor.get("source_files") != source_file_digests(project_path):
        raise DbtDeploymentError("dbt source file 집합 또는 digest가 다릅니다")

    metadata = manifest.get("metadata", {})
    if metadata.get("project_name") != descriptor.get("project_name"):
        raise DbtDeploymentError("dbt manifest project identity가 다릅니다")
    if metadata.get("dbt_version") != descriptor.get("dbt_versions", {}).get("dbt-core"):
        raise DbtDeploymentError("dbt manifest 생성 버전이 descriptor와 다릅니다")

    model_ids = _manifest_nodes(manifest, "model")
    model_selectors = _model_selectors(manifest)
    test_ids = _manifest_nodes(manifest, "test")
    if list(model_ids) != descriptor.get("model_unique_ids"):
        raise DbtDeploymentError("dbt manifest model 집합이 descriptor와 다릅니다")
    if list(test_ids) != descriptor.get("test_unique_ids"):
        raise DbtDeploymentError("dbt manifest test 집합이 descriptor와 다릅니다")
    if len(model_selectors) != len(model_ids):
        raise DbtDeploymentError("dbt manifest model selector가 완전하지 않습니다")
    profile_name = descriptor.get("profile_name")
    target_name = descriptor.get("target_name")
    if not isinstance(profile_name, str) or not profile_name:
        raise DbtDeploymentError("dbt profile 이름이 deployment 계약에 없습니다")
    if not isinstance(target_name, str) or not target_name:
        raise DbtDeploymentError("dbt target 이름이 deployment 계약에 없습니다")

    return DbtDeployment(
        deployment_id=deployment_id,
        root=root,
        project_path=project_path,
        manifest_path=manifest_path,
        model_version=actual_model_version,
        manifest_sha256=manifest_sha256,
        profile_name=profile_name,
        target_name=target_name,
        model_unique_ids=model_ids,
        model_selectors=model_selectors,
        test_unique_ids=test_ids,
    )


def load_current_deployment(deployment_root: Path) -> DbtDeployment:
    """current pointer를 한 번 읽고 해당 불변 배포본을 완전히 검증한다."""
    try:
        pointer = json.loads((deployment_root / "current.json").read_bytes())
        deployment_id = str(pointer["deployment_id"])
    except (OSError, KeyError, UnicodeError, ValueError) as error:
        raise DbtDeploymentError("현재 dbt deployment pointer를 읽을 수 없습니다") from error
    return validate_deployment(deployment_root, deployment_id)
