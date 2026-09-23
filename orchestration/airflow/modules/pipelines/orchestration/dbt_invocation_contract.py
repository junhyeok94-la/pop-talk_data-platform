"""모델 하나의 dbt 실행 범위와 결과 증거를 결정적으로 검증한다.

Airflow task ID나 CLI 문자열을 권위로 사용하지 않는다. 승인 deployment registry에서 만든
contract가 실행 범위이며, dbt 자체 invocation UUID는 실행 뒤 JSON log와 run_results에서 서로
독립적으로 관측한다.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from pipelines.orchestration.model_generation_contract import canonical_json, content_id

InvocationKind = Literal["MODEL", "OWNED_TESTS", "EMPTY_TEST_SET"]
SHA256_RE = re.compile(r"[0-9a-f]{64}")


class DbtInvocationContractError(RuntimeError):
    """exact dbt 실행 계약 또는 실행 증거가 어긋났을 때 발생한다."""


@dataclass(frozen=True)
class DbtInvocationContract:
    contract_version: int
    orchestration_invocation_id: str
    invocation_kind: InvocationKind
    deployment_id: str
    manifest_sha256: str
    model_version: str
    plan_id: str
    cohort_manifest_id: str
    build_id: str
    attempt_no: int
    fence_token: int
    claim_owner: str
    model_unique_id: str
    expected_unique_ids: tuple[str, ...]
    expected_selectors: tuple[str, ...]
    artifact_root: str
    relation_vars_json: str

    @property
    def body(self) -> dict[str, Any]:
        body = asdict(self)
        body["expected_unique_ids"] = list(self.expected_unique_ids)
        body["expected_selectors"] = list(self.expected_selectors)
        return json.loads(canonical_json(body))

    @property
    def contract_sha256(self) -> str:
        return hashlib.sha256(canonical_json(self.body)).hexdigest()


@dataclass(frozen=True)
class DbtInvocationArtifact:
    orchestration_invocation_id: str
    dbt_native_invocation_id: str | None
    build_id: str
    attempt_no: int
    fence_token: int
    cohort_manifest_id: str
    deployment_id: str
    model_unique_id: str
    invocation_kind: InvocationKind
    expected_unique_ids: tuple[str, ...]
    executed_unique_ids: tuple[str, ...]
    run_results_sha256: str | None
    argv_sha256: str
    environment_sha256: str
    artifact_path: str
    status: Literal["SUCCEEDED", "EMPTY_TEST_SET"]

    @property
    def body(self) -> dict[str, Any]:
        body = asdict(self)
        body["expected_unique_ids"] = list(self.expected_unique_ids)
        body["executed_unique_ids"] = list(self.executed_unique_ids)
        return json.loads(canonical_json(body))


def _base_invocation_body(**values: Any) -> dict[str, Any]:
    return {
        "contract_version": 1,
        "invocation_kind": values["invocation_kind"],
        "deployment_id": values["deployment_id"],
        "manifest_sha256": values["manifest_sha256"],
        "model_version": values["model_version"],
        "plan_id": values["plan_id"],
        "cohort_manifest_id": values["cohort_manifest_id"],
        "build_id": values["build_id"],
        "attempt_no": values["attempt_no"],
        "fence_token": values["fence_token"],
        "claim_owner": values["claim_owner"],
        "model_unique_id": values["model_unique_id"],
        "expected_unique_ids": list(values["expected_unique_ids"]),
        "expected_selectors": list(values["expected_selectors"]),
        "artifact_root": values["artifact_root"],
        "relation_vars_json": values["relation_vars_json"],
    }


def create_dbt_invocation_contract(
    *,
    invocation_kind: InvocationKind,
    deployment_id: str,
    manifest_sha256: str,
    model_version: str,
    plan_id: str,
    cohort_manifest_id: str,
    build_id: str,
    attempt_no: int,
    fence_token: int,
    claim_owner: str,
    model_unique_id: str,
    expected_unique_ids: Sequence[str],
    expected_selectors: Sequence[str],
    artifact_root: str,
    relation_vars: Mapping[str, Any] | None = None,
) -> DbtInvocationContract:
    """권위 입력에서 content-addressed orchestration invocation을 만든다."""
    ids = tuple(expected_unique_ids)
    selectors = tuple(expected_selectors)
    if invocation_kind == "MODEL" and (ids != (model_unique_id,) or len(selectors) != 1):
        raise DbtInvocationContractError("MODEL은 자기 model ID와 selector 하나만 실행해야 합니다")
    if invocation_kind == "EMPTY_TEST_SET" and (ids or selectors):
        raise DbtInvocationContractError("EMPTY_TEST_SET에는 실행 대상이 없어야 합니다")
    if invocation_kind == "OWNED_TESTS" and (not ids or len(ids) != len(selectors)):
        raise DbtInvocationContractError("OWNED_TESTS ID/selector 집합이 비었거나 크기가 다릅니다")
    if len(set(ids)) != len(ids) or len(set(selectors)) != len(selectors):
        raise DbtInvocationContractError("실행 대상 ID/selector가 중복됐습니다")
    if invocation_kind not in {"MODEL", "OWNED_TESTS", "EMPTY_TEST_SET"}:
        raise DbtInvocationContractError("지원하지 않는 invocation kind입니다")
    for digest in (deployment_id, manifest_sha256, model_version):
        if not SHA256_RE.fullmatch(digest):
            raise DbtInvocationContractError("deployment digest 형식이 올바르지 않습니다")
    if attempt_no < 1 or fence_token < 1 or not claim_owner:
        raise DbtInvocationContractError("attempt/fence/claim owner가 올바르지 않습니다")
    if any(not selector.startswith("fqn:") for selector in selectors):
        raise DbtInvocationContractError("strict 실행은 fqn selector만 허용합니다")
    relation_vars_json = canonical_json(relation_vars or {}).decode("utf-8")
    base = _base_invocation_body(
        invocation_kind=invocation_kind,
        deployment_id=deployment_id,
        manifest_sha256=manifest_sha256,
        model_version=model_version,
        plan_id=plan_id,
        cohort_manifest_id=cohort_manifest_id,
        build_id=build_id,
        attempt_no=attempt_no,
        fence_token=fence_token,
        claim_owner=claim_owner,
        model_unique_id=model_unique_id,
        expected_unique_ids=ids,
        expected_selectors=selectors,
        artifact_root=str(Path(artifact_root)),
        relation_vars_json=relation_vars_json,
    )
    invocation_id = content_id("dbt-invocation", base)
    return DbtInvocationContract(
        orchestration_invocation_id=invocation_id,
        **{**base, "expected_unique_ids": ids, "expected_selectors": selectors},
    )


def contract_from_json(raw: str) -> DbtInvocationContract:
    try:
        body = json.loads(raw)
        contract = DbtInvocationContract(
            **{
                **body,
                "expected_unique_ids": tuple(body["expected_unique_ids"]),
                "expected_selectors": tuple(body["expected_selectors"]),
            }
        )
    except (KeyError, TypeError, ValueError) as error:
        raise DbtInvocationContractError("strict dbt contract JSON을 읽을 수 없습니다") from error
    expected = create_dbt_invocation_contract(
        invocation_kind=contract.invocation_kind,
        deployment_id=contract.deployment_id,
        manifest_sha256=contract.manifest_sha256,
        model_version=contract.model_version,
        plan_id=contract.plan_id,
        cohort_manifest_id=contract.cohort_manifest_id,
        build_id=contract.build_id,
        attempt_no=contract.attempt_no,
        fence_token=contract.fence_token,
        claim_owner=contract.claim_owner,
        model_unique_id=contract.model_unique_id,
        expected_unique_ids=contract.expected_unique_ids,
        expected_selectors=contract.expected_selectors,
        artifact_root=contract.artifact_root,
        relation_vars=json.loads(contract.relation_vars_json),
    )
    if contract != expected:
        raise DbtInvocationContractError("strict dbt contract self-seal이 다릅니다")
    return contract


def read_regular_file_nofollow(path: Path) -> bytes:
    """root dirfd부터 각 부모와 최종 파일을 열어 symlink 경로 교체를 막는다."""
    absolute = Path(os.path.abspath(path))
    directory_flags = os.O_RDONLY
    file_flags = os.O_RDONLY
    for flag_name in ("O_CLOEXEC", "O_NOFOLLOW"):
        flag = getattr(os, flag_name, 0)
        directory_flags |= flag
        file_flags |= flag
    directory_flags |= getattr(os, "O_DIRECTORY", 0)
    directory_descriptor: int | None = None
    try:
        if not (hasattr(os, "O_NOFOLLOW") and os.open in os.supports_dir_fd):
            raise DbtInvocationContractError(
                "부모 경로 no-follow를 지원하는 런타임이 필요합니다"
            )
        directory_descriptor = os.open(absolute.anchor, directory_flags)
        for component in absolute.parts[1:-1]:
            next_descriptor = os.open(
                component,
                directory_flags,
                dir_fd=directory_descriptor,
            )
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor
        descriptor = os.open(
            absolute.name,
            file_flags,
            dir_fd=directory_descriptor,
        )
    except OSError as error:
        if directory_descriptor is not None:
            os.close(directory_descriptor)
        raise DbtInvocationContractError("검증 파일을 안전하게 열 수 없습니다") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise DbtInvocationContractError("검증 파일이 regular file이 아닙니다")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)
        if directory_descriptor is not None:
            os.close(directory_descriptor)


def _native_ids_from_json_log(log_path: Path) -> set[str]:
    ids: set[str] = set()
    try:
        for line in read_regular_file_nofollow(log_path).decode("utf-8").splitlines():
            event = json.loads(line)
            info = event.get("info", {})
            invocation_id = info.get("invocation_id") or event.get("invocation_id")
            if invocation_id:
                uuid.UUID(str(invocation_id))
                ids.add(str(invocation_id))
    except (OSError, UnicodeError, ValueError, TypeError) as error:
        raise DbtInvocationContractError("dbt JSON file log를 검증할 수 없습니다") from error
    if len(ids) != 1:
        raise DbtInvocationContractError("dbt JSON file log의 native invocation ID는 하나여야 합니다")
    return ids


def validate_run_results(
    *,
    contract: DbtInvocationContract,
    run_results_path: Path,
    log_path: Path,
    manifest: Mapping[str, Any],
) -> tuple[str, tuple[str, ...], str]:
    """fresh run-results v6를 exact ID/status 및 독립 JSON log ID와 대사한다."""
    if contract.invocation_kind == "EMPTY_TEST_SET":
        raise DbtInvocationContractError("EMPTY_TEST_SET에는 run_results가 없어야 합니다")
    try:
        raw = read_regular_file_nofollow(run_results_path)
        result_body = json.loads(raw)
    except (OSError, UnicodeError, ValueError) as error:
        raise DbtInvocationContractError("fresh run_results.json을 읽을 수 없습니다") from error
    native_id = str(result_body.get("metadata", {}).get("invocation_id", ""))
    try:
        uuid.UUID(native_id)
    except ValueError as error:
        raise DbtInvocationContractError("run_results native invocation ID가 UUID가 아닙니다") from error
    if _native_ids_from_json_log(log_path) != {native_id}:
        raise DbtInvocationContractError("JSON log와 run_results native invocation ID가 다릅니다")
    results = result_body.get("results")
    if not isinstance(results, list):
        raise DbtInvocationContractError("run_results results가 배열이 아닙니다")
    executed = tuple(str(item.get("unique_id", "")) for item in results if isinstance(item, Mapping))
    if len(executed) != len(results) or len(set(executed)) != len(executed):
        raise DbtInvocationContractError("run_results ID가 누락 또는 중복됐습니다")
    if set(executed) != set(contract.expected_unique_ids):
        raise DbtInvocationContractError("실제 실행 ID 집합이 exact contract와 다릅니다")
    nodes = manifest.get("nodes", {})
    expected_type = "model" if contract.invocation_kind == "MODEL" else "test"
    expected_status = "success" if expected_type == "model" else "pass"
    for item in results:
        unique_id = str(item["unique_id"])
        node = nodes.get(unique_id)
        if not isinstance(node, Mapping) or node.get("resource_type") != expected_type:
            raise DbtInvocationContractError("실행 resource type이 deployment manifest와 다릅니다")
        if item.get("status") != expected_status:
            raise DbtInvocationContractError("dbt 실행 status가 성공 계약과 다릅니다")
    return native_id, tuple(sorted(executed)), hashlib.sha256(raw).hexdigest()
