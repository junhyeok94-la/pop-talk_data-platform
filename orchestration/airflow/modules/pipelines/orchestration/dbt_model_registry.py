"""모델별 DAG와 dbt test gate의 manifest 기반 소유권 계약."""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Literal, Mapping

from pipelines.orchestration.model_generation_contract import (
    GenerationContractError,
    GraphSnapshot,
    graph_snapshot_from_manifest,
)
from pipelines.orchestration.dbt_deployment import DbtDeployment, validate_deployment

OwnerKind = Literal["MODEL_GATE", "SOURCE_GATE", "DEPLOYMENT_GATE"]


@dataclass(frozen=True)
class TestOwnership:
    """test unique ID를 정확히 하나의 실행 gate에 배정한다."""

    test_unique_id: str
    selector: str
    owner_kind: OwnerKind
    owner_unique_id: str
    parent_unique_ids: tuple[str, ...]


@dataclass(frozen=True)
class ModelDagSpec:
    """한 모델 DAG가 실행할 정확한 selector와 직접 부모."""

    model_unique_id: str
    selector: str
    direct_parent_unique_ids: tuple[str, ...]
    owned_test_unique_ids: tuple[str, ...]
    owned_test_selectors: tuple[str, ...]


@dataclass(frozen=True)
class ModelDagRegistry:
    """승인 deployment manifest 전체를 빠짐없이 덮는 모델/test registry."""

    deployment_id: str
    graph: GraphSnapshot
    models: tuple[ModelDagSpec, ...]
    test_ownership: tuple[TestOwnership, ...]


def _test_nodes(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    nodes = manifest.get("nodes", {})
    if not isinstance(nodes, Mapping):
        raise GenerationContractError("dbt manifest nodes 형식이 올바르지 않습니다")
    return {
        str(unique_id): node
        for unique_id, node in nodes.items()
        if isinstance(node, Mapping) and node.get("resource_type") == "test"
    }


def _model_selectors(manifest: Mapping[str, Any]) -> dict[str, str]:
    selectors: dict[str, str] = {}
    for unique_id, node in manifest.get("nodes", {}).items():
        if not isinstance(node, Mapping) or node.get("resource_type") != "model":
            continue
        fqn = node.get("fqn")
        if not isinstance(fqn, list) or not fqn or not all(isinstance(v, str) for v in fqn):
            raise GenerationContractError(f"{unique_id} model fqn이 없습니다")
        selectors[str(unique_id)] = "fqn:" + ".".join(fqn)
    return selectors


def _available_resources_for_model(
    graph: GraphSnapshot,
    model_unique_id: str,
) -> frozenset[str]:
    """model gate가 cohort로 고정할 수 있는 자기 자신과 모든 ancestor를 찾는다."""
    dependencies = graph.dependency_map
    if model_unique_id not in dependencies:
        raise GenerationContractError("test owner model이 graph에 없습니다")
    available = {model_unique_id}
    pending = [model_unique_id]
    while pending:
        current = pending.pop()
        for parent in dependencies.get(current, ()):
            if parent not in available:
                available.add(parent)
                if parent.startswith("model."):
                    pending.append(parent)
    return frozenset(available)


def build_test_ownership(
    manifest: Mapping[str, Any],
    *,
    explicit_owner_by_test_name: Mapping[str, tuple[OwnerKind, str]],
    graph: GraphSnapshot | None = None,
) -> tuple[TestOwnership, ...]:
    """단일 부모 test는 자동 배정하고 모호한 test는 명시적 소유자를 요구한다."""
    ownership: list[TestOwnership] = []
    for test_id, node in sorted(_test_nodes(manifest).items()):
        raw_parents = tuple(
            str(parent) for parent in node.get("depends_on", {}).get("nodes", [])
        )
        unsupported = [
            parent
            for parent in raw_parents
            if not parent.startswith(("model.", "source."))
        ]
        if unsupported:
            raise GenerationContractError(
                f"test {test_id}에 지원하지 않는 부모 resource가 있습니다"
            )
        parents = tuple(sorted(raw_parents))
        name = str(node.get("name", ""))
        fqn = node.get("fqn")
        if not isinstance(fqn, list) or not fqn or not all(isinstance(v, str) for v in fqn):
            raise GenerationContractError(f"{test_id} test fqn이 없습니다")
        selector = "fqn:" + ".".join(fqn)
        explicit = explicit_owner_by_test_name.get(name)
        if explicit is not None:
            owner_kind, owner_id = explicit
        elif len(parents) == 1 and parents[0].startswith("model."):
            owner_kind, owner_id = "MODEL_GATE", parents[0]
        elif len(parents) == 1 and parents[0].startswith("source."):
            owner_kind, owner_id = "SOURCE_GATE", parents[0]
        else:
            raise GenerationContractError(
                f"test {test_id}는 부모가 {len(parents)}개이므로 명시적 owner가 필요합니다"
            )
        if owner_kind not in {"MODEL_GATE", "SOURCE_GATE", "DEPLOYMENT_GATE"}:
            raise GenerationContractError(f"test {test_id} owner kind가 올바르지 않습니다")
        if owner_kind == "MODEL_GATE":
            if graph is None:
                # 독립 helper 사용자는 manifest graph를 넘겨 의미상 readiness까지 검사해야 한다.
                graph = graph_snapshot_from_manifest(manifest, deployment_id="0" * 64)
            available = _available_resources_for_model(graph, owner_id)
            unavailable = set(parents) - set(available)
            if unavailable:
                raise GenerationContractError(
                    f"test {test_id} owner gate에서 준비할 수 없는 부모입니다: {sorted(unavailable)}"
                )
        if owner_kind == "SOURCE_GATE":
            if graph is None:
                graph = graph_snapshot_from_manifest(manifest, deployment_id="0" * 64)
            if owner_id not in graph.sources or parents != (owner_id,):
                raise GenerationContractError(f"source test {test_id} owner가 실제 source 부모가 아닙니다")
        if owner_kind == "DEPLOYMENT_GATE" and parents:
            raise GenerationContractError("부모가 있는 test를 deployment gate에 배정할 수 없습니다")
        ownership.append(TestOwnership(test_id, selector, owner_kind, owner_id, parents))
    known_names = {str(node.get("name", "")) for node in _test_nodes(manifest).values()}
    unused_overrides = set(explicit_owner_by_test_name) - known_names
    if unused_overrides:
        raise GenerationContractError(
            f"manifest에 없는 test owner override입니다: {sorted(unused_overrides)}"
        )
    return tuple(ownership)


def build_model_dag_registry(
    manifest: Mapping[str, Any],
    *,
    deployment_id: str,
    explicit_owner_by_test_name: Mapping[str, tuple[OwnerKind, str]],
) -> ModelDagRegistry:
    """current가 아닌 주어진 불변 manifest 하나에서 전체 registry를 만든다."""
    graph = graph_snapshot_from_manifest(manifest, deployment_id=deployment_id)
    selectors = _model_selectors(manifest)
    ownership = build_test_ownership(
        manifest,
        explicit_owner_by_test_name=explicit_owner_by_test_name,
        graph=graph,
    )
    if set(selectors) != set(graph.models):
        raise GenerationContractError("model selector 집합이 graph model 집합과 다릅니다")

    owned_by_model: dict[str, list[TestOwnership]] = {
        model_id: [] for model_id in graph.models
    }
    for item in ownership:
        if item.owner_kind == "MODEL_GATE":
            if item.owner_unique_id not in owned_by_model:
                raise GenerationContractError("test owner model이 graph에 없습니다")
            owned_by_model[item.owner_unique_id].append(item)
    models = tuple(
        ModelDagSpec(
            model_unique_id=model_id,
            selector=selectors[model_id],
            direct_parent_unique_ids=graph.dependency_map[model_id],
            owned_test_unique_ids=tuple(
                sorted(item.test_unique_id for item in owned_by_model[model_id])
            ),
            owned_test_selectors=tuple(
                sorted(item.selector for item in owned_by_model[model_id])
            ),
        )
        for model_id in graph.models
    )
    test_ids = set(_test_nodes(manifest))
    assigned = [item.test_unique_id for item in ownership]
    if len(assigned) != len(set(assigned)) or set(assigned) != test_ids:
        raise GenerationContractError("dbt test unique ID가 누락 또는 중복 배정됐습니다")
    return ModelDagRegistry(deployment_id, graph, models, ownership)


CURRENT_TEST_OWNER_OVERRIDES: dict[str, tuple[OwnerKind, str]] = {
    # fact와 mart를 함께 읽으므로 두 결과가 준비되는 mart gate가 소유한다.
    "assert_mart_boxoffice_preserves_fact_grain": (
        "MODEL_GATE",
        "model.pop_talk_dw.mart_boxoffice_daily",
    ),
    # ref/source가 없는 시간 계약 fixture는 모델 실행이 아니라 배포 CI가 소유한다.
    "assert_revision_precedes_completion_time": (
        "DEPLOYMENT_GATE",
        "dbt_deployment_validation",
    ),
}


def registry_from_validated_deployment(
    deployment: DbtDeployment,
    *,
    explicit_owner_by_test_name: Mapping[str, tuple[OwnerKind, str]],
) -> ModelDagRegistry:
    """descriptor/source/manifest를 다시 검증한 뒤 권위 registry를 직접 재생성한다."""
    verified = validate_deployment(
        deployment.root.parent,
        deployment.deployment_id,
        expected_manifest_sha256=deployment.manifest_sha256,
        expected_model_version=deployment.model_version,
    )
    if verified != deployment:
        raise GenerationContractError("전달된 deployment와 재검증 결과가 다릅니다")
    try:
        manifest = json.loads(verified.manifest_path.read_bytes())
    except (OSError, UnicodeError, ValueError) as error:
        raise GenerationContractError("권위 deployment manifest를 읽을 수 없습니다") from error
    registry = build_model_dag_registry(
        manifest,
        deployment_id=verified.deployment_id,
        explicit_owner_by_test_name=explicit_owner_by_test_name,
    )
    if (
        tuple(item.model_unique_id for item in registry.models)
        != verified.model_unique_ids
        or tuple(item.test_unique_id for item in registry.test_ownership)
        != verified.test_unique_ids
    ):
        raise GenerationContractError("deployment descriptor와 registry 전체 resource 집합이 다릅니다")
    return registry
