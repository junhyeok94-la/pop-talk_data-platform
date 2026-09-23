"""모델 단위 DAG가 공유하는 generation·result·cohort 순수 계약.

이 모듈은 Airflow나 외부 DB를 import하지 않는다. scheduler의 Asset event는 깨우기
신호일 뿐이며, 실제 실행 가능 여부는 여기서 정의한 불변 manifest와 durable ledger가
판정한다. 저장 adapter는 다음 단계에서 이 값들을 PostgreSQL/Snowflake/S3에 보존한다.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Literal, Mapping, Sequence

CONTRACT_VERSION = 2
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
GENERATION_PATTERN = re.compile(r"gen-[0-9]{12}")
SlotMode = Literal["REBUILD", "REUSE"]
GateKind = Literal["SOURCE_GATE", "DEPLOYMENT_GATE"]


class GenerationContractError(ValueError):
    """세대 또는 부모 snapshot이 불변 계약과 다를 때 발생한다."""


@dataclass(frozen=True)
class GateReceipt:
    """한 generation에서 source 또는 deployment 검증이 끝났다는 불변 증거."""

    gate_receipt_id: str
    gate_kind: GateKind
    generation_id: str
    plan_id: str
    deployment_id: str
    owner_unique_id: str
    source_snapshot_id: str | None
    executed_test_ids: tuple[str, ...]
    evidence_sha256: str


def create_gate_receipt(
    *,
    gate_kind: GateKind,
    generation_id: str,
    plan_id: str,
    deployment_id: str,
    owner_unique_id: str,
    source_snapshot_id: str | None,
    executed_test_ids: Sequence[str],
    evidence_sha256: str,
) -> GateReceipt:
    """검증 대상과 실행 증거를 content-addressed receipt 하나로 봉인한다."""
    body = {
        "contract_version": CONTRACT_VERSION,
        "gate_kind": gate_kind,
        "generation_id": generation_id,
        "plan_id": plan_id,
        "deployment_id": deployment_id,
        "owner_unique_id": owner_unique_id,
        "source_snapshot_id": source_snapshot_id,
        "executed_test_ids": sorted(executed_test_ids),
        "evidence_sha256": evidence_sha256,
    }
    receipt = GateReceipt(
        gate_receipt_id=content_id("gate", body),
        **{
            **{key: value for key, value in body.items() if key != "contract_version"},
            "executed_test_ids": tuple(body["executed_test_ids"]),
        },
    )
    return validate_gate_receipt(receipt)


def validate_gate_receipt(receipt: GateReceipt) -> GateReceipt:
    """DB에서 읽은 gate receipt의 identity와 대상 결합을 다시 계산한다."""
    if receipt.gate_kind not in {"SOURCE_GATE", "DEPLOYMENT_GATE"}:
        raise GenerationContractError("지원하지 않는 gate 종류입니다")
    if not GENERATION_PATTERN.fullmatch(receipt.generation_id) or not receipt.plan_id:
        raise GenerationContractError("gate generation/plan identity가 올바르지 않습니다")
    _require_sha256(receipt.deployment_id, "gate deployment_id")
    _require_sha256(receipt.evidence_sha256, "gate evidence_sha256")
    if not receipt.executed_test_ids or len(set(receipt.executed_test_ids)) != len(
        receipt.executed_test_ids
    ):
        raise GenerationContractError("gate test 증거가 비었거나 중복됐습니다")
    if receipt.gate_kind == "SOURCE_GATE":
        _require_resource_id(receipt.owner_unique_id, ("source.",))
        if not receipt.source_snapshot_id or not receipt.source_snapshot_id.startswith("src-"):
            raise GenerationContractError("source gate에 source snapshot ID가 없습니다")
    elif receipt.source_snapshot_id is not None or not receipt.owner_unique_id:
        raise GenerationContractError("deployment gate 대상 형식이 올바르지 않습니다")
    body = {
        "contract_version": CONTRACT_VERSION,
        "gate_kind": receipt.gate_kind,
        "generation_id": receipt.generation_id,
        "plan_id": receipt.plan_id,
        "deployment_id": receipt.deployment_id,
        "owner_unique_id": receipt.owner_unique_id,
        "source_snapshot_id": receipt.source_snapshot_id,
        "executed_test_ids": list(receipt.executed_test_ids),
        "evidence_sha256": receipt.evidence_sha256,
    }
    if receipt.gate_receipt_id != content_id("gate", body):
        raise GenerationContractError("gate receipt content ID가 다릅니다")
    return receipt


def canonical_json(value: Any) -> bytes:
    """manifest ID와 digest에 사용하는 결정적 JSON bytes를 만든다."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def content_id(prefix: str, value: Any) -> str:
    """사람이 종류를 구분할 수 있는 content-addressed ID를 만든다."""
    return f"{prefix}-{hashlib.sha256(canonical_json(value)).hexdigest()}"


def _require_sha256(value: str, field: str) -> None:
    if not SHA256_PATTERN.fullmatch(value):
        raise GenerationContractError(f"{field}는 64자리 소문자 SHA-256이어야 합니다")


def _require_resource_id(value: str, allowed: tuple[str, ...]) -> None:
    if not value.startswith(allowed):
        raise GenerationContractError(f"지원하지 않는 dbt resource ID입니다: {value}")


@dataclass(frozen=True)
class GraphSnapshot:
    """승인된 dbt manifest에서 추출한 model/source 직접 의존 그래프."""

    deployment_id: str
    dependencies: tuple[tuple[str, tuple[str, ...]], ...]
    sources: tuple[str, ...]
    graph_digest: str

    @property
    def dependency_map(self) -> dict[str, tuple[str, ...]]:
        return dict(self.dependencies)

    @property
    def models(self) -> tuple[str, ...]:
        return tuple(model_id for model_id, _ in self.dependencies)


def graph_snapshot_from_manifest(
    manifest: Mapping[str, Any],
    *,
    deployment_id: str,
) -> GraphSnapshot:
    """manifest model/source만 골라 cycle과 dangling parent를 거부한다."""
    _require_sha256(deployment_id, "deployment_id")
    nodes = manifest.get("nodes")
    sources_node = manifest.get("sources", {})
    if not isinstance(nodes, Mapping) or not isinstance(sources_node, Mapping):
        raise GenerationContractError("dbt manifest nodes/sources 형식이 올바르지 않습니다")

    unsupported_resources = sorted(
        str(unique_id)
        for unique_id, node in nodes.items()
        if isinstance(node, Mapping)
        and node.get("resource_type") in {"seed", "snapshot"}
    )
    if unsupported_resources:
        raise GenerationContractError(
            f"모델별 DAG에서 지원하지 않는 dbt resource입니다: {unsupported_resources}"
        )

    model_ids = {
        str(unique_id)
        for unique_id, node in nodes.items()
        if isinstance(node, Mapping) and node.get("resource_type") == "model"
    }
    source_ids = {
        str(unique_id)
        for unique_id, node in sources_node.items()
        if isinstance(node, Mapping) and node.get("resource_type") == "source"
    }
    if not model_ids:
        raise GenerationContractError("dbt manifest에 model이 없습니다")

    dependencies: list[tuple[str, tuple[str, ...]]] = []
    allowed_parents = model_ids | source_ids
    for model_id in sorted(model_ids):
        materialized = nodes[model_id].get("config", {}).get("materialized")
        if materialized == "ephemeral":
            raise GenerationContractError(
                f"{model_id} ephemeral 모델은 모델별 DAG 계약에서 지원하지 않습니다"
            )
        raw_parents = tuple(
            str(parent)
            for parent in nodes[model_id].get("depends_on", {}).get("nodes", [])
        )
        unsupported = [
            parent
            for parent in raw_parents
            if not parent.startswith(("model.", "source."))
        ]
        if unsupported:
            raise GenerationContractError(
                f"{model_id}에 지원하지 않는 부모 resource가 있습니다: {unsupported}"
            )
        parents = tuple(sorted(raw_parents))
        dangling = set(parents) - allowed_parents
        if dangling:
            raise GenerationContractError(
                f"{model_id}의 부모가 manifest에 없습니다: {sorted(dangling)}"
            )
        dependencies.append((model_id, parents))

    graph = dict(dependencies)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(model_id: str) -> None:
        if model_id in visiting:
            raise GenerationContractError("dbt model 의존 그래프에 cycle이 있습니다")
        if model_id in visited:
            return
        visiting.add(model_id)
        for parent in graph[model_id]:
            if parent in graph:
                visit(parent)
        visiting.remove(model_id)
        visited.add(model_id)

    for model_id in sorted(model_ids):
        visit(model_id)

    graph_body = {
        "deployment_id": deployment_id,
        "dependencies": dependencies,
        "sources": sorted(source_ids),
    }
    return GraphSnapshot(
        deployment_id=deployment_id,
        dependencies=tuple(dependencies),
        sources=tuple(sorted(source_ids)),
        graph_digest=hashlib.sha256(canonical_json(graph_body)).hexdigest(),
    )


def validate_graph_snapshot(graph: GraphSnapshot) -> GraphSnapshot:
    """역직렬화된 graph의 digest, resource, cycle과 중복을 다시 검사한다."""
    _require_sha256(graph.deployment_id, "deployment_id")
    _require_sha256(graph.graph_digest, "graph_digest")
    dependency_map = dict(graph.dependencies)
    if len(dependency_map) != len(graph.dependencies):
        raise GenerationContractError("graph model dependency가 중복됐습니다")
    models = set(dependency_map)
    sources = set(graph.sources)
    if len(sources) != len(graph.sources) or not models:
        raise GenerationContractError("graph source가 중복됐거나 model이 없습니다")
    for model_id, parents in graph.dependencies:
        _require_resource_id(model_id, ("model.",))
        if len(set(parents)) != len(parents):
            raise GenerationContractError(f"{model_id} parent가 중복됐습니다")
        for parent in parents:
            _require_resource_id(parent, ("model.", "source."))
            if parent not in models | sources:
                raise GenerationContractError(f"{model_id} parent가 graph에 없습니다")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(model_id: str) -> None:
        if model_id in visiting:
            raise GenerationContractError("dbt model 의존 그래프에 cycle이 있습니다")
        if model_id in visited:
            return
        visiting.add(model_id)
        for parent in dependency_map[model_id]:
            if parent in dependency_map:
                visit(parent)
        visiting.remove(model_id)
        visited.add(model_id)

    for model_id in sorted(models):
        visit(model_id)
    expected = hashlib.sha256(
        canonical_json(
            {
                "deployment_id": graph.deployment_id,
                "dependencies": graph.dependencies,
                "sources": sorted(graph.sources),
            }
        )
    ).hexdigest()
    if graph.graph_digest != expected:
        raise GenerationContractError("graph snapshot content digest가 다릅니다")
    return graph


def affected_models(
    graph: GraphSnapshot,
    changed_resources: Iterable[str],
) -> frozenset[str]:
    """변경 source/model에서 시작해 모든 model 후손을 전이적으로 찾는다."""
    known = set(graph.models) | set(graph.sources)
    affected = set(changed_resources)
    unknown = affected - known
    if unknown:
        raise GenerationContractError(f"변경 resource가 graph에 없습니다: {sorted(unknown)}")
    changed = True
    while changed:
        changed = False
        for model_id, parents in graph.dependencies:
            if model_id not in affected and set(parents) & affected:
                affected.add(model_id)
                changed = True
    return frozenset(affected & set(graph.models))


@dataclass(frozen=True)
class SourceSnapshot:
    """GenerationPlan이 고정하는 source 하나의 정확한 입력 snapshot."""

    source_unique_id: str
    source_batch_id: str
    dataset_revision: int
    manifest_key: str
    cutoff: str
    content_sha256: str
    snapshot_id: str
    relation_id: str | None = None


def source_snapshot_body(source: SourceSnapshot) -> dict[str, Any]:
    """기존 snapshot의 content ID를 유지하면서 물리 테이블 위치를 선택적으로 봉인한다."""
    body = asdict(source)
    if source.relation_id is None:
        body.pop("relation_id")
    return body


def create_source_snapshot(
    *,
    source_unique_id: str,
    source_batch_id: str,
    dataset_revision: int,
    manifest_key: str,
    cutoff: str,
    content_sha256: str,
    relation_id: str | None = None,
) -> SourceSnapshot:
    _require_resource_id(source_unique_id, ("source.",))
    _require_sha256(content_sha256, "source content_sha256")
    if not source_batch_id or dataset_revision < 1 or not manifest_key or not cutoff:
        raise GenerationContractError("source snapshot 필수 lineage가 비었습니다")
    body = {
        "contract_version": CONTRACT_VERSION,
        "source_unique_id": source_unique_id,
        "source_batch_id": source_batch_id,
        "dataset_revision": dataset_revision,
        "manifest_key": manifest_key,
        "cutoff": cutoff,
        "content_sha256": content_sha256,
    }
    if relation_id is not None:
        from pipelines.orchestration.dbt_relation_bindings import relation_parts
        relation_parts(relation_id)
        body["relation_id"] = relation_id
    return SourceSnapshot(
        **{
            **{key: value for key, value in body.items() if key != "contract_version"},
            "snapshot_id": content_id("src", body),
        }
    )


def validate_source_snapshot(source: SourceSnapshot) -> SourceSnapshot:
    """source snapshot ID와 그 안의 모든 lineage를 재계산한다."""
    _require_resource_id(source.source_unique_id, ("source.",))
    _require_sha256(source.content_sha256, "source content_sha256")
    if (
        not source.source_batch_id
        or source.dataset_revision < 1
        or not source.manifest_key
        or not source.cutoff
    ):
        raise GenerationContractError("source snapshot 필수 lineage가 비었습니다")
    body = {
        "contract_version": CONTRACT_VERSION,
        "source_unique_id": source.source_unique_id,
        "source_batch_id": source.source_batch_id,
        "dataset_revision": source.dataset_revision,
        "manifest_key": source.manifest_key,
        "cutoff": source.cutoff,
        "content_sha256": source.content_sha256,
    }
    if source.relation_id is not None:
        from pipelines.orchestration.dbt_relation_bindings import relation_parts
        relation_parts(source.relation_id)
        body["relation_id"] = source.relation_id
    if source.snapshot_id != content_id("src", body):
        raise GenerationContractError("source snapshot content ID가 다릅니다")
    return source


@dataclass(frozen=True)
class ModelResultManifest:
    """모델 성공 뒤 한 번만 봉인되는 generation별 물리 결과."""

    result_manifest_id: str
    generation_id: str
    plan_id: str
    deployment_id: str
    model_unique_id: str
    build_id: str
    cohort_manifest_id: str
    relation_id: str
    row_count: int
    content_sha256: str
    parent_binding_ids: tuple[str, ...]


def _model_result_body(result: ModelResultManifest) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "generation_id": result.generation_id,
        "plan_id": result.plan_id,
        "deployment_id": result.deployment_id,
        "model_unique_id": result.model_unique_id,
        "build_id": result.build_id,
        "cohort_manifest_id": result.cohort_manifest_id,
        "relation_id": result.relation_id,
        "row_count": result.row_count,
        "content_sha256": result.content_sha256,
        "parent_binding_ids": list(result.parent_binding_ids),
    }


def validate_model_result(result: ModelResultManifest) -> ModelResultManifest:
    """저장소에서 읽은 result의 content ID와 기본 구조를 재검증한다."""
    if not GENERATION_PATTERN.fullmatch(result.generation_id):
        raise GenerationContractError("result generation_id 형식이 올바르지 않습니다")
    _require_sha256(result.deployment_id, "result deployment_id")
    _require_sha256(result.content_sha256, "model result content_sha256")
    _require_resource_id(result.model_unique_id, ("model.",))
    if not result.relation_id or result.row_count < 0:
        raise GenerationContractError("result relation/row_count가 올바르지 않습니다")
    if len(set(result.parent_binding_ids)) != len(result.parent_binding_ids):
        raise GenerationContractError("result parent binding ID가 중복됐습니다")
    if result.result_manifest_id != content_id("result", _model_result_body(result)):
        raise GenerationContractError("ModelResultManifest content ID가 다릅니다")
    return result


@dataclass(frozen=True)
class BuildSlot:
    """GenerationPlan이 미리 정하는 모델 실행 또는 이전 결과 재사용 슬롯."""

    model_unique_id: str
    mode: SlotMode
    build_id: str
    reuse_result_manifest_id: str | None


@dataclass(frozen=True)
class GenerationPlan:
    """미래 결과 digest 없이 source/graph/build 결정을 봉인한 최초 plan."""

    plan_id: str
    generation_id: str
    generation_sequence: int
    deployment_id: str
    graph_digest: str
    graph_dependencies: tuple[tuple[str, tuple[str, ...]], ...]
    source_snapshots: tuple[SourceSnapshot, ...]
    build_slots: tuple[BuildSlot, ...]
    required_serving_components: tuple[str, ...]

    @property
    def slot_map(self) -> dict[str, BuildSlot]:
        return {slot.model_unique_id: slot for slot in self.build_slots}

    @property
    def source_map(self) -> dict[str, SourceSnapshot]:
        return {item.source_unique_id: item for item in self.source_snapshots}

    @property
    def dependency_map(self) -> dict[str, tuple[str, ...]]:
        return dict(self.graph_dependencies)


def _generation_plan_body(plan: GenerationPlan) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "generation_id": plan.generation_id,
        "generation_sequence": plan.generation_sequence,
        "deployment_id": plan.deployment_id,
        "graph_digest": plan.graph_digest,
        "graph_dependencies": plan.graph_dependencies,
        "source_snapshots": [
            source_snapshot_body(item)
            for item in sorted(plan.source_snapshots, key=lambda item: item.source_unique_id)
        ],
        "build_slots": [asdict(item) for item in plan.build_slots],
        "required_serving_components": list(plan.required_serving_components),
    }


def validate_generation_plan(plan: GenerationPlan) -> GenerationPlan:
    """저장소에서 읽은 plan의 ID, graph, source와 slot 구조를 재검증한다."""
    if plan.generation_sequence < 1 or plan.generation_id != f"gen-{plan.generation_sequence:012d}":
        raise GenerationContractError("generation ID와 sequence가 다릅니다")
    _require_sha256(plan.deployment_id, "deployment_id")
    graph = GraphSnapshot(
        deployment_id=plan.deployment_id,
        dependencies=plan.graph_dependencies,
        sources=tuple(item.source_unique_id for item in plan.source_snapshots),
        graph_digest=plan.graph_digest,
    )
    validate_graph_snapshot(graph)
    source_ids: set[str] = set()
    for source in plan.source_snapshots:
        validate_source_snapshot(source)
        if source.source_unique_id in source_ids:
            raise GenerationContractError("GenerationPlan source가 중복됐습니다")
        source_ids.add(source.source_unique_id)
    slots = plan.slot_map
    if len(slots) != len(plan.build_slots) or set(slots) != set(graph.models):
        raise GenerationContractError("GenerationPlan build slot 집합이 graph와 다릅니다")
    for slot in plan.build_slots:
        expected_build_id = content_id(
            "build",
            {
                "generation_id": plan.generation_id,
                "deployment_id": plan.deployment_id,
                "model_unique_id": slot.model_unique_id,
            },
        )
        if slot.build_id != expected_build_id:
            raise GenerationContractError("model build slot content ID가 다릅니다")
        if slot.mode == "REBUILD" and slot.reuse_result_manifest_id is not None:
            raise GenerationContractError("REBUILD slot에 reuse result가 있습니다")
        if slot.mode == "REUSE" and not slot.reuse_result_manifest_id:
            raise GenerationContractError("REUSE slot에 result binding이 없습니다")
        if slot.mode not in {"REBUILD", "REUSE"}:
            raise GenerationContractError("지원하지 않는 build slot mode입니다")
    if tuple(sorted(set(plan.required_serving_components))) != plan.required_serving_components:
        raise GenerationContractError("required serving component가 정렬되지 않았거나 중복됐습니다")
    if plan.plan_id != content_id("plan", _generation_plan_body(plan)):
        raise GenerationContractError("GenerationPlan content ID가 다릅니다")
    return plan


def _result_compatible_for_reuse(
    result: ModelResultManifest,
    *,
    model_id: str,
    deployment_id: str,
    previous_plan: GenerationPlan | None = None,
) -> bool:
    if result.model_unique_id != model_id or result.deployment_id != deployment_id:
        return False
    if previous_plan is None:
        return False
    previous_slot = previous_plan.slot_map.get(model_id)
    if previous_slot is None:
        return False
    if previous_slot.mode == "REUSE":
        return result.result_manifest_id == previous_slot.reuse_result_manifest_id
    return (
        result.plan_id == previous_plan.plan_id
        and result.generation_id == previous_plan.generation_id
        and result.build_id == previous_slot.build_id
    )


def create_generation_plan(
    *,
    generation_sequence: int,
    graph: GraphSnapshot,
    source_snapshots: Sequence[SourceSnapshot],
    changed_resources: Iterable[str],
    previous_results: Mapping[str, ModelResultManifest],
    required_serving_components: Sequence[str],
    previous_plan: GenerationPlan | None = None,
    manifest_catalog: ManifestCatalog | None = None,
) -> GenerationPlan:
    """변경 영향 후손은 REBUILD, 호환되는 나머지는 REUSE로 고정한다."""
    validate_graph_snapshot(graph)
    if generation_sequence < 1:
        raise GenerationContractError("generation_sequence는 1 이상이어야 합니다")
    for source in source_snapshots:
        validate_source_snapshot(source)
    if previous_plan is not None:
        validate_generation_plan(previous_plan)
        if manifest_catalog is None or manifest_catalog.plans.get(previous_plan.plan_id) != previous_plan:
            raise GenerationContractError("previous plan이 권위 catalog 객체와 다릅니다")
    authoritative_previous_results: dict[str, ModelResultManifest] = {}
    for model_id, result in previous_results.items():
        if manifest_catalog is None:
            raise GenerationContractError("previous result 재사용에는 provenance catalog가 필요합니다")
        authoritative = validate_result_provenance(
            result.result_manifest_id,
            catalog=manifest_catalog,
        )
        if result.model_unique_id != model_id:
            raise GenerationContractError("previous result map key와 model이 다릅니다")
        if result != authoritative:
            raise GenerationContractError("previous result가 권위 catalog 객체와 다릅니다")
        authoritative_previous_results[model_id] = authoritative
    source_map = {item.source_unique_id: item for item in source_snapshots}
    if len(source_map) != len(source_snapshots) or set(source_map) != set(graph.sources):
        raise GenerationContractError("GenerationPlan source snapshot 집합이 graph와 다릅니다")

    if previous_plan is None:
        inferred_changed_sources = set(graph.sources)
    else:
        previous_sources = previous_plan.source_map
        inferred_changed_sources = {
            source_id
            for source_id in graph.sources
            if source_id not in previous_sources
            or previous_sources[source_id].snapshot_id != source_map[source_id].snapshot_id
        }
    rebuild_roots = set(changed_resources) | inferred_changed_sources
    for model_id in graph.models:
        previous = authoritative_previous_results.get(model_id)
        if previous is None or not _result_compatible_for_reuse(
            previous,
            model_id=model_id,
            deployment_id=graph.deployment_id,
            previous_plan=previous_plan,
        ):
            rebuild_roots.add(model_id)
    rebuild_models = affected_models(graph, rebuild_roots)
    generation_id = f"gen-{generation_sequence:012d}"
    slots: list[BuildSlot] = []
    for model_id in graph.models:
        build_id = content_id(
            "build",
            {
                "generation_id": generation_id,
                "deployment_id": graph.deployment_id,
                "model_unique_id": model_id,
            },
        )
        if model_id in rebuild_models:
            slots.append(BuildSlot(model_id, "REBUILD", build_id, None))
        else:
            slots.append(
                BuildSlot(
                    model_id,
                    "REUSE",
                    build_id,
                    authoritative_previous_results[model_id].result_manifest_id,
                )
            )

    body = {
        "contract_version": CONTRACT_VERSION,
        "generation_id": generation_id,
        "generation_sequence": generation_sequence,
        "deployment_id": graph.deployment_id,
        "graph_digest": graph.graph_digest,
        "graph_dependencies": graph.dependencies,
        "source_snapshots": [source_snapshot_body(item) for item in sorted(source_snapshots, key=lambda x: x.source_unique_id)],
        "build_slots": [asdict(item) for item in slots],
        "required_serving_components": sorted(set(required_serving_components)),
    }
    plan = GenerationPlan(
        **{
            **{key: value for key, value in body.items() if key != "contract_version"},
            "source_snapshots": tuple(sorted(source_snapshots, key=lambda x: x.source_unique_id)),
            "build_slots": tuple(slots),
            "required_serving_components": tuple(sorted(set(required_serving_components))),
            "plan_id": content_id("plan", body),
        }
    )
    return validate_generation_plan(plan)


@dataclass(frozen=True)
class ParentBinding:
    """소비 generation의 parent slot을 실제 불변 source/result에 연결한다."""

    parent_unique_id: str
    binding_kind: Literal["SOURCE", "RESULT", "REUSED_RESULT"]
    manifest_id: str
    origin_generation_id: str | None
    relation_id: str
    content_sha256: str


@dataclass(frozen=True)
class ModelCohortManifest:
    """모델 실행 직전에 exact 직접 부모 vector를 봉인한 manifest."""

    cohort_manifest_id: str
    generation_id: str
    plan_id: str
    deployment_id: str
    model_unique_id: str
    build_id: str
    parent_bindings: tuple[ParentBinding, ...]


@dataclass(frozen=True)
class ManifestCatalog:
    """저장 adapter가 원자적으로 보존할 plan/cohort/result 권위 집합."""

    plans: Mapping[str, GenerationPlan]
    cohorts: Mapping[str, ModelCohortManifest]
    results: Mapping[str, ModelResultManifest]


def empty_manifest_catalog() -> ManifestCatalog:
    return ManifestCatalog(plans={}, cohorts={}, results={})


def _cohort_body(cohort: ModelCohortManifest) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "generation_id": cohort.generation_id,
        "plan_id": cohort.plan_id,
        "deployment_id": cohort.deployment_id,
        "model_unique_id": cohort.model_unique_id,
        "build_id": cohort.build_id,
        "parent_bindings": [asdict(item) for item in cohort.parent_bindings],
    }


def _binding_from_result(
    result: ModelResultManifest,
    *,
    kind: Literal["RESULT", "REUSED_RESULT"],
) -> ParentBinding:
    return ParentBinding(
        parent_unique_id=result.model_unique_id,
        binding_kind=kind,
        manifest_id=result.result_manifest_id,
        origin_generation_id=result.generation_id,
        relation_id=result.relation_id,
        content_sha256=result.content_sha256,
    )


def validate_result_catalog(
    results: Mapping[str, ModelResultManifest],
) -> Mapping[str, ModelResultManifest]:
    """catalog key/내용 ID와 동일 build slot의 상충 result를 전역 검사한다."""
    by_slot: dict[tuple[str, str, str, str, str], str] = {}
    for catalog_key, result in results.items():
        validate_model_result(result)
        if catalog_key != result.result_manifest_id:
            raise GenerationContractError("result catalog key와 내부 content ID가 다릅니다")
        identity = (
            result.plan_id,
            result.generation_id,
            result.deployment_id,
            result.model_unique_id,
            result.build_id,
        )
        previous_id = by_slot.get(identity)
        if previous_id is not None and previous_id != result.result_manifest_id:
            raise GenerationContractError("같은 build slot에 상충 result가 여러 개입니다")
        by_slot[identity] = result.result_manifest_id
    return results


def validate_plan_reuse_bindings(
    *,
    plan: GenerationPlan,
    results: Mapping[str, ModelResultManifest],
) -> None:
    """모든 REUSE slot이 실제 같은 deployment/model result를 가리키는지 검사한다."""
    validate_generation_plan(plan)
    validate_result_catalog(results)
    for slot in plan.build_slots:
        if slot.mode != "REUSE":
            continue
        result = results.get(str(slot.reuse_result_manifest_id))
        if (
            result is None
            or result.model_unique_id != slot.model_unique_id
            or result.deployment_id != plan.deployment_id
        ):
            raise GenerationContractError("plan REUSE slot의 result binding이 올바르지 않습니다")


def validate_model_cohort(
    *,
    plan: GenerationPlan,
    cohort: ModelCohortManifest,
    results: Mapping[str, ModelResultManifest],
) -> ModelCohortManifest:
    """cohort ID뿐 아니라 plan의 정확한 직접 부모와 실제 result를 대사한다."""
    validate_generation_plan(plan)
    validate_result_catalog(results)
    slot = plan.slot_map.get(cohort.model_unique_id)
    if (
        slot is None
        or slot.mode != "REBUILD"
        or cohort.plan_id != plan.plan_id
        or cohort.generation_id != plan.generation_id
        or cohort.deployment_id != plan.deployment_id
        or cohort.build_id != slot.build_id
    ):
        raise GenerationContractError("cohort가 plan의 REBUILD slot과 다릅니다")
    bindings = {item.parent_unique_id: item for item in cohort.parent_bindings}
    if len(bindings) != len(cohort.parent_bindings):
        raise GenerationContractError("cohort parent binding이 중복됐습니다")
    expected_parents = set(plan.dependency_map[cohort.model_unique_id])
    if set(bindings) != expected_parents:
        raise GenerationContractError("cohort parent 집합이 manifest 직접 부모와 다릅니다")
    for parent_id in sorted(expected_parents):
        binding = bindings[parent_id]
        _require_sha256(binding.content_sha256, "parent binding content_sha256")
        if not binding.relation_id or not binding.manifest_id:
            raise GenerationContractError("cohort parent binding 필수값이 비었습니다")
        if parent_id.startswith("source."):
            source = plan.source_map[parent_id]
            expected = ParentBinding(
                parent_unique_id=parent_id,
                binding_kind="SOURCE",
                manifest_id=source.snapshot_id,
                origin_generation_id=None,
                relation_id=source.relation_id or source.manifest_key,
                content_sha256=source.content_sha256,
            )
        else:
            parent_slot = plan.slot_map[parent_id]
            if parent_slot.mode == "REUSE":
                result_id = str(parent_slot.reuse_result_manifest_id)
                kind: Literal["RESULT", "REUSED_RESULT"] = "REUSED_RESULT"
            else:
                candidates = [
                    item
                    for item in results.values()
                    if item.plan_id == plan.plan_id
                    and item.generation_id == plan.generation_id
                    and item.deployment_id == plan.deployment_id
                    and item.model_unique_id == parent_id
                    and item.build_id == parent_slot.build_id
                ]
                if len(candidates) != 1:
                    raise GenerationContractError("cohort의 REBUILD 부모 result가 유일하지 않습니다")
                result_id = candidates[0].result_manifest_id
                kind = "RESULT"
            result = results.get(result_id)
            if result is None:
                raise GenerationContractError("cohort parent result가 catalog에 없습니다")
            expected = _binding_from_result(result, kind=kind)
        if binding != expected:
            raise GenerationContractError("cohort parent binding이 권위 snapshot/result와 다릅니다")
    if cohort.cohort_manifest_id != content_id("cohort", _cohort_body(cohort)):
        raise GenerationContractError("ModelCohortManifest content ID가 다릅니다")
    return cohort


def validate_result_provenance(
    result_manifest_id: str,
    *,
    catalog: ManifestCatalog,
    _visiting: set[str] | None = None,
    _validated: set[str] | None = None,
) -> ModelResultManifest:
    """result의 원래 plan/cohort와 모든 result 부모 provenance를 재귀 검증한다."""
    validate_result_catalog(catalog.results)
    result = catalog.results.get(result_manifest_id)
    if result is None:
        raise GenerationContractError("result manifest가 권위 catalog에 없습니다")
    visiting = _visiting if _visiting is not None else set()
    validated = _validated if _validated is not None else set()
    if result_manifest_id in validated:
        return result
    if result_manifest_id in visiting:
        raise GenerationContractError("result provenance에 cycle이 있습니다")
    visiting.add(result_manifest_id)

    plan = catalog.plans.get(result.plan_id)
    if plan is None or plan.plan_id != result.plan_id:
        raise GenerationContractError("result의 원래 GenerationPlan이 catalog에 없습니다")
    validate_generation_plan(plan)
    cohort = catalog.cohorts.get(result.cohort_manifest_id)
    if cohort is None or cohort.cohort_manifest_id != result.cohort_manifest_id:
        raise GenerationContractError("result의 원래 cohort가 catalog에 없습니다")
    validate_model_cohort(plan=plan, cohort=cohort, results=catalog.results)
    slot = plan.slot_map.get(result.model_unique_id)
    if (
        slot is None
        or slot.mode != "REBUILD"
        or result.generation_id != plan.generation_id
        or result.deployment_id != plan.deployment_id
        or result.build_id != slot.build_id
        or cohort.model_unique_id != result.model_unique_id
        or cohort.build_id != result.build_id
    ):
        raise GenerationContractError("result identity가 원래 plan/cohort와 다릅니다")
    expected_parent_ids = tuple(
        binding.manifest_id for binding in cohort.parent_bindings
    )
    if result.parent_binding_ids != expected_parent_ids:
        raise GenerationContractError("result parent vector가 원래 cohort와 다릅니다")
    for binding in cohort.parent_bindings:
        if binding.binding_kind in {"RESULT", "REUSED_RESULT"}:
            validate_result_provenance(
                binding.manifest_id,
                catalog=catalog,
                _visiting=visiting,
                _validated=validated,
            )
    visiting.remove(result_manifest_id)
    validated.add(result_manifest_id)
    return result


def register_plan(catalog: ManifestCatalog, plan: GenerationPlan) -> ManifestCatalog:
    """같은 ID의 다른 plan을 거부하고 불변 catalog 사본을 반환한다."""
    validate_manifest_catalog(catalog)
    validate_generation_plan(plan)
    previous = catalog.plans.get(plan.plan_id)
    if previous is not None and previous != plan:
        raise GenerationContractError("같은 plan ID에 다른 GenerationPlan이 있습니다")
    return ManifestCatalog(
        plans={**catalog.plans, plan.plan_id: plan},
        cohorts=dict(catalog.cohorts),
        results=dict(catalog.results),
    )


def register_cohort(
    catalog: ManifestCatalog,
    cohort: ModelCohortManifest,
) -> ManifestCatalog:
    """plan과 부모가 권위 catalog에 있는 cohort만 불변 등록한다."""
    validate_manifest_catalog(catalog)
    plan = catalog.plans.get(cohort.plan_id)
    if plan is None:
        raise GenerationContractError("cohort GenerationPlan이 catalog에 없습니다")
    validate_model_cohort(plan=plan, cohort=cohort, results=catalog.results)
    previous = catalog.cohorts.get(cohort.cohort_manifest_id)
    if previous is not None and previous != cohort:
        raise GenerationContractError("같은 cohort ID에 다른 manifest가 있습니다")
    return ManifestCatalog(
        plans=dict(catalog.plans),
        cohorts={**catalog.cohorts, cohort.cohort_manifest_id: cohort},
        results=dict(catalog.results),
    )


def register_result(
    catalog: ManifestCatalog,
    result: ModelResultManifest,
) -> ManifestCatalog:
    """원래 plan/cohort/부모 provenance가 완전한 result만 불변 등록한다."""
    validate_manifest_catalog(catalog)
    validate_model_result(result)
    previous = catalog.results.get(result.result_manifest_id)
    if previous is not None and previous != result:
        raise GenerationContractError("같은 result ID에 다른 manifest가 있습니다")
    updated = ManifestCatalog(
        plans=dict(catalog.plans),
        cohorts=dict(catalog.cohorts),
        results={**catalog.results, result.result_manifest_id: result},
    )
    validate_result_provenance(result.result_manifest_id, catalog=updated)
    return updated


def validate_manifest_catalog(catalog: ManifestCatalog) -> ManifestCatalog:
    """durable adapter 조회 뒤 전체 manifest graph를 전수 검증한다."""
    for key, plan in catalog.plans.items():
        if key != plan.plan_id:
            raise GenerationContractError("plan catalog key와 내부 content ID가 다릅니다")
        validate_generation_plan(plan)
    validate_result_catalog(catalog.results)
    for key, cohort in catalog.cohorts.items():
        if key != cohort.cohort_manifest_id:
            raise GenerationContractError("cohort catalog key와 내부 content ID가 다릅니다")
        plan = catalog.plans.get(cohort.plan_id)
        if plan is None:
            raise GenerationContractError("cohort plan이 catalog에 없습니다")
        validate_model_cohort(plan=plan, cohort=cohort, results=catalog.results)
    validated: set[str] = set()
    for result_id in catalog.results:
        validate_result_provenance(
            result_id,
            catalog=catalog,
            _validated=validated,
        )
    return catalog


def _binding_for_model_parent(
    plan: GenerationPlan,
    parent_model_id: str,
    results: Mapping[str, ModelResultManifest],
) -> ParentBinding | None:
    slot = plan.slot_map[parent_model_id]
    if slot.mode == "REUSE":
        result = results.get(str(slot.reuse_result_manifest_id))
        kind: Literal["RESULT", "REUSED_RESULT"] = "REUSED_RESULT"
    else:
        matches = [
            result
            for result in results.values()
            if result.build_id == slot.build_id
            and result.model_unique_id == parent_model_id
            and result.generation_id == plan.generation_id
            and result.plan_id == plan.plan_id
        ]
        if len(matches) > 1:
            raise GenerationContractError("같은 build slot에 결과 manifest가 여러 개입니다")
        result = matches[0] if matches else None
        kind = "RESULT"
    if result is None:
        return None
    # 현재 plan의 REUSE slot이 exact result ID를 고정하며, 그 result가 이전
    # plan slot에 속하는지는 generation 생성 시 검증한다.
    if result.model_unique_id != parent_model_id or result.deployment_id != plan.deployment_id:
        raise GenerationContractError("parent result가 plan deployment/model과 다릅니다")
    return ParentBinding(
        parent_unique_id=parent_model_id,
        binding_kind=kind,
        manifest_id=result.result_manifest_id,
        origin_generation_id=result.generation_id,
        relation_id=result.relation_id,
        content_sha256=result.content_sha256,
    )


def create_model_cohort(
    *,
    plan: GenerationPlan,
    model_unique_id: str,
    catalog: ManifestCatalog,
) -> ModelCohortManifest | None:
    """모든 직접 부모가 준비됐을 때만 exact cohort를 반환한다."""
    validate_manifest_catalog(catalog)
    if catalog.plans.get(plan.plan_id) != plan:
        raise GenerationContractError("GenerationPlan이 권위 catalog에 등록되지 않았습니다")
    results = catalog.results
    slot = plan.slot_map.get(model_unique_id)
    if slot is None:
        raise GenerationContractError("model이 GenerationPlan에 없습니다")
    if slot.mode != "REBUILD":
        raise GenerationContractError("REUSE 모델은 새 실행 cohort를 만들지 않습니다")

    bindings: list[ParentBinding] = []
    for parent in plan.dependency_map[model_unique_id]:
        if parent.startswith("source."):
            source = plan.source_map.get(parent)
            if source is None:
                return None
            bindings.append(
                ParentBinding(
                    parent_unique_id=parent,
                    binding_kind="SOURCE",
                    manifest_id=source.snapshot_id,
                    origin_generation_id=None,
                    relation_id=source.relation_id or source.manifest_key,
                    content_sha256=source.content_sha256,
                )
            )
        else:
            binding = _binding_for_model_parent(plan, parent, results)
            if binding is None:
                return None
            bindings.append(binding)
    bindings.sort(key=lambda item: item.parent_unique_id)
    body = {
        "contract_version": CONTRACT_VERSION,
        "generation_id": plan.generation_id,
        "plan_id": plan.plan_id,
        "deployment_id": plan.deployment_id,
        "model_unique_id": model_unique_id,
        "build_id": slot.build_id,
        "parent_bindings": [asdict(item) for item in bindings],
    }
    cohort = ModelCohortManifest(
        **{
            **{key: value for key, value in body.items() if key != "contract_version"},
            "parent_bindings": tuple(bindings),
            "cohort_manifest_id": content_id("cohort", body),
        }
    )
    return validate_model_cohort(plan=plan, cohort=cohort, results=results)


def create_model_result(
    *,
    plan: GenerationPlan,
    cohort: ModelCohortManifest,
    relation_id: str,
    row_count: int,
    content_sha256: str,
    catalog: ManifestCatalog,
) -> ModelResultManifest:
    """성공·test·digest 검증 뒤 모델의 immutable result를 봉인한다."""
    validate_manifest_catalog(catalog)
    if catalog.plans.get(plan.plan_id) != plan:
        raise GenerationContractError("result GenerationPlan이 catalog에 없습니다")
    if catalog.cohorts.get(cohort.cohort_manifest_id) != cohort:
        raise GenerationContractError("result cohort가 catalog에 등록되지 않았습니다")
    validate_model_cohort(plan=plan, cohort=cohort, results=catalog.results)
    _require_sha256(content_sha256, "model result content_sha256")
    if cohort.plan_id != plan.plan_id or cohort.generation_id != plan.generation_id:
        raise GenerationContractError("cohort가 GenerationPlan과 다릅니다")
    slot = plan.slot_map.get(cohort.model_unique_id)
    if (
        slot is None
        or slot.mode != "REBUILD"
        or slot.build_id != cohort.build_id
        or cohort.deployment_id != plan.deployment_id
    ):
        raise GenerationContractError("cohort가 plan의 REBUILD slot과 다릅니다")
    if row_count < 0 or not relation_id:
        raise GenerationContractError("result relation/row_count가 올바르지 않습니다")
    body = {
        "contract_version": CONTRACT_VERSION,
        "generation_id": plan.generation_id,
        "plan_id": plan.plan_id,
        "deployment_id": plan.deployment_id,
        "model_unique_id": cohort.model_unique_id,
        "build_id": cohort.build_id,
        "cohort_manifest_id": cohort.cohort_manifest_id,
        "relation_id": relation_id,
        "row_count": row_count,
        "content_sha256": content_sha256,
        "parent_binding_ids": [item.manifest_id for item in cohort.parent_bindings],
    }
    result = ModelResultManifest(
        **{
            **{key: value for key, value in body.items() if key != "contract_version"},
            "parent_binding_ids": tuple(body["parent_binding_ids"]),
            "result_manifest_id": content_id("result", body),
        }
    )
    return validate_model_result(result)


def scan_ready_cohorts(
    *,
    plan: GenerationPlan,
    catalog: ManifestCatalog,
    existing_cohort_ids: Iterable[str] = (),
) -> tuple[ModelCohortManifest, ...]:
    """durable ledger 재스캔 시 지금 실행 가능한 미발행 cohort 0..N개를 찾는다."""
    validate_manifest_catalog(catalog)
    if catalog.plans.get(plan.plan_id) != plan:
        raise GenerationContractError("scan GenerationPlan이 catalog에 없습니다")
    results = catalog.results
    validate_plan_reuse_bindings(plan=plan, results=results)
    existing = set(existing_cohort_ids)
    ready: list[ModelCohortManifest] = []
    for slot in plan.build_slots:
        if slot.mode != "REBUILD":
            continue
        same_build = [item for item in results.values() if item.build_id == slot.build_id]
        invalid = [
            item
            for item in same_build
            if not (
                item.plan_id == plan.plan_id
                and item.generation_id == plan.generation_id
                and item.deployment_id == plan.deployment_id
                and item.model_unique_id == slot.model_unique_id
            )
        ]
        if invalid:
            raise GenerationContractError("build ID가 같지만 plan/generation/model이 다른 result입니다")
        if len(same_build) > 1:
            raise GenerationContractError("같은 plan build slot에 result가 여러 개입니다")
        if same_build:
            continue
        cohort = create_model_cohort(
            plan=plan,
            model_unique_id=slot.model_unique_id,
            catalog=catalog,
        )
        if cohort is not None and cohort.cohort_manifest_id not in existing:
            ready.append(cohort)
    return tuple(sorted(ready, key=lambda item: item.model_unique_id))


@dataclass(frozen=True)
class CohortReadyEventV2:
    """모델 DAG 하나를 깨우는 generation-partitioned cohort event."""

    contract_version: int
    partition_key: str
    generation_id: str
    plan_id: str
    deployment_id: str
    model_unique_id: str
    build_id: str
    cohort_manifest_id: str


def build_cohort_ready_event(
    *,
    plan: GenerationPlan,
    cohort: ModelCohortManifest,
    catalog: ManifestCatalog,
) -> CohortReadyEventV2:
    """권위 catalog에 등록된 REBUILD cohort만 실행 준비 event로 만든다."""
    validate_manifest_catalog(catalog)
    if catalog.plans.get(plan.plan_id) != plan:
        raise GenerationContractError("COHORT_READY GenerationPlan이 catalog에 없습니다")
    if catalog.cohorts.get(cohort.cohort_manifest_id) != cohort:
        raise GenerationContractError("COHORT_READY cohort가 권위 catalog와 다릅니다")
    validate_model_cohort(plan=plan, cohort=cohort, results=catalog.results)
    slot = plan.slot_map.get(cohort.model_unique_id)
    if slot is None or slot.mode != "REBUILD" or slot.build_id != cohort.build_id:
        raise GenerationContractError("COHORT_READY는 plan의 REBUILD slot이어야 합니다")
    return CohortReadyEventV2(
        contract_version=CONTRACT_VERSION,
        partition_key=plan.generation_id,
        generation_id=plan.generation_id,
        plan_id=plan.plan_id,
        deployment_id=plan.deployment_id,
        model_unique_id=cohort.model_unique_id,
        build_id=cohort.build_id,
        cohort_manifest_id=cohort.cohort_manifest_id,
    )


def validate_cohort_ready_event(
    event: Mapping[str, Any],
    *,
    actual_partition_key: str,
    expected_model_unique_id: str,
) -> CohortReadyEventV2:
    """실제 Asset partition과 고정 model DAG identity를 함께 검증한다."""
    required = {field.name for field in CohortReadyEventV2.__dataclass_fields__.values()}
    if set(event) != required:
        raise GenerationContractError("cohort READY event 필드 집합이 정확하지 않습니다")
    ready = CohortReadyEventV2(**{name: event[name] for name in required})
    if ready.contract_version != CONTRACT_VERSION:
        raise GenerationContractError("지원하지 않는 cohort Asset 계약 버전입니다")
    if not GENERATION_PATTERN.fullmatch(ready.generation_id):
        raise GenerationContractError("generation_id 형식이 올바르지 않습니다")
    if actual_partition_key != ready.partition_key or ready.partition_key != ready.generation_id:
        raise GenerationContractError("실제 Asset partition key와 cohort generation이 다릅니다")
    if ready.model_unique_id != expected_model_unique_id:
        raise GenerationContractError("다른 model의 cohort READY event입니다")
    for value, prefix in (
        (ready.plan_id, "plan-"),
        (ready.build_id, "build-"),
        (ready.cohort_manifest_id, "cohort-"),
    ):
        if not value.startswith(prefix):
            raise GenerationContractError("cohort READY content ID 형식이 올바르지 않습니다")
    _require_sha256(ready.deployment_id, "deployment_id")
    return ready


@dataclass(frozen=True)
class ModelReadyEventV2:
    """Airflow Asset에 넣는 작고 검증 가능한 generation-partitioned event."""

    contract_version: int
    partition_key: str
    generation_id: str
    plan_id: str
    deployment_id: str
    model_unique_id: str
    result_manifest_id: str
    origin_generation_id: str
    relation_id: str
    row_count: int
    content_sha256: str


def build_model_ready_event(
    *,
    plan: GenerationPlan,
    model_unique_id: str,
    result: ModelResultManifest,
    catalog: ManifestCatalog,
) -> ModelReadyEventV2:
    """REBUILD/REUSE 모두 소비 generation partition으로 READY를 표현한다."""
    validate_manifest_catalog(catalog)
    if catalog.plans.get(plan.plan_id) != plan:
        raise GenerationContractError("READY GenerationPlan이 catalog에 없습니다")
    validate_result_provenance(result.result_manifest_id, catalog=catalog)
    if catalog.results.get(result.result_manifest_id) != result:
        raise GenerationContractError("READY result가 권위 catalog와 다릅니다")
    slot = plan.slot_map.get(model_unique_id)
    if slot is None or result.model_unique_id != model_unique_id:
        raise GenerationContractError("READY result가 plan model slot과 다릅니다")
    if slot.mode == "REUSE":
        if (
            result.result_manifest_id != slot.reuse_result_manifest_id
            or result.deployment_id != plan.deployment_id
        ):
            raise GenerationContractError("READY result가 plan reuse binding과 다릅니다")
    elif (
        result.build_id != slot.build_id
        or result.generation_id != plan.generation_id
        or result.plan_id != plan.plan_id
        or result.deployment_id != plan.deployment_id
    ):
        raise GenerationContractError("READY result가 plan REBUILD slot과 다릅니다")
    return ModelReadyEventV2(
        contract_version=CONTRACT_VERSION,
        partition_key=plan.generation_id,
        generation_id=plan.generation_id,
        plan_id=plan.plan_id,
        deployment_id=plan.deployment_id,
        model_unique_id=model_unique_id,
        result_manifest_id=result.result_manifest_id,
        origin_generation_id=result.generation_id,
        relation_id=result.relation_id,
        row_count=result.row_count,
        content_sha256=result.content_sha256,
    )


def validate_model_ready_event(
    event: Mapping[str, Any],
    *,
    actual_partition_key: str,
    expected_model_unique_id: str,
) -> ModelReadyEventV2:
    """extra의 generation 문자열이 아닌 실제 Asset partition key까지 대사한다."""
    required = {field.name for field in ModelReadyEventV2.__dataclass_fields__.values()}
    if not required.issubset(event):
        raise GenerationContractError("model READY event 필수 lineage가 없습니다")
    ready = ModelReadyEventV2(**{name: event[name] for name in required})
    if ready.contract_version != CONTRACT_VERSION:
        raise GenerationContractError("지원하지 않는 model Asset 계약 버전입니다")
    if not GENERATION_PATTERN.fullmatch(ready.generation_id):
        raise GenerationContractError("generation_id 형식이 올바르지 않습니다")
    if actual_partition_key != ready.partition_key or ready.partition_key != ready.generation_id:
        raise GenerationContractError("실제 Asset partition key와 generation이 다릅니다")
    if ready.model_unique_id != expected_model_unique_id:
        raise GenerationContractError("예상하지 않은 model READY event입니다")
    _require_sha256(ready.content_sha256, "READY content_sha256")
    return ready
