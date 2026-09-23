"""PostgreSQL 기반 모델 generation control store.

Airflow Asset은 깨우기 신호이고 이 저장소가 plan, claim, cohort, result와 전달 작업의
권위 원장이다. 모든 공개 쓰기는 content body replay와 current fencing token을 검사한다.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from psycopg.types.json import Jsonb
from psycopg import IsolationLevel
from psycopg.pq import TransactionStatus

from pipelines.orchestration.dbt_model_registry import (
    ModelDagRegistry,
    OwnerKind,
    registry_from_validated_deployment,
)
from pipelines.orchestration.dbt_deployment import DbtDeployment
from pipelines.orchestration.dbt_relation_bindings import build_relation_vars
from pipelines.orchestration.dbt_invocation_contract import (
    DbtInvocationArtifact,
    DbtInvocationContract,
    DbtInvocationContractError,
    contract_from_json,
    create_dbt_invocation_contract,
)

from pipelines.orchestration.model_generation_contract import (
    BuildSlot,
    GenerationPlan,
    GateReceipt,
    ManifestCatalog,
    ModelCohortManifest,
    ModelResultManifest,
    ParentBinding,
    SourceSnapshot,
    source_snapshot_body,
    build_cohort_ready_event,
    build_model_ready_event,
    canonical_json,
    content_id,
    create_model_cohort,
    empty_manifest_catalog,
    register_cohort,
    register_plan,
    register_result,
    validate_manifest_catalog,
    validate_plan_reuse_bindings,
    validate_cohort_ready_event,
    validate_gate_receipt,
)


class ControlStoreConflict(RuntimeError):
    """동일 identity에 다른 본문 또는 stale attempt가 접근했을 때 발생한다."""


@dataclass(frozen=True)
class BuildClaim:
    plan_id: str
    model_unique_id: str
    build_id: str
    attempt_no: int
    fence_token: int
    claim_owner: str
    lease_expires_at: datetime
    relation_id: str


@dataclass(frozen=True)
class DbtInvocationReservation:
    orchestration_invocation_id: str
    invocation_kind: str
    state: str
    contract_sha256: str
    artifact_path: str | None
    dbt_native_invocation_id: str | None


@dataclass(frozen=True)
class ExecutionReceipt:
    build_id: str
    attempt_no: int
    fence_token: int
    cohort_manifest_id: str
    deployment_id: str
    parent_vector_sha256: str
    claim_owner: str
    relation_id: str
    row_count: int
    content_sha256: str
    model_invocation_id: str
    model_unique_id: str
    model_run_results_sha256: str
    test_invocation_id: str | None
    test_run_results_sha256: str | None
    executed_test_ids: tuple[str, ...]
    tests_sha256: str
    relation_validation_query_id: str
    status: str


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    aggregate_kind: str
    aggregate_id: str
    asset_uri: str
    partition_key: str
    event_body: Mapping[str, Any]


@dataclass(frozen=True)
class ReleaseDeliverySpec:
    deployment_id: str
    model_unique_id: str
    asset_uri: str
    recipient_work: tuple[tuple[str, str], ...]

    @property
    def recipient_map(self) -> dict[str, str]:
        return dict(self.recipient_work)


@dataclass(frozen=True)
class ReleaseDeliveryRegistry:
    deployment_id: str
    graph_digest: str
    specs: tuple[ReleaseDeliverySpec, ...]
    registry_digest: str


def create_release_delivery_registry(
    model_registry: ModelDagRegistry,
    *,
    terminal_recipient_work_by_model: Mapping[str, Mapping[str, str]] | None = None,
) -> ReleaseDeliveryRegistry:
    """승인 dbt graph 전체에서 model READY의 direct recipient를 결정적으로 만든다."""
    terminal = terminal_recipient_work_by_model or {}
    unknown = set(terminal) - set(model_registry.graph.models)
    if unknown:
        raise ControlStoreConflict(f"terminal recipient의 model이 graph에 없습니다: {sorted(unknown)}")
    spec_map = {item.model_unique_id: item for item in model_registry.models}
    if len(spec_map) != len(model_registry.models) or set(spec_map) != set(
        model_registry.graph.models
    ):
        raise ControlStoreConflict("ModelDagRegistry model spec 집합이 graph와 다릅니다")
    for model_id, graph_parents in model_registry.graph.dependencies:
        if spec_map[model_id].direct_parent_unique_ids != graph_parents:
            raise ControlStoreConflict("ModelDagSpec direct parent가 권위 graph와 다릅니다")
    children: dict[str, set[str]] = {model: set() for model in model_registry.graph.models}
    for child, graph_parents in model_registry.graph.dependencies:
        for parent in graph_parents:
            if parent.startswith("model."):
                children[parent].add(child)
    specs = []
    for model_id in model_registry.graph.models:
        recipients = {
            child: "PREPARE_MODEL_COHORT" for child in sorted(children[model_id])
        }
        for consumer, work_kind in terminal.get(model_id, {}).items():
            if not consumer.startswith("publication.") or work_kind != "ASSEMBLE_PUBLICATION":
                raise ControlStoreConflict("terminal consumer/work는 publication 계약이어야 합니다")
            if consumer in recipients:
                raise ControlStoreConflict("delivery recipient가 중복됐습니다")
            recipients[consumer] = work_kind
        specs.append(
            ReleaseDeliverySpec(
                deployment_id=model_registry.deployment_id,
                model_unique_id=model_id,
                asset_uri=f"pop-talk://model/{model_id}/ready",
                recipient_work=tuple(sorted(recipients.items())),
            )
        )
    body = {
        "deployment_id": model_registry.deployment_id,
        "graph_digest": model_registry.graph.graph_digest,
        "specs": [asdict(spec) for spec in specs],
    }
    return ReleaseDeliveryRegistry(
        deployment_id=model_registry.deployment_id,
        graph_digest=model_registry.graph.graph_digest,
        specs=tuple(specs),
        registry_digest=hashlib.sha256(canonical_json(body)).hexdigest(),
    )


@dataclass(frozen=True)
class WorkClaim:
    consumer_id: str
    event_id: str
    work_kind: str
    work_identity: str
    claim_owner: str
    claim_token: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class OutboxClaim:
    event: OutboxEvent
    publisher_id: str
    claim_token: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class ModelExecutionBundle:
    """model DAG가 XCom으로 전달할 claim과 exact dbt 계약 묶음."""

    work_claim: WorkClaim
    build_claim: BuildClaim
    cohort: ModelCohortManifest
    model_contract: DbtInvocationContract
    test_contract: DbtInvocationContract


def _plan_body(plan: GenerationPlan) -> dict[str, Any]:
    body = asdict(plan)
    body["source_snapshots"] = [source_snapshot_body(source) for source in plan.source_snapshots]
    body["contract_version"] = 2
    return json.loads(canonical_json(body))


def _cohort_body(cohort: ModelCohortManifest) -> dict[str, Any]:
    body = asdict(cohort)
    body["contract_version"] = 2
    return json.loads(canonical_json(body))


def _result_body(result: ModelResultManifest) -> dict[str, Any]:
    body = asdict(result)
    body["contract_version"] = 2
    return json.loads(canonical_json(body))


def _gate_body(receipt: GateReceipt) -> dict[str, Any]:
    body = asdict(receipt)
    body["contract_version"] = 2
    return json.loads(canonical_json(body))


def _plan_from_body(raw: Mapping[str, Any]) -> GenerationPlan:
    body = dict(raw)
    body.pop("contract_version", None)
    body["graph_dependencies"] = tuple(
        (str(model), tuple(str(parent) for parent in parents))
        for model, parents in body["graph_dependencies"]
    )
    body["source_snapshots"] = tuple(SourceSnapshot(**item) for item in body["source_snapshots"])
    body["build_slots"] = tuple(BuildSlot(**item) for item in body["build_slots"])
    body["required_serving_components"] = tuple(body["required_serving_components"])
    return GenerationPlan(**body)


def _cohort_from_body(raw: Mapping[str, Any]) -> ModelCohortManifest:
    body = dict(raw)
    body.pop("contract_version", None)
    body["parent_bindings"] = tuple(ParentBinding(**item) for item in body["parent_bindings"])
    return ModelCohortManifest(**body)


def _result_from_body(raw: Mapping[str, Any]) -> ModelResultManifest:
    body = dict(raw)
    body.pop("contract_version", None)
    body["parent_binding_ids"] = tuple(body["parent_binding_ids"])
    return ModelResultManifest(**body)


def parent_vector_sha256(cohort: ModelCohortManifest) -> str:
    """receipt가 증명할 cohort 직접 부모 vector의 결정적 digest다."""
    return hashlib.sha256(canonical_json([asdict(item) for item in cohort.parent_bindings])).hexdigest()


def tests_sha256(test_ids: Iterable[str]) -> str:
    return hashlib.sha256(canonical_json(sorted(test_ids))).hexdigest()


class PostgresControlStore:
    """호출자가 제공한 psycopg connection에서 짧은 원자 작업만 수행한다."""

    def __init__(self, connection: Any):
        self.connection = connection

    @contextmanager
    def _repeatable_read(self):
        """독립 호출은 RR로 시작하고, 중첩 호출은 이미 RR인 상위 transaction만 받는다."""
        is_idle = self.connection.info.transaction_status == TransactionStatus.IDLE
        previous = self.connection.isolation_level
        if is_idle:
            self.connection.isolation_level = IsolationLevel.REPEATABLE_READ
        elif previous != IsolationLevel.REPEATABLE_READ:
            raise ControlStoreConflict("상위 transaction은 REPEATABLE READ여야 합니다")
        try:
            with self.connection.transaction():
                yield
        finally:
            if is_idle:
                self.connection.isolation_level = previous

    def load_catalog(self, cursor: Any | None = None) -> ManifestCatalog:
        if cursor is None:
            with self._repeatable_read():
                with self.connection.cursor() as current:
                    return self.load_catalog(current)
        current = cursor
        current.execute(
            """SELECT plan_id,generation_id,generation_sequence,deployment_id,graph_digest,body
               FROM dw_control.generation_plans ORDER BY generation_sequence"""
        )
        plans = []
        for row in current.fetchall():
            plan = _plan_from_body(row[5])
            if row[:5] != (
                plan.plan_id, plan.generation_id, plan.generation_sequence,
                plan.deployment_id, plan.graph_digest,
            ):
                raise ControlStoreConflict("generation plan SQL identity와 body가 다릅니다")
            plans.append(plan)
        current.execute(
            """SELECT cohort_manifest_id,plan_id,model_unique_id,build_id,body
               FROM dw_control.model_cohorts ORDER BY created_at,cohort_manifest_id"""
        )
        cohorts = []
        for row in current.fetchall():
            cohort = _cohort_from_body(row[4])
            if row[:4] != (
                cohort.cohort_manifest_id, cohort.plan_id,
                cohort.model_unique_id, cohort.build_id,
            ):
                raise ControlStoreConflict("cohort SQL identity와 body가 다릅니다")
            cohorts.append(cohort)
        current.execute(
            """SELECT result_manifest_id,plan_id,cohort_manifest_id,model_unique_id,build_id,
                      relation_id,row_count,content_sha256,body
               FROM dw_control.model_results ORDER BY created_at,result_manifest_id"""
        )
        results = []
        for row in current.fetchall():
            result = _result_from_body(row[8])
            if row[:8] != (
                result.result_manifest_id, result.plan_id, result.cohort_manifest_id,
                result.model_unique_id, result.build_id, result.relation_id,
                result.row_count, result.content_sha256,
            ):
                raise ControlStoreConflict("result SQL identity와 body가 다릅니다")
            results.append(result)
        catalog = empty_manifest_catalog()
        # 순수 register 순서는 provenance를 보존한다. result는 부모 result가 먼저 오도록 반복한다.
        for plan in plans:
            catalog = register_plan(catalog, plan)
        pending_cohorts = {item.cohort_manifest_id: item for item in cohorts}
        pending_results = {item.result_manifest_id: item for item in results}
        while pending_cohorts or pending_results:
            progressed = False
            for key, cohort in list(pending_cohorts.items()):
                try:
                    catalog = register_cohort(catalog, cohort)
                except Exception:
                    continue
                del pending_cohorts[key]
                progressed = True
            for key, result in list(pending_results.items()):
                try:
                    catalog = register_result(catalog, result)
                except Exception:
                    continue
                del pending_results[key]
                progressed = True
            if not progressed:
                raise ControlStoreConflict("DB manifest graph를 provenance 순서로 복원할 수 없습니다")
        return validate_manifest_catalog(catalog)

    def register_delivery_registry(
        self,
        model_registry: ModelDagRegistry,
        *,
        terminal_recipient_work_by_model: Mapping[str, Mapping[str, str]] | None = None,
    ) -> ReleaseDeliveryRegistry:
        """deployment writer만 승인 graph 전체의 delivery registry를 불변 등록한다."""
        if (
            model_registry.deployment_id != model_registry.graph.deployment_id
            or {item.model_unique_id for item in model_registry.models}
            != set(model_registry.graph.models)
        ):
            raise ControlStoreConflict("ModelDagRegistry graph/model 집합이 다릅니다")
        registry = create_release_delivery_registry(
            model_registry,
            terminal_recipient_work_by_model=terminal_recipient_work_by_model,
        )
        expected_body = {
            "deployment_id": registry.deployment_id,
            "graph_digest": registry.graph_digest,
            "specs": [asdict(spec) for spec in registry.specs],
        }
        if (
            hashlib.sha256(canonical_json(expected_body)).hexdigest()
            != registry.registry_digest
            or {spec.model_unique_id for spec in registry.specs}
            != set(model_registry.graph.models)
            or len(registry.specs) != len(model_registry.graph.models)
            or any(spec.deployment_id != registry.deployment_id for spec in registry.specs)
        ):
            raise ControlStoreConflict("release delivery registry digest/identity가 다릅니다")
        normalized_body = json.loads(canonical_json(expected_body))
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT current_user")
                if cursor.fetchone()[0] == "dw_control_runtime":
                    raise ControlStoreConflict("runtime role은 delivery registry를 등록할 수 없습니다")
                cursor.execute(
                    """INSERT INTO dw_control.release_delivery_registries
                       (deployment_id,graph_digest,registry_digest,body)
                       VALUES (%s,%s,%s,%s) ON CONFLICT (deployment_id) DO NOTHING""",
                    (registry.deployment_id, registry.graph_digest, registry.registry_digest, Jsonb(normalized_body)),
                )
                cursor.execute(
                    """SELECT graph_digest,registry_digest,body
                       FROM dw_control.release_delivery_registries WHERE deployment_id=%s""",
                    (registry.deployment_id,),
                )
                if cursor.fetchone() != (registry.graph_digest, registry.registry_digest, normalized_body):
                    raise ControlStoreConflict("같은 deployment에 다른 delivery registry가 있습니다")
                for spec in registry.specs:
                    if (
                        not spec.model_unique_id.startswith("model.")
                        or not spec.asset_uri
                        or tuple(sorted(set(spec.recipient_work))) != spec.recipient_work
                        or any(not consumer or not work for consumer, work in spec.recipient_work)
                        or len(spec.recipient_map) != len(spec.recipient_work)
                    ):
                        raise ControlStoreConflict("release delivery spec 형식이 올바르지 않습니다")
                    spec_body = {
                        "deployment_id": spec.deployment_id,
                        "model_unique_id": spec.model_unique_id,
                        "asset_uri": spec.asset_uri,
                        "recipient_work": dict(spec.recipient_work),
                    }
                    digest = hashlib.sha256(canonical_json(spec_body)).hexdigest()
                    cursor.execute(
                        """INSERT INTO dw_control.release_delivery_specs
                           (deployment_id,model_unique_id,asset_uri,recipient_work,body_sha256,registry_digest)
                           VALUES (%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (deployment_id,model_unique_id) DO NOTHING""",
                        (
                            spec.deployment_id, spec.model_unique_id, spec.asset_uri,
                            Jsonb(dict(spec.recipient_work)), digest, registry.registry_digest,
                        ),
                    )
                    cursor.execute(
                        """SELECT asset_uri,recipient_work,body_sha256,registry_digest
                           FROM dw_control.release_delivery_specs
                           WHERE deployment_id=%s AND model_unique_id=%s""",
                        (spec.deployment_id, spec.model_unique_id),
                    )
                    if cursor.fetchone() != (
                        spec.asset_uri, dict(spec.recipient_work), digest, registry.registry_digest,
                    ):
                        raise ControlStoreConflict("같은 release/model에 다른 delivery spec이 있습니다")
                cursor.execute(
                    """SELECT model_unique_id FROM dw_control.release_delivery_specs
                       WHERE deployment_id=%s ORDER BY model_unique_id""",
                    (registry.deployment_id,),
                )
                if tuple(row[0] for row in cursor.fetchall()) != tuple(
                    sorted(model_registry.graph.models)
                ):
                    raise ControlStoreConflict("delivery registry spec 집합이 graph 전체와 다릅니다")
        return registry

    def _delivery_spec(self, cursor: Any, *, deployment_id: str, model_unique_id: str) -> ReleaseDeliverySpec:
        cursor.execute(
            """SELECT s.asset_uri,s.recipient_work,s.body_sha256,s.registry_digest,r.body
               FROM dw_control.release_delivery_specs s
               JOIN dw_control.release_delivery_registries r
                 ON r.deployment_id=s.deployment_id AND r.registry_digest=s.registry_digest
               WHERE s.deployment_id=%s AND s.model_unique_id=%s""",
            (deployment_id, model_unique_id),
        )
        row = cursor.fetchone()
        if row is None:
            raise ControlStoreConflict("권위 release delivery spec이 없습니다")
        asset_uri, recipient_work, digest, registry_digest, registry_body = row
        body = {
            "deployment_id": deployment_id,
            "model_unique_id": model_unique_id,
            "asset_uri": asset_uri,
            "recipient_work": recipient_work,
        }
        if hashlib.sha256(canonical_json(body)).hexdigest() != digest.strip():
            raise ControlStoreConflict("release delivery spec digest가 다릅니다")
        if hashlib.sha256(canonical_json(registry_body)).hexdigest() != registry_digest.strip():
            raise ControlStoreConflict("release delivery registry digest가 다릅니다")
        body_specs = {
            item["model_unique_id"]: item
            for item in registry_body.get("specs", [])
            if isinstance(item, Mapping) and "model_unique_id" in item
        }
        expected_spec_body = body_specs.get(model_unique_id)
        if expected_spec_body != {
            "deployment_id": deployment_id,
            "model_unique_id": model_unique_id,
            "asset_uri": asset_uri,
            "recipient_work": [list(item) for item in sorted(recipient_work.items())],
        }:
            raise ControlStoreConflict("delivery spec이 registry 본문 exact 항목과 다릅니다")
        return ReleaseDeliverySpec(
            deployment_id,
            model_unique_id,
            asset_uri,
            tuple(sorted((str(key), str(value)) for key, value in recipient_work.items())),
        )

    def register_plan(self, plan: GenerationPlan) -> None:
        body = _plan_body(plan)
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('dw_control.generation'))")
                catalog = self.load_catalog(cursor)
                register_plan(catalog, plan)
                validate_plan_reuse_bindings(plan=plan, results=catalog.results)
                cursor.execute(
                    "SELECT body FROM dw_control.generation_plans WHERE plan_id = %s",
                    (plan.plan_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[0] != body:
                        raise ControlStoreConflict("같은 plan ID에 다른 본문이 있습니다")
                    return
                cursor.execute(
                    """INSERT INTO dw_control.generation_plans
                       (plan_id,generation_id,generation_sequence,deployment_id,graph_digest,body)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    (
                        plan.plan_id, plan.generation_id, plan.generation_sequence,
                        plan.deployment_id, plan.graph_digest, Jsonb(body),
                    ),
                )
                for slot in plan.build_slots:
                    state = "REUSED" if slot.mode == "REUSE" else "PLANNED"
                    cursor.execute(
                        """INSERT INTO dw_control.model_build_slots
                           (plan_id,model_unique_id,build_id,mode,state,
                            reuse_result_manifest_id,result_manifest_id)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            plan.plan_id, slot.model_unique_id, slot.build_id, slot.mode,
                            state, slot.reuse_result_manifest_id,
                            slot.reuse_result_manifest_id if slot.mode == "REUSE" else None,
                        ),
                    )

    def register_gate_receipt(self, receipt: GateReceipt) -> None:
        """현재 plan과 exact registry 소유권에 맞는 gate 증거만 불변 등록한다."""
        validate_gate_receipt(receipt)
        body = _gate_body(receipt)
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT generation_id,deployment_id,body
                       FROM dw_control.generation_plans WHERE plan_id=%s""",
                    (receipt.plan_id,),
                )
                plan_row = cursor.fetchone()
                if plan_row is None:
                    raise ControlStoreConflict("gate receipt의 GenerationPlan이 없습니다")
                generation_id, deployment_id, plan_body = plan_row
                plan = _plan_from_body(plan_body)
                if (
                    generation_id != receipt.generation_id
                    or deployment_id.strip() != receipt.deployment_id
                    or plan.deployment_id != receipt.deployment_id
                ):
                    raise ControlStoreConflict("gate receipt가 plan identity와 다릅니다")
                if receipt.gate_kind == "SOURCE_GATE":
                    source = plan.source_map.get(receipt.owner_unique_id)
                    if source is None or source.snapshot_id != receipt.source_snapshot_id:
                        raise ControlStoreConflict("source gate가 plan의 exact snapshot과 다릅니다")
                cursor.execute(
                    """SELECT body FROM dw_control.dbt_invocation_registries
                       WHERE deployment_id=%s""",
                    (receipt.deployment_id,),
                )
                registry_row = cursor.fetchone()
                if registry_row is None:
                    raise ControlStoreConflict("gate receipt의 invocation registry가 없습니다")
                expected_test_ids = tuple(
                    sorted(
                        str(item["test_unique_id"])
                        for item in registry_row[0].get("test_ownership", [])
                        if item.get("owner_kind") == receipt.gate_kind
                        and item.get("owner_unique_id") == receipt.owner_unique_id
                    )
                )
                if receipt.executed_test_ids != expected_test_ids or not expected_test_ids:
                    raise ControlStoreConflict("gate receipt test 집합이 registry 소유권과 다릅니다")
                cursor.execute(
                    """INSERT INTO dw_control.generation_gate_receipts
                       (gate_receipt_id,gate_kind,generation_id,plan_id,deployment_id,
                        owner_unique_id,source_snapshot_id,evidence_sha256,body)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (gate_receipt_id) DO NOTHING""",
                    (
                        receipt.gate_receipt_id, receipt.gate_kind,
                        receipt.generation_id, receipt.plan_id, receipt.deployment_id,
                        receipt.owner_unique_id, receipt.source_snapshot_id,
                        receipt.evidence_sha256, Jsonb(body),
                    ),
                )
                cursor.execute(
                    """SELECT gate_kind,generation_id,plan_id,deployment_id,owner_unique_id,
                              source_snapshot_id,evidence_sha256,body
                       FROM dw_control.generation_gate_receipts WHERE gate_receipt_id=%s""",
                    (receipt.gate_receipt_id,),
                )
                existing = cursor.fetchone()
                expected = (
                    receipt.gate_kind, receipt.generation_id, receipt.plan_id,
                    receipt.deployment_id, receipt.owner_unique_id,
                    receipt.source_snapshot_id, receipt.evidence_sha256, body,
                )
                if existing is None or (
                    existing[0], existing[1], existing[2], existing[3].strip(),
                    existing[4], existing[5], existing[6].strip(), existing[7]
                ) != expected:
                    raise ControlStoreConflict("같은 gate receipt ID에 다른 본문이 있습니다")

    def _assert_generation_gates(
        self,
        cursor: Any,
        *,
        plan: GenerationPlan,
        model_unique_id: str,
    ) -> bool:
        """필수 gate가 모두 있으면 True, 아직 도착하지 않았으면 False를 반환한다."""
        cursor.execute(
            """SELECT body FROM dw_control.dbt_invocation_registries
               WHERE deployment_id=%s""",
            (plan.deployment_id,),
        )
        registry_row = cursor.fetchone()
        if registry_row is None:
            raise ControlStoreConflict("승인 invocation registry가 없습니다")
        ownership = registry_row[0].get("test_ownership", [])
        required: list[tuple[str, str, str | None, tuple[str, ...]]] = []
        deployment_owners = sorted(
            {
                str(item["owner_unique_id"])
                for item in ownership
                if item.get("owner_kind") == "DEPLOYMENT_GATE"
            }
        )
        for owner in deployment_owners:
            required.append(
                (
                    "DEPLOYMENT_GATE",
                    owner,
                    None,
                    tuple(
                        sorted(
                            str(item["test_unique_id"])
                            for item in ownership
                            if item.get("owner_kind") == "DEPLOYMENT_GATE"
                            and item.get("owner_unique_id") == owner
                        )
                    ),
                )
            )
        if not deployment_owners:
            raise ControlStoreConflict("deployment gate 소유권이 없습니다")
        for parent in plan.dependency_map[model_unique_id]:
            if not parent.startswith("source."):
                continue
            source = plan.source_map[parent]
            required.append(
                (
                    "SOURCE_GATE",
                    parent,
                    source.snapshot_id,
                    tuple(
                        sorted(
                            str(item["test_unique_id"])
                            for item in ownership
                            if item.get("owner_kind") == "SOURCE_GATE"
                            and item.get("owner_unique_id") == parent
                        )
                    ),
                )
            )
        for gate_kind, owner, snapshot_id, expected_tests in required:
            if not expected_tests:
                raise ControlStoreConflict(f"{owner} gate에 소유 test가 없습니다")
            cursor.execute(
                """SELECT body FROM dw_control.generation_gate_receipts
                   WHERE plan_id=%s AND gate_kind=%s AND owner_unique_id=%s""",
                (plan.plan_id, gate_kind, owner),
            )
            row = cursor.fetchone()
            if row is None:
                return False
            raw = dict(row[0])
            raw.pop("contract_version", None)
            raw["executed_test_ids"] = tuple(raw["executed_test_ids"])
            receipt = validate_gate_receipt(GateReceipt(**raw))
            if (
                receipt.generation_id != plan.generation_id
                or receipt.deployment_id != plan.deployment_id
                or receipt.source_snapshot_id != snapshot_id
                or receipt.executed_test_ids != expected_tests
            ):
                raise ControlStoreConflict("gate receipt가 현재 plan의 exact binding과 다릅니다")
        return True

    def materialize_ready_cohorts(self, *, plan_id: str) -> tuple[OutboxEvent, ...]:
        """현재 ledger를 재스캔해 준비된 direct-parent cohort/outbox를 중복 없이 만든다."""
        with self._repeatable_read():
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"cohort:{plan_id}",))
                catalog = self.load_catalog(cursor)
                plan = catalog.plans.get(plan_id)
                if plan is None:
                    raise ControlStoreConflict("재스캔할 GenerationPlan이 없습니다")
                existing = {
                    cohort.cohort_manifest_id
                    for cohort in catalog.cohorts.values()
                    if cohort.plan_id == plan_id
                }
                candidates = []
                for model_unique_id in plan.dependency_map:
                    slot = plan.slot_map[model_unique_id]
                    if slot.mode != "REBUILD":
                        continue
                    cohort = create_model_cohort(
                        plan=plan,
                        model_unique_id=model_unique_id,
                        catalog=catalog,
                    )
                    if cohort is not None and cohort.cohort_manifest_id not in existing:
                        if self._assert_generation_gates(
                            cursor, plan=plan, model_unique_id=model_unique_id
                        ):
                            candidates.append(cohort)
                events = []
                for cohort in candidates:
                    events.append(self.register_cohort_and_outbox(cohort))
                return tuple(events)

    def register_reuse_outbox(self, *, plan_id: str) -> tuple[OutboxEvent, ...]:
        """REUSE 결과를 현재 generation partition의 MODEL_READY로 재발행한다."""
        with self._repeatable_read():
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"reuse:{plan_id}",))
                catalog = self.load_catalog(cursor)
                plan = catalog.plans.get(plan_id)
                if plan is None:
                    raise ControlStoreConflict("REUSE를 발행할 GenerationPlan이 없습니다")
                events = []
                for slot in plan.build_slots:
                    if slot.mode != "REUSE":
                        continue
                    if not self._assert_generation_gates(
                        cursor, plan=plan, model_unique_id=slot.model_unique_id
                    ):
                        continue
                    result = catalog.results.get(str(slot.reuse_result_manifest_id))
                    if result is None:
                        raise ControlStoreConflict("REUSE 원본 result가 catalog에 없습니다")
                    spec = self._delivery_spec(
                        cursor,
                        deployment_id=plan.deployment_id,
                        model_unique_id=slot.model_unique_id,
                    )
                    ready = build_model_ready_event(
                        plan=plan,
                        model_unique_id=slot.model_unique_id,
                        result=result,
                        catalog=catalog,
                    )
                    event_body = json.loads(canonical_json(asdict(ready)))
                    envelope = {
                        "aggregate_kind": "MODEL_READY",
                        "aggregate_id": result.result_manifest_id,
                        "asset_uri": spec.asset_uri,
                        "partition_key": plan.generation_id,
                        "event_body": event_body,
                    }
                    event = OutboxEvent(
                        content_id("event", envelope), "MODEL_READY",
                        result.result_manifest_id, spec.asset_uri,
                        plan.generation_id, event_body,
                    )
                    recipients = tuple(sorted(spec.recipient_map))
                    initial_state = "PENDING" if recipients else "COMPLETE"
                    cursor.execute(
                        """INSERT INTO dw_control.asset_outbox
                           (event_id,aggregate_kind,aggregate_id,asset_uri,partition_key,
                            event_body,state,completed_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,
                                   CASE WHEN %s='COMPLETE' THEN clock_timestamp() ELSE NULL END)
                           ON CONFLICT (event_id) DO NOTHING""",
                        (
                            event.event_id, event.aggregate_kind, event.aggregate_id,
                            event.asset_uri, event.partition_key, Jsonb(event_body),
                            initial_state, initial_state,
                        ),
                    )
                    cursor.execute(
                        """SELECT aggregate_kind,aggregate_id,asset_uri,partition_key,event_body,state
                           FROM dw_control.asset_outbox WHERE event_id=%s""",
                        (event.event_id,),
                    )
                    existing_event = cursor.fetchone()
                    if existing_event is None or existing_event[:5] != (
                        event.aggregate_kind, event.aggregate_id, event.asset_uri,
                        event.partition_key, event_body,
                    ):
                        raise ControlStoreConflict("REUSE MODEL_READY outbox가 다릅니다")
                    for recipient in recipients:
                        cursor.execute(
                            """INSERT INTO dw_control.asset_deliveries(event_id,consumer_id,state)
                               VALUES (%s,%s,'PENDING')
                               ON CONFLICT (event_id,consumer_id) DO NOTHING""",
                            (event.event_id, recipient),
                        )
                    # 이미 ACK가 끝난 replay는 새 Airflow event를 만들지 않는다. 미전송
                    # PENDING만 현재 dispatcher가 claim하며 SENDING은 기존 publisher/reconciler 몫이다.
                    if existing_event[5] == "PENDING":
                        events.append(event)
                return tuple(events)

    def delivery_consumers(self, *, event_id: str) -> tuple[str, ...]:
        """MODEL_READY event의 승인된 PREPARE_MODEL_COHORT 수신자만 반환한다."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT d.consumer_id
                   FROM dw_control.asset_deliveries d
                   JOIN dw_control.asset_outbox o ON o.event_id=d.event_id
                   JOIN dw_control.model_results r ON r.result_manifest_id=o.aggregate_id
                   JOIN dw_control.generation_plans p ON p.plan_id=r.plan_id
                   JOIN dw_control.release_delivery_specs s
                     ON s.deployment_id=p.deployment_id AND s.model_unique_id=r.model_unique_id
                   WHERE d.event_id=%s AND o.aggregate_kind='MODEL_READY'
                     AND s.recipient_work->>d.consumer_id='PREPARE_MODEL_COHORT'
                   ORDER BY d.consumer_id""",
                (event_id,),
            )
            return tuple(row[0] for row in cursor.fetchall())

    def register_cohort(self, cohort: ModelCohortManifest) -> None:
        body = _cohort_body(cohort)
        with self._repeatable_read():
            with self.connection.cursor() as cursor:
                catalog = self.load_catalog(cursor)
                register_cohort(catalog, cohort)
                cursor.execute(
                    "SELECT body FROM dw_control.model_cohorts WHERE cohort_manifest_id = %s",
                    (cohort.cohort_manifest_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[0] != body:
                        raise ControlStoreConflict("같은 cohort ID에 다른 본문이 있습니다")
                    return
                cursor.execute(
                    """INSERT INTO dw_control.model_cohorts
                       (cohort_manifest_id,plan_id,model_unique_id,build_id,body)
                       VALUES (%s,%s,%s,%s,%s)""",
                    (
                        cohort.cohort_manifest_id, cohort.plan_id,
                        cohort.model_unique_id, cohort.build_id, Jsonb(body),
                    ),
                )

    def register_cohort_and_outbox(self, cohort: ModelCohortManifest) -> OutboxEvent:
        """cohort와 그 model DAG용 COHORT_READY delivery를 원자 등록한다."""
        body = _cohort_body(cohort)
        with self._repeatable_read():
            with self.connection.cursor() as cursor:
                catalog = self.load_catalog(cursor)
                updated_catalog = register_cohort(catalog, cohort)
                plan = updated_catalog.plans[cohort.plan_id]
                ready = build_cohort_ready_event(
                    plan=plan,
                    cohort=cohort,
                    catalog=updated_catalog,
                )
                event_body = json.loads(canonical_json(asdict(ready)))
                asset_uri = f"pop-talk://cohort/{cohort.model_unique_id}/ready"
                envelope = {
                    "aggregate_kind": "COHORT_READY",
                    "aggregate_id": cohort.cohort_manifest_id,
                    "asset_uri": asset_uri,
                    "partition_key": plan.generation_id,
                    "event_body": event_body,
                }
                event = OutboxEvent(
                    content_id("event", envelope),
                    "COHORT_READY",
                    cohort.cohort_manifest_id,
                    asset_uri,
                    plan.generation_id,
                    event_body,
                )
                cursor.execute(
                    "SELECT body FROM dw_control.model_cohorts WHERE cohort_manifest_id=%s",
                    (cohort.cohort_manifest_id,),
                )
                existing = cursor.fetchone()
                if existing is None:
                    cursor.execute(
                        """INSERT INTO dw_control.model_cohorts
                           (cohort_manifest_id,plan_id,model_unique_id,build_id,body)
                           VALUES (%s,%s,%s,%s,%s)""",
                        (
                            cohort.cohort_manifest_id, cohort.plan_id,
                            cohort.model_unique_id, cohort.build_id, Jsonb(body),
                        ),
                    )
                elif existing[0] != body:
                    raise ControlStoreConflict("같은 cohort ID에 다른 본문이 있습니다")
                cursor.execute(
                    """INSERT INTO dw_control.asset_outbox
                       (event_id,aggregate_kind,aggregate_id,asset_uri,partition_key,event_body,state)
                       VALUES (%s,%s,%s,%s,%s,%s,'PENDING')
                       ON CONFLICT (event_id) DO NOTHING""",
                    (
                        event.event_id, event.aggregate_kind, event.aggregate_id,
                        event.asset_uri, event.partition_key, Jsonb(event_body),
                    ),
                )
                cursor.execute(
                    """SELECT aggregate_kind,aggregate_id,asset_uri,partition_key,event_body
                       FROM dw_control.asset_outbox WHERE event_id=%s""",
                    (event.event_id,),
                )
                if cursor.fetchone() != (
                    event.aggregate_kind, event.aggregate_id, event.asset_uri,
                    event.partition_key, event_body,
                ):
                    raise ControlStoreConflict("같은 COHORT_READY event ID에 다른 본문이 있습니다")
                cursor.execute(
                    """INSERT INTO dw_control.asset_deliveries(event_id,consumer_id,state)
                       VALUES (%s,%s,'PENDING') ON CONFLICT (event_id,consumer_id) DO NOTHING""",
                    (event.event_id, cohort.model_unique_id),
                )
                return event

    def acknowledge_cohort_delivery(
        self,
        *,
        event_id: str,
        expected_model_unique_id: str,
        actual_asset_uri: str,
        actual_partition_key: str,
        actual_event_body: Mapping[str, Any],
    ) -> None:
        """COHORT_READY ACK와 EXECUTE_MODEL inbox를 같은 transaction에서 기록한다."""
        body = json.loads(canonical_json(actual_event_body))
        ready = validate_cohort_ready_event(
            body,
            actual_partition_key=actual_partition_key,
            expected_model_unique_id=expected_model_unique_id,
        )
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT o.aggregate_id,o.asset_uri,o.partition_key,o.event_body,d.state,c.body
                       FROM dw_control.asset_outbox o
                       JOIN dw_control.asset_deliveries d ON d.event_id=o.event_id
                       JOIN dw_control.model_cohorts c ON c.cohort_manifest_id=o.aggregate_id
                       WHERE o.event_id=%s AND o.aggregate_kind='COHORT_READY'
                         AND d.consumer_id=%s FOR UPDATE OF d""",
                    (event_id, expected_model_unique_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("model DAG용 COHORT_READY delivery가 없습니다")
                aggregate_id, asset_uri, partition_key, expected_body, _, cohort_body = row
                if (
                    (aggregate_id, asset_uri, partition_key, expected_body)
                    != (
                        ready.cohort_manifest_id,
                        actual_asset_uri,
                        actual_partition_key,
                        body,
                    )
                    or cohort_body.get("model_unique_id") != expected_model_unique_id
                ):
                    raise ControlStoreConflict("COHORT_READY ACK identity/body가 권위 원장과 다릅니다")
                work_identity = content_id(
                    "work",
                    {
                        "consumer_id": expected_model_unique_id,
                        "event_id": event_id,
                        "aggregate_id": aggregate_id,
                        "partition_key": partition_key,
                        "work_kind": "EXECUTE_MODEL",
                    },
                )
                cursor.execute(
                    """INSERT INTO dw_control.asset_inbox
                       (consumer_id,event_id,asset_uri,partition_key,aggregate_id,event_body,
                        work_kind,work_identity,state)
                       VALUES (%s,%s,%s,%s,%s,%s,'EXECUTE_MODEL',%s,'PENDING')
                       ON CONFLICT (consumer_id,event_id) DO NOTHING""",
                    (
                        expected_model_unique_id, event_id, asset_uri, partition_key,
                        aggregate_id, Jsonb(body), work_identity,
                    ),
                )
                cursor.execute(
                    """SELECT asset_uri,partition_key,aggregate_id,event_body,work_kind,work_identity
                       FROM dw_control.asset_inbox WHERE consumer_id=%s AND event_id=%s""",
                    (expected_model_unique_id, event_id),
                )
                if cursor.fetchone() != (
                    asset_uri, partition_key, aggregate_id, body,
                    "EXECUTE_MODEL", work_identity,
                ):
                    raise ControlStoreConflict("같은 cohort inbox ID에 다른 work/body가 있습니다")
                cursor.execute(
                    """UPDATE dw_control.asset_deliveries
                       SET state=CASE WHEN state='COMPLETED' THEN state ELSE 'ACKED' END,
                           acknowledged_at=COALESCE(acknowledged_at,clock_timestamp())
                       WHERE event_id=%s AND consumer_id=%s""",
                    (event_id, expected_model_unique_id),
                )
                cursor.execute(
                    """UPDATE dw_control.asset_outbox SET state='COMPLETE',completed_at=clock_timestamp()
                       WHERE event_id=%s AND NOT EXISTS (
                         SELECT 1 FROM dw_control.asset_deliveries
                         WHERE event_id=%s AND state NOT IN ('ACKED','COMPLETED'))""",
                    (event_id, event_id),
                )

    def register_dbt_invocation_registry(
        self,
        deployment: DbtDeployment,
        *,
        explicit_owner_by_test_name: Mapping[str, tuple[OwnerKind, str]],
    ) -> str:
        """deployment writer가 model별 exact selector/test 집합을 불변 등록한다."""
        model_registry = registry_from_validated_deployment(
            deployment,
            explicit_owner_by_test_name=explicit_owner_by_test_name,
        )
        spec_by_model = {spec.model_unique_id: spec for spec in model_registry.models}
        if (
            len(spec_by_model) != len(model_registry.models)
            or set(spec_by_model) != set(model_registry.graph.models)
        ):
            raise ControlStoreConflict("dbt invocation registry model 집합이 graph와 다릅니다")
        ownership_ids = [item.test_unique_id for item in model_registry.test_ownership]
        if len(ownership_ids) != len(set(ownership_ids)):
            raise ControlStoreConflict("dbt test ownership이 중복됐습니다")
        owned_by_model: dict[str, list[Any]] = {
            model_id: [] for model_id in model_registry.graph.models
        }
        for item in model_registry.test_ownership:
            if item.owner_kind == "MODEL_GATE":
                if item.owner_unique_id not in owned_by_model:
                    raise ControlStoreConflict("dbt test owner model이 graph에 없습니다")
                owned_by_model[item.owner_unique_id].append(item)
        for model_id, spec in spec_by_model.items():
            expected_ids = tuple(sorted(item.test_unique_id for item in owned_by_model[model_id]))
            expected_selectors = tuple(sorted(item.selector for item in owned_by_model[model_id]))
            if (
                spec.direct_parent_unique_ids != model_registry.graph.dependency_map[model_id]
                or spec.owned_test_unique_ids != expected_ids
                or spec.owned_test_selectors != expected_selectors
                or not spec.selector.startswith("fqn:")
            ):
                raise ControlStoreConflict("dbt model invocation spec이 graph/test ownership과 다릅니다")
        models = [
            {
                "model_unique_id": spec.model_unique_id,
                "model_selector": spec.selector,
                "owned_test_unique_ids": list(spec.owned_test_unique_ids),
                "owned_test_selectors": list(spec.owned_test_selectors),
            }
            for spec in model_registry.models
        ]
        body = json.loads(
            canonical_json(
                {
                    "deployment_id": model_registry.deployment_id,
                    "graph_digest": model_registry.graph.graph_digest,
                    "manifest_sha256": deployment.manifest_sha256,
                    "model_version": deployment.model_version,
                    "models": models,
                    "test_ownership": [asdict(item) for item in model_registry.test_ownership],
                }
            )
        )
        registry_digest = hashlib.sha256(canonical_json(body)).hexdigest()
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT current_user")
                if cursor.fetchone()[0] == "dw_control_runtime":
                    raise ControlStoreConflict("runtime role은 dbt invocation registry를 등록할 수 없습니다")
                cursor.execute(
                    """SELECT graph_digest,registry_digest,body
                       FROM dw_control.release_delivery_registries WHERE deployment_id=%s""",
                    (model_registry.deployment_id,),
                )
                release = cursor.fetchone()
                if release is None or release[0].strip() != model_registry.graph.graph_digest:
                    raise ControlStoreConflict("같은 graph의 release registry가 먼저 필요합니다")
                release_digest = release[1].strip()
                cursor.execute(
                    """INSERT INTO dw_control.dbt_invocation_registries
                       (deployment_id,graph_digest,manifest_sha256,model_version,invocation_registry_digest,
                        release_registry_digest,body)
                       VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (deployment_id) DO NOTHING""",
                    (
                        model_registry.deployment_id,
                        model_registry.graph.graph_digest,
                        deployment.manifest_sha256,
                        deployment.model_version,
                        registry_digest,
                        release_digest,
                        Jsonb(body),
                    ),
                )
                cursor.execute(
                    """SELECT graph_digest,manifest_sha256,model_version,invocation_registry_digest,
                              release_registry_digest,body
                       FROM dw_control.dbt_invocation_registries WHERE deployment_id=%s""",
                    (model_registry.deployment_id,),
                )
                if cursor.fetchone() != (
                    model_registry.graph.graph_digest,
                    deployment.manifest_sha256,
                    deployment.model_version,
                    registry_digest,
                    release_digest,
                    body,
                ):
                    raise ControlStoreConflict("같은 deployment에 다른 dbt invocation registry가 있습니다")
                for spec in model_registry.models:
                    spec_body = {
                        "deployment_id": model_registry.deployment_id,
                        "model_unique_id": spec.model_unique_id,
                        "model_selector": spec.selector,
                        "owned_test_unique_ids": list(spec.owned_test_unique_ids),
                        "owned_test_selectors": list(spec.owned_test_selectors),
                    }
                    spec_digest = hashlib.sha256(canonical_json(spec_body)).hexdigest()
                    cursor.execute(
                        """INSERT INTO dw_control.dbt_model_invocation_specs
                           (deployment_id,model_unique_id,model_selector,
                            owned_test_unique_ids,owned_test_selectors,body_sha256,
                            invocation_registry_digest)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (deployment_id,model_unique_id) DO NOTHING""",
                        (
                            model_registry.deployment_id,
                            spec.model_unique_id,
                            spec.selector,
                            Jsonb(list(spec.owned_test_unique_ids)),
                            Jsonb(list(spec.owned_test_selectors)),
                            spec_digest,
                            registry_digest,
                        ),
                    )
                    cursor.execute(
                        """SELECT model_selector,owned_test_unique_ids,owned_test_selectors,
                                  body_sha256,invocation_registry_digest
                           FROM dw_control.dbt_model_invocation_specs
                           WHERE deployment_id=%s AND model_unique_id=%s""",
                        (model_registry.deployment_id, spec.model_unique_id),
                    )
                    if cursor.fetchone() != (
                        spec.selector,
                        list(spec.owned_test_unique_ids),
                        list(spec.owned_test_selectors),
                        spec_digest,
                        registry_digest,
                    ):
                        raise ControlStoreConflict("같은 model에 다른 dbt invocation spec이 있습니다")
                cursor.execute(
                    """SELECT model_unique_id FROM dw_control.dbt_model_invocation_specs
                       WHERE deployment_id=%s ORDER BY model_unique_id""",
                    (model_registry.deployment_id,),
                )
                if tuple(row[0] for row in cursor.fetchall()) != tuple(
                    sorted(model_registry.graph.models)
                ):
                    raise ControlStoreConflict("dbt invocation spec이 deployment model 전체와 다릅니다")
        return registry_digest

    @staticmethod
    def _reservation_from_row(row: tuple[Any, ...]) -> DbtInvocationReservation:
        return DbtInvocationReservation(
            orchestration_invocation_id=row[0],
            invocation_kind=row[1],
            state=row[2],
            contract_sha256=row[3].strip(),
            artifact_path=row[4],
            dbt_native_invocation_id=str(row[5]) if row[5] is not None else None,
        )

    def reserve_dbt_invocation(
        self, contract: DbtInvocationContract
    ) -> tuple[DbtInvocationReservation, bool]:
        """dbt SQL 전에 attempt/kind를 원자 예약한다. bool은 실행 소유권이다."""
        body = contract.body
        digest = contract.contract_sha256
        try:
            if contract_from_json(canonical_json(body).decode("utf-8")) != contract:
                raise ControlStoreConflict("invocation contract self-seal이 다릅니다")
        except DbtInvocationContractError as error:
            raise ControlStoreConflict("invocation contract self-seal이 다릅니다") from error
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT orchestration_invocation_id,invocation_kind,state,
                              contract_sha256,artifact_path,dbt_native_invocation_id,
                              contract_body
                       FROM dw_control.dbt_invocation_reservations
                       WHERE orchestration_invocation_id=%s FOR UPDATE""",
                    (contract.orchestration_invocation_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[6] != body or existing[3].strip() != digest:
                        raise ControlStoreConflict("같은 invocation ID에 다른 contract가 있습니다")
                    return self._reservation_from_row(existing[:6]), False
                cursor.execute(
                    """SELECT s.plan_id,s.model_unique_id,s.current_attempt_no,
                              s.current_fence_token,s.claim_owner,s.claim_expires_at,
                              p.deployment_id,c.cohort_manifest_id,
                              i.model_selector,i.owned_test_unique_ids,i.owned_test_selectors,
                              r.manifest_sha256,r.model_version
                       FROM dw_control.model_build_slots s
                       JOIN dw_control.generation_plans p ON p.plan_id=s.plan_id
                       JOIN dw_control.model_cohorts c ON c.build_id=s.build_id
                       JOIN dw_control.dbt_model_invocation_specs i
                         ON i.deployment_id=p.deployment_id
                        AND i.model_unique_id=s.model_unique_id
                       JOIN dw_control.dbt_invocation_registries r
                         ON r.deployment_id=i.deployment_id
                        AND r.invocation_registry_digest=i.invocation_registry_digest
                       WHERE s.build_id=%s AND s.state='CLAIMED'
                       FOR UPDATE OF s""",
                    (contract.build_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("invocation 대상 current claim/spec이 없습니다")
                (
                    plan_id, model_id, attempt_no, fence, owner, expires,
                    deployment_id, cohort_id, model_selector, test_ids, test_selectors,
                    manifest_sha256, model_version,
                ) = row
                expected_ids = (
                    [model_id]
                    if contract.invocation_kind == "MODEL"
                    else ([] if contract.invocation_kind == "EMPTY_TEST_SET" else test_ids)
                )
                expected_selectors = (
                    [model_selector]
                    if contract.invocation_kind == "MODEL"
                    else ([] if contract.invocation_kind == "EMPTY_TEST_SET" else test_selectors)
                )
                if (
                    expires <= datetime.now(timezone.utc)
                    or (plan_id, cohort_id, deployment_id, model_id, attempt_no, fence, owner)
                    != (
                        contract.plan_id, contract.cohort_manifest_id,
                        contract.deployment_id, contract.model_unique_id,
                        contract.attempt_no, contract.fence_token, contract.claim_owner,
                    )
                    or manifest_sha256.strip() != contract.manifest_sha256
                    or model_version.strip() != contract.model_version
                    or tuple(expected_ids) != contract.expected_unique_ids
                    or tuple(expected_selectors) != contract.expected_selectors
                    or (contract.invocation_kind == "EMPTY_TEST_SET" and test_ids)
                ):
                    raise ControlStoreConflict("invocation contract가 current fenced registry와 다릅니다")
                if contract.relation_vars_json != "{}":
                    # 호출자가 다시 해시한 vars도 원장의 cohort/attempt와 같아야 한다.
                    cursor.execute(
                        """SELECT c.body,a.relation_id FROM dw_control.model_cohorts c
                           JOIN dw_control.model_build_attempts a ON a.build_id=c.build_id
                           WHERE c.cohort_manifest_id=%s AND a.attempt_no=%s
                             AND a.fence_token=%s""",
                        (cohort_id, attempt_no, fence),
                    )
                    binding_row = cursor.fetchone()
                    if binding_row is None or json.loads(contract.relation_vars_json) != build_relation_vars(*binding_row):
                        raise ControlStoreConflict("relation bindings가 current cohort/attempt와 다릅니다")
                if contract.invocation_kind != "MODEL":
                    cursor.execute(
                        """SELECT 1 FROM dw_control.dbt_invocation_artifacts
                           WHERE build_id=%s AND attempt_no=%s AND fence_token=%s
                             AND invocation_kind='MODEL' AND status='SUCCEEDED'""",
                        (contract.build_id, contract.attempt_no, contract.fence_token),
                    )
                    if cursor.fetchone() is None:
                        raise ControlStoreConflict("성공 MODEL 증거 전에 test를 예약할 수 없습니다")
                try:
                    cursor.execute(
                        """INSERT INTO dw_control.dbt_invocation_reservations
                           (orchestration_invocation_id,build_id,attempt_no,fence_token,
                            cohort_manifest_id,plan_id,deployment_id,model_unique_id,
                            invocation_kind,manifest_sha256,model_version,
                            contract_sha256,contract_body,claim_owner,state)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RESERVED')
                           RETURNING orchestration_invocation_id,invocation_kind,state,
                                     contract_sha256,artifact_path,dbt_native_invocation_id""",
                        (
                            contract.orchestration_invocation_id, contract.build_id,
                            contract.attempt_no, contract.fence_token,
                            contract.cohort_manifest_id, contract.plan_id,
                            contract.deployment_id, contract.model_unique_id,
                            contract.invocation_kind, contract.manifest_sha256,
                            contract.model_version, digest, Jsonb(body), contract.claim_owner,
                        ),
                    )
                except Exception as error:
                    if getattr(error, "sqlstate", None) == "23505":
                        raise ControlStoreConflict("같은 attempt/kind가 이미 다른 ID로 예약됐습니다") from error
                    raise
                return self._reservation_from_row(cursor.fetchone()), True

    def set_dbt_invocation_state(
        self,
        contract: DbtInvocationContract,
        *,
        expected_state: str,
        new_state: str,
        artifact_path: str | None = None,
        dbt_native_invocation_id: str | None = None,
        failure_reason: str | None = None,
    ) -> DbtInvocationReservation:
        """reservation 상태를 current fence와 예상 상태 CAS로 전이한다."""
        allowed = {
            ("RESERVED", "RUNNING"),
            ("RESERVED", "FILE_SEALED"),
            ("RUNNING", "FILE_SEALED"),
            ("RUNNING", "FAILED"),
            ("FILE_SEALED", "COMPLETED"),
            ("FILE_SEALED", "FAILED"),
            ("RESERVED", "ABANDONED"),
            ("RUNNING", "ABANDONED"),
        }
        if (expected_state, new_state) not in allowed:
            raise ValueError("허용되지 않은 invocation 상태 전이입니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.dbt_invocation_reservations r
                       SET state=%s,
                           artifact_path=COALESCE(%s,artifact_path),
                           dbt_native_invocation_id=COALESCE(%s::uuid,dbt_native_invocation_id),
                           failure_reason=COALESCE(%s,failure_reason),
                           started_at=CASE WHEN %s='RUNNING' THEN clock_timestamp() ELSE started_at END,
                           completed_at=CASE WHEN %s IN ('COMPLETED','FAILED','ABANDONED')
                                             THEN clock_timestamp() ELSE completed_at END,
                           heartbeat_at=clock_timestamp()
                       FROM dw_control.model_build_slots s
                       WHERE r.orchestration_invocation_id=%s AND r.state=%s
                         AND r.contract_sha256=%s AND r.contract_body=%s
                         AND s.build_id=r.build_id AND s.state='CLAIMED'
                         AND s.current_attempt_no=r.attempt_no
                         AND s.current_fence_token=r.fence_token
                         AND s.claim_owner=r.claim_owner
                         AND s.claim_expires_at > clock_timestamp()
                       RETURNING r.orchestration_invocation_id,r.invocation_kind,r.state,
                                 r.contract_sha256,r.artifact_path,r.dbt_native_invocation_id""",
                    (
                        new_state, artifact_path, dbt_native_invocation_id, failure_reason,
                        new_state, new_state, contract.orchestration_invocation_id,
                        expected_state, contract.contract_sha256, Jsonb(contract.body),
                    ),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("stale fence 또는 다른 상태는 invocation을 전이할 수 없습니다")
                return self._reservation_from_row(row)

    def heartbeat_dbt_invocation(
        self, contract: DbtInvocationContract, *, lease_seconds: int = 300
    ) -> datetime:
        """RUNNING invocation과 build lease를 한 transaction에서 연장한다."""
        claim = BuildClaim(
            contract.plan_id, contract.model_unique_id, contract.build_id,
            contract.attempt_no, contract.fence_token, contract.claim_owner,
            datetime.now(timezone.utc), "unused",
        )
        expires = self.heartbeat(claim, lease_seconds=lease_seconds)
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.dbt_invocation_reservations
                       SET heartbeat_at=clock_timestamp()
                       WHERE orchestration_invocation_id=%s AND state='RUNNING'
                         AND contract_sha256=%s RETURNING heartbeat_at""",
                    (contract.orchestration_invocation_id, contract.contract_sha256),
                )
                if cursor.fetchone() is None:
                    raise ControlStoreConflict("RUNNING invocation만 heartbeat할 수 있습니다")
        return expires

    def current_dbt_invocation_lease_expires_at(
        self, contract: DbtInvocationContract
    ) -> datetime:
        """현재 fenced claim 만료를 독립 transaction에서 읽고 즉시 닫는다."""
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                """SELECT s.claim_expires_at
                   FROM dw_control.dbt_invocation_reservations r
                   JOIN dw_control.model_build_slots s ON s.build_id=r.build_id
                   WHERE r.orchestration_invocation_id=%s
                     AND r.contract_sha256=%s AND r.contract_body=%s
                     AND r.state IN ('RESERVED','RUNNING')
                     AND s.state='CLAIMED'
                     AND s.current_attempt_no=r.attempt_no
                     AND s.current_fence_token=r.fence_token
                     AND s.claim_owner=r.claim_owner""",
                (
                    contract.orchestration_invocation_id,
                    contract.contract_sha256,
                    Jsonb(contract.body),
                ),
            )
                row = cursor.fetchone()
        if row is None:
            raise ControlStoreConflict("current fenced invocation lease를 읽을 수 없습니다")
        return row[0]

    def record_dbt_invocation_artifact(
        self, contract: DbtInvocationContract, artifact: DbtInvocationArtifact
    ) -> None:
        """봉인 파일과 동일한 성공 증거를 immutable ledger에 등록한다."""
        body = artifact.body
        if (
            artifact.orchestration_invocation_id != contract.orchestration_invocation_id
            or artifact.build_id != contract.build_id
            or artifact.attempt_no != contract.attempt_no
            or artifact.fence_token != contract.fence_token
            or artifact.cohort_manifest_id != contract.cohort_manifest_id
            or artifact.deployment_id != contract.deployment_id
            or artifact.model_unique_id != contract.model_unique_id
            or artifact.invocation_kind != contract.invocation_kind
            or artifact.expected_unique_ids != contract.expected_unique_ids
        ):
            raise ControlStoreConflict("artifact identity가 invocation contract와 다릅니다")
        if not all(re.fullmatch(r"[0-9a-f]{64}", value) for value in (
            artifact.argv_sha256,
            artifact.environment_sha256,
        )):
            raise ControlStoreConflict("artifact argv/environment digest 형식이 다릅니다")
        if contract.invocation_kind == "EMPTY_TEST_SET":
            if (
                artifact.status != "EMPTY_TEST_SET"
                or artifact.dbt_native_invocation_id is not None
                or artifact.run_results_sha256 is not None
                or artifact.executed_unique_ids
            ):
                raise ControlStoreConflict("EMPTY_TEST_SET artifact 형식이 다릅니다")
        else:
            try:
                uuid.UUID(str(artifact.dbt_native_invocation_id))
            except ValueError as error:
                raise ControlStoreConflict("artifact native invocation ID가 UUID가 아닙니다") from error
            if (
                artifact.status != "SUCCEEDED"
                or artifact.run_results_sha256 is None
                or not re.fullmatch(r"[0-9a-f]{64}", artifact.run_results_sha256)
                or len(set(artifact.executed_unique_ids)) != len(artifact.executed_unique_ids)
                or set(artifact.executed_unique_ids) != set(contract.expected_unique_ids)
            ):
                raise ControlStoreConflict("artifact 실행 결과가 exact contract와 다릅니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT body FROM dw_control.dbt_invocation_artifacts
                       WHERE orchestration_invocation_id=%s""",
                    (contract.orchestration_invocation_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[0] != body:
                        raise ControlStoreConflict("같은 invocation ID에 다른 artifact가 있습니다")
                    return
                cursor.execute(
                    """SELECT r.state,r.contract_body
                       FROM dw_control.dbt_invocation_reservations r
                       JOIN dw_control.model_build_slots s ON s.build_id=r.build_id
                       WHERE r.orchestration_invocation_id=%s
                         AND s.state='CLAIMED'
                         AND s.current_attempt_no=r.attempt_no
                         AND s.current_fence_token=r.fence_token
                         AND s.claim_owner=r.claim_owner
                         AND s.claim_expires_at > clock_timestamp()
                       FOR UPDATE OF r,s""",
                    (contract.orchestration_invocation_id,),
                )
                reservation = cursor.fetchone()
                if reservation != ("FILE_SEALED", contract.body):
                    raise ControlStoreConflict("FILE_SEALED exact reservation만 등록할 수 있습니다")
                cursor.execute(
                    """INSERT INTO dw_control.dbt_invocation_artifacts
                       (orchestration_invocation_id,dbt_native_invocation_id,build_id,
                        attempt_no,fence_token,cohort_manifest_id,deployment_id,
                        model_unique_id,invocation_kind,expected_unique_ids,
                        executed_unique_ids,run_results_sha256,argv_sha256,
                        environment_sha256,artifact_path,status,body)
                       VALUES (%s,%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        artifact.orchestration_invocation_id,
                        artifact.dbt_native_invocation_id, artifact.build_id,
                        artifact.attempt_no, artifact.fence_token,
                        artifact.cohort_manifest_id, artifact.deployment_id,
                        artifact.model_unique_id, artifact.invocation_kind,
                        Jsonb(list(artifact.expected_unique_ids)),
                        Jsonb(list(artifact.executed_unique_ids)),
                        artifact.run_results_sha256, artifact.argv_sha256,
                        artifact.environment_sha256, artifact.artifact_path,
                        artifact.status, Jsonb(body),
                    ),
                )
                cursor.execute(
                    """UPDATE dw_control.dbt_invocation_reservations
                       SET state='COMPLETED',completed_at=clock_timestamp(),
                           dbt_native_invocation_id=%s::uuid,artifact_path=%s
                       WHERE orchestration_invocation_id=%s AND state='FILE_SEALED'
                       RETURNING orchestration_invocation_id""",
                    (
                        artifact.dbt_native_invocation_id,
                        artifact.artifact_path,
                        artifact.orchestration_invocation_id,
                    ),
                )
                if cursor.fetchone() is None:
                    raise ControlStoreConflict("artifact 완료 상태 전이에 실패했습니다")

    def mark_dbt_artifact_missing(self, contract: DbtInvocationContract) -> None:
        """DB 완료 row는 유지하고 영속 파일 유실만 복구 대상으로 표시한다."""
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.dbt_invocation_reservations r
                       SET failure_reason='ARTIFACT_MISSING'
                       FROM dw_control.dbt_invocation_artifacts a
                       WHERE r.orchestration_invocation_id=%s
                         AND r.orchestration_invocation_id=a.orchestration_invocation_id
                         AND r.state='COMPLETED'
                         AND r.contract_sha256=%s AND r.contract_body=%s
                       RETURNING r.orchestration_invocation_id""",
                    (
                        contract.orchestration_invocation_id,
                        contract.contract_sha256,
                        Jsonb(contract.body),
                    ),
                )
                if cursor.fetchone() is None:
                    raise ControlStoreConflict("완료된 exact artifact만 유실 표시할 수 있습니다")

    def load_dbt_invocation_artifact(
        self, contract: DbtInvocationContract
    ) -> DbtInvocationArtifact | None:
        """immutable SQL 열과 JSON body를 대사해 완료 증거를 읽는다."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT orchestration_invocation_id,dbt_native_invocation_id,build_id,
                          attempt_no,fence_token,cohort_manifest_id,deployment_id,
                          model_unique_id,invocation_kind,expected_unique_ids,
                          executed_unique_ids,run_results_sha256,argv_sha256,
                          environment_sha256,artifact_path,status,body
                   FROM dw_control.dbt_invocation_artifacts
                   WHERE orchestration_invocation_id=%s""",
                (contract.orchestration_invocation_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        artifact = DbtInvocationArtifact(
            orchestration_invocation_id=row[0],
            dbt_native_invocation_id=str(row[1]) if row[1] is not None else None,
            build_id=row[2],
            attempt_no=row[3],
            fence_token=row[4],
            cohort_manifest_id=row[5],
            deployment_id=row[6].strip(),
            model_unique_id=row[7],
            invocation_kind=row[8],
            expected_unique_ids=tuple(row[9]),
            executed_unique_ids=tuple(row[10]),
            run_results_sha256=row[11].strip() if row[11] is not None else None,
            argv_sha256=row[12].strip(),
            environment_sha256=row[13].strip(),
            artifact_path=row[14],
            status=row[15],
        )
        if artifact.body != row[16] or artifact.orchestration_invocation_id != contract.orchestration_invocation_id:
            raise ControlStoreConflict("dbt artifact SQL identity와 body가 다릅니다")
        return artifact

    def claim_build(self, *, build_id: str, claim_owner: str, lease_seconds: int = 300) -> BuildClaim:
        if not claim_owner or lease_seconds < 1:
            raise ValueError("claim owner와 양수 lease가 필요합니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT plan_id,model_unique_id,state,current_attempt_no,
                              current_fence_token,claim_owner,claim_expires_at
                       FROM dw_control.model_build_slots WHERE build_id=%s FOR UPDATE""",
                    (build_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("build slot이 없습니다")
                plan_id, model_id, state, attempt_no, fence, owner, expires = row
                now = datetime.now(timezone.utc)
                if state in {"SUCCEEDED", "REUSED"}:
                    raise ControlStoreConflict("이미 완료된 build slot입니다")
                if state == "CLAIMED" and expires > now:
                    raise ControlStoreConflict("다른 worker의 lease가 아직 유효합니다")
                if state == "CLAIMED":
                    cursor.execute(
                        """UPDATE dw_control.model_build_attempts
                           SET state='EXPIRED', completed_at=clock_timestamp()
                           WHERE build_id=%s AND attempt_no=%s AND fence_token=%s AND state='CLAIMED'""",
                        (build_id, attempt_no, fence),
                    )
                next_attempt = int(attempt_no or 0) + 1
                cursor.execute("SELECT nextval('dw_control.fence_token_seq')")
                next_fence = int(cursor.fetchone()[0])
                safe_model = re.sub(r"[^A-Z0-9_]", "_", model_id.rsplit(".", 1)[-1].upper())[:32]
                relation_id = (
                    f"POP_TALK_DW_DEV.CANDIDATE.{safe_model}__B{build_id[-12:].upper()}"
                    f"__A{next_attempt}__F{next_fence}"
                )
                cursor.execute(
                    """INSERT INTO dw_control.model_build_attempts
                       (build_id,attempt_no,fence_token,claim_owner,state,lease_expires_at,
                        heartbeat_at,relation_id)
                       VALUES (%s,%s,%s,%s,'CLAIMED',clock_timestamp()+(%s * interval '1 second'),
                               clock_timestamp(),%s)
                       RETURNING lease_expires_at""",
                    (build_id, next_attempt, next_fence, claim_owner, lease_seconds, relation_id),
                )
                lease_expires = cursor.fetchone()[0]
                cursor.execute(
                    """UPDATE dw_control.model_build_slots
                       SET state='CLAIMED',current_attempt_no=%s,current_fence_token=%s,
                           claim_owner=%s,claim_expires_at=%s,heartbeat_at=clock_timestamp()
                       WHERE build_id=%s""",
                    (next_attempt, next_fence, claim_owner, lease_expires, build_id),
                )
        return BuildClaim(plan_id, model_id, build_id, next_attempt, next_fence, claim_owner, lease_expires, relation_id)

    def heartbeat(self, claim: BuildClaim, *, lease_seconds: int = 300) -> datetime:
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.model_build_slots
                       SET heartbeat_at=clock_timestamp(),
                           claim_expires_at=clock_timestamp()+(%s * interval '1 second')
                       WHERE build_id=%s AND state='CLAIMED' AND current_attempt_no=%s
                         AND current_fence_token=%s AND claim_owner=%s
                         AND claim_expires_at > clock_timestamp()
                       RETURNING claim_expires_at""",
                    (lease_seconds, claim.build_id, claim.attempt_no, claim.fence_token, claim.claim_owner),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("stale 또는 만료된 claim은 heartbeat할 수 없습니다")
                expires = row[0]
                cursor.execute(
                    """UPDATE dw_control.model_build_attempts
                       SET heartbeat_at=clock_timestamp(),lease_expires_at=%s
                       WHERE build_id=%s AND attempt_no=%s AND fence_token=%s AND state='CLAIMED'""",
                    (expires, claim.build_id, claim.attempt_no, claim.fence_token),
                )
                return expires

    def record_receipt(self, receipt: ExecutionReceipt) -> None:
        body = asdict(receipt)
        body["executed_test_ids"] = list(receipt.executed_test_ids)
        expected_tests_digest = tests_sha256(receipt.executed_test_ids)
        if receipt.tests_sha256 != expected_tests_digest:
            raise ControlStoreConflict("receipt test ID digest가 다릅니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT s.plan_id,s.model_unique_id,s.current_attempt_no,s.current_fence_token,
                              s.claim_owner,s.claim_expires_at,a.relation_id,c.body
                       FROM dw_control.model_build_slots s
                       JOIN dw_control.model_build_attempts a ON a.build_id=s.build_id
                         AND a.attempt_no=s.current_attempt_no
                       JOIN dw_control.model_cohorts c ON c.build_id=s.build_id
                       WHERE s.build_id=%s AND s.state='CLAIMED' FOR UPDATE OF s""",
                    (receipt.build_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("receipt 대상 current claim/cohort가 없습니다")
                _, model_id, attempt_no, fence, owner, expires, relation_id, cohort_body = row
                cohort = _cohort_from_body(cohort_body)
                if (
                    attempt_no != receipt.attempt_no or fence != receipt.fence_token
                    or owner != receipt.claim_owner or expires <= datetime.now(timezone.utc)
                    or relation_id != receipt.relation_id or model_id != receipt.model_unique_id
                    or cohort.cohort_manifest_id != receipt.cohort_manifest_id
                    or cohort.deployment_id != receipt.deployment_id
                    or parent_vector_sha256(cohort) != receipt.parent_vector_sha256
                ):
                    raise ControlStoreConflict("receipt가 current fenced cohort/relation과 다릅니다")
                cursor.execute(
                    "SELECT body FROM dw_control.model_execution_receipts WHERE build_id=%s AND attempt_no=%s",
                    (receipt.build_id, receipt.attempt_no),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[0] != body:
                        raise ControlStoreConflict("같은 attempt에 다른 receipt가 있습니다")
                    return
                cursor.execute(
                    """INSERT INTO dw_control.model_execution_receipts
                       (build_id,attempt_no,fence_token,cohort_manifest_id,deployment_id,
                        parent_vector_sha256,claim_owner,relation_id,row_count,content_sha256,
                        model_invocation_id,model_unique_id,model_run_results_sha256,
                        test_invocation_id,test_run_results_sha256,executed_test_ids,tests_sha256,
                        relation_validation_query_id,status,body)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        receipt.build_id, receipt.attempt_no, receipt.fence_token,
                        receipt.cohort_manifest_id, receipt.deployment_id,
                        receipt.parent_vector_sha256, receipt.claim_owner, receipt.relation_id,
                        receipt.row_count, receipt.content_sha256, receipt.model_invocation_id,
                        receipt.model_unique_id, receipt.model_run_results_sha256,
                        receipt.test_invocation_id, receipt.test_run_results_sha256,
                        Jsonb(list(receipt.executed_test_ids)), receipt.tests_sha256,
                        receipt.relation_validation_query_id, receipt.status, Jsonb(body),
                    ),
                )

    def commit_result(
        self,
        *,
        result: ModelResultManifest,
        claim: BuildClaim,
    ) -> OutboxEvent:
        result_body = _result_body(result)
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                catalog = self.load_catalog(cursor)
                updated_catalog = register_result(catalog, result)
                plan = updated_catalog.plans[result.plan_id]
                spec = self._delivery_spec(
                    cursor,
                    deployment_id=result.deployment_id,
                    model_unique_id=result.model_unique_id,
                )
                ready = build_model_ready_event(
                    plan=plan,
                    model_unique_id=result.model_unique_id,
                    result=result,
                    catalog=updated_catalog,
                )
                event_body = json.loads(canonical_json(asdict(ready)))
                envelope = {
                    "aggregate_kind": "MODEL_READY",
                    "aggregate_id": result.result_manifest_id,
                    "asset_uri": spec.asset_uri,
                    "partition_key": plan.generation_id,
                    "event_body": event_body,
                }
                event = OutboxEvent(
                    event_id=content_id("event", envelope),
                    aggregate_kind="MODEL_READY",
                    aggregate_id=result.result_manifest_id,
                    asset_uri=spec.asset_uri,
                    partition_key=plan.generation_id,
                    event_body=event_body,
                )
                recipients = tuple(sorted(spec.recipient_map))
                cursor.execute(
                    """SELECT r.status,r.relation_id,r.row_count,r.content_sha256,
                              s.current_attempt_no,s.current_fence_token,s.claim_owner,s.claim_expires_at
                       FROM dw_control.model_execution_receipts r
                       JOIN dw_control.model_build_slots s ON s.build_id=r.build_id
                       WHERE r.build_id=%s AND r.attempt_no=%s FOR UPDATE OF s""",
                    (claim.build_id, claim.attempt_no),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("성공 receipt가 없습니다")
                status, relation_id, row_count, digest, attempt_no, fence, owner, expires = row
                if (
                    status not in {"SUCCEEDED", "EMPTY_TEST_SET"}
                    or (relation_id, row_count, digest) != (result.relation_id, result.row_count, result.content_sha256)
                    or (attempt_no, fence, owner) != (claim.attempt_no, claim.fence_token, claim.claim_owner)
                ):
                    raise ControlStoreConflict("result가 current fenced 성공 receipt와 다릅니다")
                cursor.execute(
                    "SELECT body FROM dw_control.model_results WHERE result_manifest_id=%s",
                    (result.result_manifest_id,),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing[0] != result_body:
                        raise ControlStoreConflict("같은 result ID에 다른 본문이 있습니다")
                    cursor.execute(
                        """SELECT state,result_manifest_id FROM dw_control.model_build_slots
                           WHERE build_id=%s""",
                        (claim.build_id,),
                    )
                    if cursor.fetchone() != ("SUCCEEDED", result.result_manifest_id):
                        raise ControlStoreConflict("기존 result와 slot 완료 상태가 다릅니다")
                    cursor.execute(
                        """SELECT aggregate_kind,aggregate_id,asset_uri,partition_key,event_body
                           FROM dw_control.asset_outbox WHERE event_id=%s""",
                        (event.event_id,),
                    )
                    if cursor.fetchone() != (
                        event.aggregate_kind, event.aggregate_id, event.asset_uri,
                        event.partition_key, event_body,
                    ):
                        raise ControlStoreConflict("기존 result의 outbox가 없거나 다릅니다")
                    cursor.execute(
                        "SELECT consumer_id FROM dw_control.asset_deliveries WHERE event_id=%s ORDER BY consumer_id",
                        (event.event_id,),
                    )
                    if tuple(row[0] for row in cursor.fetchall()) != recipients:
                        raise ControlStoreConflict("기존 outbox의 required recipient가 다릅니다")
                    return event
                if expires <= datetime.now(timezone.utc):
                    raise ControlStoreConflict("만료된 claim은 새 result를 commit할 수 없습니다")
                cursor.execute(
                    """INSERT INTO dw_control.model_results
                       (result_manifest_id,plan_id,cohort_manifest_id,model_unique_id,build_id,
                        attempt_no,fence_token,relation_id,row_count,content_sha256,body)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        result.result_manifest_id, result.plan_id, result.cohort_manifest_id,
                        result.model_unique_id, result.build_id, claim.attempt_no,
                        claim.fence_token, result.relation_id, result.row_count,
                        result.content_sha256, Jsonb(result_body),
                    ),
                )
                cursor.execute(
                    """UPDATE dw_control.model_build_slots SET state='SUCCEEDED',result_manifest_id=%s
                       WHERE build_id=%s AND state='CLAIMED' AND current_attempt_no=%s
                         AND current_fence_token=%s AND claim_owner=%s
                       RETURNING build_id""",
                    (result.result_manifest_id, claim.build_id, claim.attempt_no, claim.fence_token, claim.claim_owner),
                )
                if cursor.fetchone() is None:
                    raise ControlStoreConflict("stale worker는 result를 commit할 수 없습니다")
                cursor.execute(
                    """UPDATE dw_control.model_build_attempts SET state='SUCCEEDED',completed_at=clock_timestamp()
                       WHERE build_id=%s AND attempt_no=%s AND fence_token=%s AND state='CLAIMED'""",
                    (claim.build_id, claim.attempt_no, claim.fence_token),
                )
                initial_state = "PENDING" if recipients else "COMPLETE"
                cursor.execute(
                    """INSERT INTO dw_control.asset_outbox
                       (event_id,aggregate_kind,aggregate_id,asset_uri,partition_key,event_body,state,completed_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,
                               CASE WHEN %s='COMPLETE' THEN clock_timestamp() ELSE NULL END)
                       ON CONFLICT (event_id) DO NOTHING""",
                    (
                        event.event_id, event.aggregate_kind, event.aggregate_id, event.asset_uri,
                        event.partition_key, Jsonb(event_body), initial_state, initial_state,
                    ),
                )
                cursor.execute(
                    """SELECT aggregate_kind,aggregate_id,asset_uri,partition_key,event_body
                       FROM dw_control.asset_outbox WHERE event_id=%s""",
                    (event.event_id,),
                )
                if cursor.fetchone() != (
                    event.aggregate_kind, event.aggregate_id, event.asset_uri,
                    event.partition_key, event_body,
                ):
                    raise ControlStoreConflict("같은 event ID에 다른 본문이 있습니다")
                for recipient in recipients:
                    cursor.execute(
                        """INSERT INTO dw_control.asset_deliveries(event_id,consumer_id,state)
                           VALUES (%s,%s,'PENDING') ON CONFLICT (event_id,consumer_id) DO NOTHING""",
                        (event.event_id, recipient),
                    )
                return event

    def acknowledge_delivery(
        self,
        *,
        event_id: str,
        consumer_id: str,
        actual_asset_uri: str,
        actual_partition_key: str,
        actual_event_body: Mapping[str, Any],
    ) -> None:
        """recipient ACK와 durable pending work 인계를 같은 transaction에서 기록한다."""
        body = json.loads(canonical_json(actual_event_body))
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT o.aggregate_id,o.asset_uri,o.partition_key,o.event_body,d.state,
                              p.deployment_id,r.model_unique_id
                       FROM dw_control.asset_outbox o JOIN dw_control.asset_deliveries d
                         ON d.event_id=o.event_id
                       JOIN dw_control.model_results r ON r.result_manifest_id=o.aggregate_id
                       JOIN dw_control.generation_plans p ON p.plan_id=r.plan_id
                       WHERE o.event_id=%s AND o.aggregate_kind='MODEL_READY'
                         AND d.consumer_id=%s FOR UPDATE OF d""",
                    (event_id, consumer_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("required recipient delivery가 없습니다")
                aggregate_id, asset_uri, partition_key, expected_body, _, deployment_id, model_id = row
                spec = self._delivery_spec(
                    cursor, deployment_id=deployment_id, model_unique_id=model_id
                )
                work_kind = spec.recipient_map.get(consumer_id)
                if work_kind is None:
                    raise ControlStoreConflict("consumer가 release required recipient가 아닙니다")
                if (asset_uri, partition_key, expected_body) != (
                    actual_asset_uri, actual_partition_key, body,
                ) or asset_uri != spec.asset_uri:
                    raise ControlStoreConflict("ACK event identity/body가 권위 outbox와 다릅니다")
                work_identity = content_id(
                    "work",
                    {
                        "consumer_id": consumer_id,
                        "event_id": event_id,
                        "aggregate_id": aggregate_id,
                        "partition_key": partition_key,
                        "work_kind": work_kind,
                    },
                )
                cursor.execute(
                    """INSERT INTO dw_control.asset_inbox
                       (consumer_id,event_id,asset_uri,partition_key,aggregate_id,event_body,
                        work_kind,work_identity,state)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'PENDING')
                       ON CONFLICT (consumer_id,event_id) DO NOTHING""",
                    (
                        consumer_id, event_id, asset_uri, partition_key,
                        aggregate_id, Jsonb(body), work_kind, work_identity,
                    ),
                )
                cursor.execute(
                    """SELECT asset_uri,partition_key,aggregate_id,event_body,work_kind,work_identity
                       FROM dw_control.asset_inbox WHERE consumer_id=%s AND event_id=%s""",
                    (consumer_id, event_id),
                )
                if cursor.fetchone() != (
                    asset_uri, partition_key, aggregate_id, body,
                    work_kind, work_identity,
                ):
                    raise ControlStoreConflict("같은 inbox ID에 다른 work/body가 있습니다")
                cursor.execute(
                    """UPDATE dw_control.asset_deliveries
                       SET state=CASE WHEN state='COMPLETED' THEN state ELSE 'ACKED' END,
                           acknowledged_at=COALESCE(acknowledged_at,clock_timestamp())
                       WHERE event_id=%s AND consumer_id=%s""",
                    (event_id, consumer_id),
                )
                cursor.execute(
                    """UPDATE dw_control.asset_outbox SET state='COMPLETE',completed_at=clock_timestamp()
                       WHERE event_id=%s AND NOT EXISTS (
                         SELECT 1 FROM dw_control.asset_deliveries
                         WHERE event_id=%s AND state NOT IN ('ACKED','COMPLETED'))""",
                    (event_id, event_id),
                )

    def claim_outbox(
        self,
        *,
        publisher_id: str,
        lease_seconds: int = 60,
        limit: int = 20,
    ) -> tuple[OutboxClaim, ...]:
        """미전달 event를 claim한다. 전송 성공만으로 delivery를 ACK하지 않는다."""
        if not publisher_id or lease_seconds < 1 or limit < 1:
            raise ValueError("publisher, lease, limit가 올바르지 않습니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT event_id FROM dw_control.asset_outbox
                       WHERE state='PENDING'
                          OR (state='SENDING' AND lease_expires_at <= clock_timestamp())
                       ORDER BY created_at,event_id
                       FOR UPDATE SKIP LOCKED LIMIT %s""",
                    (limit,),
                )
                event_ids = [row[0] for row in cursor.fetchall()]
                if not event_ids:
                    return ()
                cursor.execute(
                    """UPDATE dw_control.asset_outbox
                       SET state='SENDING',lease_owner=%s,
                           lease_expires_at=clock_timestamp()+(%s * interval '1 second'),
                           publisher_claim_token=nextval('dw_control.publisher_claim_token_seq')
                       WHERE event_id=ANY(%s)
                       RETURNING event_id,aggregate_kind,aggregate_id,asset_uri,partition_key,event_body,
                                 publisher_claim_token,lease_expires_at""",
                    (publisher_id, lease_seconds, event_ids),
                )
                return tuple(
                    OutboxClaim(
                        OutboxEvent(row[0], row[1], row[2], row[3], row[4], row[5]),
                        publisher_id,
                        row[6],
                        row[7],
                    )
                    for row in cursor.fetchall()
                )

    def claim_outbox_event(
        self,
        *,
        event_id: str,
        publisher_id: str,
        lease_seconds: int = 60,
    ) -> OutboxClaim:
        """한 DAG가 자신이 만든 exact outbox event 하나만 발행하도록 claim한다."""
        if not event_id or not publisher_id or lease_seconds < 1:
            raise ValueError("event, publisher, lease가 올바르지 않습니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.asset_outbox
                       SET state='SENDING',lease_owner=%s,
                           lease_expires_at=clock_timestamp()+(%s * interval '1 second'),
                           publisher_claim_token=nextval('dw_control.publisher_claim_token_seq')
                       WHERE event_id=%s
                         AND (state='PENDING'
                              OR (state='SENDING' AND lease_owner=%s)
                              OR (state='SENDING' AND lease_expires_at <= clock_timestamp()))
                       RETURNING event_id,aggregate_kind,aggregate_id,asset_uri,partition_key,
                                 event_body,publisher_claim_token,lease_expires_at""",
                    (publisher_id, lease_seconds, event_id, publisher_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ControlStoreConflict("exact outbox event가 pending 상태가 아닙니다")
                return OutboxClaim(
                    OutboxEvent(row[0], row[1], row[2], row[3], row[4], row[5]),
                    publisher_id,
                    row[6],
                    row[7],
                )

    def prepare_model_execution(
        self,
        *,
        event_id: str,
        model_unique_id: str,
        claim_owner: str,
        artifact_root: str,
        lease_seconds: int = 300,
        bind_relations: bool = False,
    ) -> ModelExecutionBundle:
        """ACK된 EXECUTE_MODEL을 claim하고 model/test exact 계약을 만든다."""
        # inbox와 build claim 사이에서 worker가 죽어도 한쪽만 commit되지 않도록 바깥
        # transaction이 두 공개 claim 메서드의 savepoint를 함께 감싼다.
        with self.connection.transaction():
            work = self.claim_work(
                consumer_id=model_unique_id,
                event_id=event_id,
                claim_owner=claim_owner,
                lease_seconds=lease_seconds,
            )
            if work.work_kind != "EXECUTE_MODEL":
                raise ControlStoreConflict("model DAG가 EXECUTE_MODEL 외 work를 받았습니다")
            with self.connection.cursor() as cursor:
                cursor.execute(
                """SELECT c.body,p.deployment_id,r.manifest_sha256,r.model_version,
                          s.model_selector,s.owned_test_unique_ids,s.owned_test_selectors
                   FROM dw_control.asset_inbox i
                   JOIN dw_control.model_cohorts c ON c.cohort_manifest_id=i.aggregate_id
                   JOIN dw_control.generation_plans p ON p.plan_id=c.plan_id
                   JOIN dw_control.dbt_invocation_registries r ON r.deployment_id=p.deployment_id
                   JOIN dw_control.dbt_model_invocation_specs s
                     ON s.deployment_id=p.deployment_id AND s.model_unique_id=c.model_unique_id
                   WHERE i.consumer_id=%s AND i.event_id=%s""",
                    (model_unique_id, event_id),
                )
                row = cursor.fetchone()
            if row is None:
                raise ControlStoreConflict("model execution cohort/spec을 찾을 수 없습니다")
            cohort = _cohort_from_body(row[0])
            if cohort.model_unique_id != model_unique_id:
                raise ControlStoreConflict("model execution cohort가 DAG model과 다릅니다")
            build = self.claim_build(
                build_id=cohort.build_id,
                claim_owner=claim_owner,
                lease_seconds=lease_seconds,
            )
            # 검증 실패 시 inbox/build claim도 함께 rollback한다.
            relation_vars = build_relation_vars(asdict(cohort), build.relation_id) if bind_relations else {}
        deployment_id = row[1].strip()
        common = {
            "deployment_id": deployment_id,
            "manifest_sha256": row[2].strip(),
            "model_version": row[3].strip(),
            "plan_id": cohort.plan_id,
            "cohort_manifest_id": cohort.cohort_manifest_id,
            "build_id": cohort.build_id,
            "attempt_no": build.attempt_no,
            "fence_token": build.fence_token,
            "claim_owner": claim_owner,
            "model_unique_id": model_unique_id,
            "artifact_root": artifact_root,
            "relation_vars": relation_vars,
        }
        model_contract = create_dbt_invocation_contract(
            invocation_kind="MODEL",
            expected_unique_ids=(model_unique_id,),
            expected_selectors=(row[4],),
            **common,
        )
        test_ids = tuple(row[5])
        test_selectors = tuple(row[6])
        test_contract = create_dbt_invocation_contract(
            invocation_kind="OWNED_TESTS" if test_ids else "EMPTY_TEST_SET",
            expected_unique_ids=test_ids,
            expected_selectors=test_selectors,
            **common,
        )
        return ModelExecutionBundle(work, build, cohort, model_contract, test_contract)

    def mark_outbox_sent(self, claim: OutboxClaim) -> None:
        """emit 완료 시 시도 이력만 남긴다. ACK/COMPLETE는 consumer가 결정한다."""
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.asset_outbox
                       SET send_attempts=CASE WHEN last_sent_claim_token=%s
                                              THEN send_attempts ELSE send_attempts+1 END,
                           last_sent_at=CASE WHEN last_sent_claim_token=%s
                                             THEN last_sent_at ELSE clock_timestamp() END,
                           last_sent_claim_token=%s
                       WHERE event_id=%s AND state='SENDING' AND lease_owner=%s
                         AND publisher_claim_token=%s
                         AND lease_expires_at > clock_timestamp()
                       RETURNING event_id""",
                    (
                        claim.claim_token, claim.claim_token, claim.claim_token,
                        claim.event.event_id, claim.publisher_id, claim.claim_token,
                    ),
                )
                if cursor.fetchone() is None:
                    raise ControlStoreConflict("stale publisher는 전송 완료를 기록할 수 없습니다")

    def claim_work(
        self,
        *,
        consumer_id: str,
        event_id: str,
        claim_owner: str,
        lease_seconds: int = 300,
        required: bool = True,
    ) -> WorkClaim | None:
        """ACK로 인계된 pending work를 별도 lease로 claim한다."""
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE dw_control.asset_inbox
                       SET state='CLAIMED',claim_owner=%s,
                           claim_expires_at=clock_timestamp()+(%s * interval '1 second'),
                           work_claim_token=nextval('dw_control.work_claim_token_seq')
                       WHERE consumer_id=%s AND event_id=%s
                         AND (state IN ('PENDING','FAILED')
                              OR (state='CLAIMED' AND claim_expires_at <= clock_timestamp()))
                       RETURNING work_kind,work_identity,work_claim_token,claim_expires_at""",
                    (claim_owner, lease_seconds, consumer_id, event_id),
                )
                row = cursor.fetchone()
                if row is None:
                    if not required:
                        return None
                    raise ControlStoreConflict("pending 또는 만료된 inbox work가 없습니다")
                return WorkClaim(consumer_id, event_id, row[0], row[1], claim_owner, row[2], row[3])

    def complete_work(self, claim: WorkClaim) -> None:
        """work 완료와 recipient delivery 완료를 원자적으로 연결한다."""
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT state,work_kind,work_identity,claim_owner,work_claim_token
                       FROM dw_control.asset_inbox WHERE consumer_id=%s AND event_id=%s FOR UPDATE""",
                    (claim.consumer_id, claim.event_id),
                )
                current = cursor.fetchone()
                if current == (
                    "COMPLETED", claim.work_kind, claim.work_identity,
                    claim.claim_owner, claim.claim_token,
                ):
                    return
                cursor.execute(
                    """UPDATE dw_control.asset_inbox
                       SET state='COMPLETED',completed_at=clock_timestamp()
                       WHERE consumer_id=%s AND event_id=%s AND state='CLAIMED'
                         AND claim_owner=%s AND work_claim_token=%s
                         AND work_kind=%s AND work_identity=%s
                         AND claim_expires_at > clock_timestamp()
                       RETURNING event_id""",
                    (
                        claim.consumer_id, claim.event_id, claim.claim_owner,
                        claim.claim_token, claim.work_kind, claim.work_identity,
                    ),
                )
                if cursor.fetchone() is None:
                    raise ControlStoreConflict("stale consumer는 work를 완료할 수 없습니다")
                cursor.execute(
                    """UPDATE dw_control.asset_deliveries
                       SET state='COMPLETED',completed_at=clock_timestamp()
                       WHERE event_id=%s AND consumer_id=%s AND state IN ('ACKED','COMPLETED')""",
                    (claim.event_id, claim.consumer_id),
                )

    def pending_work(self) -> list[tuple[str, str, str, str]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT consumer_id,event_id,work_kind,work_identity
                   FROM dw_control.asset_inbox
                   WHERE state IN ('PENDING','FAILED')
                      OR (state='CLAIMED' AND claim_expires_at <= clock_timestamp())
                   ORDER BY created_at,consumer_id,event_id"""
            )
            return list(cursor.fetchall())

    def requeue_one_recoverable_inbox_event(
        self, *, min_pending_age_seconds: int = 600
    ) -> OutboxEvent | None:
        """오래 pending이거나 lease가 만료된 inbox 하나의 원본 event를 다시 연다."""
        if min_pending_age_seconds < 1:
            raise ValueError("pending 복구 대기 시간은 양수여야 합니다")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """SELECT i.event_id
                       FROM dw_control.asset_inbox i
                       WHERE (i.state IN ('PENDING','FAILED')
                              AND i.created_at <= clock_timestamp()-(%s * interval '1 second'))
                          OR (i.state='CLAIMED' AND i.claim_expires_at <= clock_timestamp())
                       ORDER BY i.created_at,i.event_id
                       FOR UPDATE SKIP LOCKED LIMIT 1""",
                    (min_pending_age_seconds,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                cursor.execute(
                    """UPDATE dw_control.asset_outbox
                       SET state='PENDING',lease_owner=NULL,lease_expires_at=NULL,
                           completed_at=NULL
                       WHERE event_id=%s
                       RETURNING event_id,aggregate_kind,aggregate_id,asset_uri,
                                 partition_key,event_body""",
                    (row[0],),
                )
                event_row = cursor.fetchone()
                if event_row is None:
                    raise ControlStoreConflict("recoverable inbox의 원본 outbox가 없습니다")
                return OutboxEvent(*event_row)
