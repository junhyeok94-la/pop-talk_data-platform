"""실제 PostgreSQL에서 durable control store의 transaction/fence/FK를 검증한다."""
from __future__ import annotations

import os
import hashlib
import json
import threading
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from pathlib import Path
from pipelines.paths import DBT_DEPLOYMENT_ROOT
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg import IsolationLevel
from psycopg.types.json import Jsonb

from pipelines.orchestration.model_generation_contract import (
    GraphSnapshot,
    create_generation_plan,
    create_gate_receipt,
    create_model_cohort,
    create_model_result,
    create_source_snapshot,
    empty_manifest_catalog,
    canonical_json,
    graph_snapshot_from_manifest,
    register_cohort,
    register_plan,
    register_result,
)
from pipelines.orchestration.dbt_model_registry import build_model_dag_registry
from pipelines.orchestration.dbt_model_registry import (
    CURRENT_TEST_OWNER_OVERRIDES,
    registry_from_validated_deployment,
)
from pipelines.orchestration.dbt_deployment import load_current_deployment
from pipelines.orchestration.dbt_invocation_contract import (
    DbtInvocationArtifact,
    create_dbt_invocation_contract,
)
from pipelines.orchestration.postgres_control_store import (
    ControlStoreConflict,
    ExecutionReceipt,
    PostgresControlStore,
    create_release_delivery_registry,
    parent_vector_sha256,
    tests_sha256,
)


class ReleaseDeliveryRegistryContractTests(unittest.TestCase):
    def _registry(self):
        source = "source.pop_talk_dw.synthetic.input"
        parent = "model.pop_talk_dw.synthetic_parent"
        child = "model.pop_talk_dw.synthetic_child"
        manifest = {
            "nodes": {
                parent: {
                    "resource_type": "model",
                    "fqn": ["pop_talk_dw", "synthetic", "synthetic_parent"],
                    "depends_on": {"nodes": [source]},
                },
                child: {
                    "resource_type": "model",
                    "fqn": ["pop_talk_dw", "synthetic", "synthetic_child"],
                    "depends_on": {"nodes": [parent]},
                },
            },
            "sources": {source: {"resource_type": "source"}},
        }
        return build_model_dag_registry(
            manifest,
            deployment_id="d" * 64,
            explicit_owner_by_test_name={},
        ), parent, child

    def test_children_are_derived_from_authoritative_graph(self) -> None:
        registry, parent, child = self._registry()
        delivery = create_release_delivery_registry(registry)
        specs = {item.model_unique_id: item for item in delivery.specs}
        self.assertEqual(
            specs[parent].recipient_work,
            ((child, "PREPARE_MODEL_COHORT"),),
        )

    def test_tampered_model_spec_parent_vector_is_rejected(self) -> None:
        registry, _, child = self._registry()
        tampered_models = tuple(
            replace(item, direct_parent_unique_ids=())
            if item.model_unique_id == child
            else item
            for item in registry.models
        )
        with self.assertRaises(ControlStoreConflict):
            create_release_delivery_registry(replace(registry, models=tampered_models))


@unittest.skipUnless(
    os.environ.get("RUN_DW_CONTROL_POSTGRES_TESTS") == "1",
    "실제 PostgreSQL 통합 검증에서만 실행",
)
class PostgresControlStoreIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = psycopg.connect(
            host=os.environ.get("POP_TALK_CONTROL_POSTGRES_HOST", os.environ["POP_TALK_POSTGRES_HOST"]),
            port=int(os.environ.get("POP_TALK_CONTROL_POSTGRES_PORT", os.environ.get("POP_TALK_POSTGRES_PORT", "5432"))),
            dbname=os.environ.get("POP_TALK_CONTROL_POSTGRES_DB", os.environ["POP_TALK_POSTGRES_DB"]),
            user=os.environ.get("POP_TALK_CONTROL_POSTGRES_USER", "dw_control_runtime"),
            password=os.environ["POP_TALK_CONTROL_POSTGRES_PASSWORD"],
        )
        # 각 test는 실제 constraint/lock을 사용하되 마지막에 전체를 rollback한다.
        self.connection.isolation_level = IsolationLevel.REPEATABLE_READ
        self.connection.execute("BEGIN")
        self.store = PostgresControlStore(self.connection)
        self.registered_deployments: set[str] = set()

    def tearDown(self) -> None:
        self.connection.rollback()
        self.connection.close()
        for deployment_id in self.registered_deployments:
            with self._admin_connection() as admin:
                admin.execute(
                    "DELETE FROM dw_control.dbt_model_invocation_specs WHERE deployment_id=%s",
                    (deployment_id,),
                )
                admin.execute(
                    "DELETE FROM dw_control.dbt_invocation_registries WHERE deployment_id=%s",
                    (deployment_id,),
                )
                admin.execute(
                    "DELETE FROM dw_control.release_delivery_specs WHERE deployment_id=%s",
                    (deployment_id,),
                )
                admin.execute(
                    "DELETE FROM dw_control.release_delivery_registries WHERE deployment_id=%s",
                    (deployment_id,),
                )

    @staticmethod
    def _runtime_connection():
        return psycopg.connect(
            host=os.environ.get("POP_TALK_CONTROL_POSTGRES_HOST", os.environ["POP_TALK_POSTGRES_HOST"]),
            port=int(os.environ.get("POP_TALK_CONTROL_POSTGRES_PORT", os.environ.get("POP_TALK_POSTGRES_PORT", "5432"))),
            dbname=os.environ.get("POP_TALK_CONTROL_POSTGRES_DB", os.environ["POP_TALK_POSTGRES_DB"]),
            user=os.environ.get("POP_TALK_CONTROL_POSTGRES_USER", "dw_control_runtime"),
            password=os.environ["POP_TALK_CONTROL_POSTGRES_PASSWORD"],
        )

    @staticmethod
    def _admin_connection():
        return psycopg.connect(
            host=os.environ["POP_TALK_POSTGRES_HOST"],
            port=int(os.environ.get("POP_TALK_POSTGRES_PORT", "5432")),
            dbname=os.environ["POP_TALK_POSTGRES_DB"],
            user=os.environ["POP_TALK_POSTGRES_USER"],
            password=os.environ["POP_TALK_POSTGRES_PASSWORD"],
        )

    def _cleanup_committed_fixture(self, plan, cohort) -> None:
        """동시성 test가 commit한 정확한 fixture만 owner로 제거한다."""
        with self._admin_connection() as admin:
            with admin.cursor() as cursor:
                cursor.execute("DELETE FROM dw_control.dbt_invocation_artifacts WHERE build_id=%s", (cohort.build_id,))
                cursor.execute("DELETE FROM dw_control.dbt_invocation_reservations WHERE build_id=%s", (cohort.build_id,))
                cursor.execute("DELETE FROM dw_control.model_build_attempts WHERE build_id=%s", (cohort.build_id,))
                cursor.execute("DELETE FROM dw_control.model_cohorts WHERE cohort_manifest_id=%s", (cohort.cohort_manifest_id,))
                cursor.execute("DELETE FROM dw_control.model_build_slots WHERE plan_id=%s", (plan.plan_id,))
                cursor.execute("DELETE FROM dw_control.generation_plans WHERE plan_id=%s", (plan.plan_id,))
                cursor.execute(
                    "DELETE FROM dw_control.dbt_model_invocation_specs WHERE deployment_id=%s",
                    (plan.deployment_id,),
                )
                cursor.execute(
                    "DELETE FROM dw_control.dbt_invocation_registries WHERE deployment_id=%s",
                    (plan.deployment_id,),
                )
                cursor.execute(
                    "DELETE FROM dw_control.release_delivery_specs WHERE deployment_id=%s AND model_unique_id=%s",
                    (plan.deployment_id, cohort.model_unique_id),
                )
                cursor.execute(
                    "DELETE FROM dw_control.release_delivery_registries WHERE deployment_id=%s",
                    (plan.deployment_id,),
                )
        self.registered_deployments.discard(plan.deployment_id)

    def _plan_and_cohort(self, source_relation=None):
        # deployment registry를 runtime transaction의 첫 snapshot 전에 commit할 수 있게 owner로 예약한다.
        with self._admin_connection() as admin, admin.cursor() as cursor:
            cursor.execute("SELECT COALESCE(max(generation_sequence),0)+1001 FROM dw_control.generation_plans")
            sequence = int(cursor.fetchone()[0])
        source_id = "source.pop_talk_dw.synthetic.input"
        model_id = "model.pop_talk_dw.synthetic_leaf"
        deployment_id = f"{sequence % 16:x}" * 64
        manifest = {
            "nodes": {
                model_id: {
                    "resource_type": "model",
                    "fqn": ["pop_talk_dw", "synthetic", "synthetic_leaf"],
                    "depends_on": {"nodes": [source_id]},
                }
            },
            "sources": {source_id: {"resource_type": "source"}},
        }
        graph = graph_snapshot_from_manifest(manifest, deployment_id=deployment_id)
        model_registry = build_model_dag_registry(
            manifest,
            deployment_id=deployment_id,
            explicit_owner_by_test_name={},
        )
        with self._admin_connection() as admin:
            admin_store = PostgresControlStore(admin)
            release_registry = admin_store.register_delivery_registry(
                model_registry,
                terminal_recipient_work_by_model={
                    model_id: {
                        "publication.consumer-a": "ASSEMBLE_PUBLICATION",
                        "publication.consumer-b": "ASSEMBLE_PUBLICATION",
                    }
                },
            )
            # 통합 test의 synthetic graph는 실제 배포 디렉터리가 아니므로 fixture owner가
            # v6 identity까지 직접 심는다. 운영 API는 validated deployment만 허용한다.
            invocation_body = json.loads(canonical_json({
                "deployment_id": deployment_id,
                "graph_digest": graph.graph_digest,
                "manifest_sha256": "e" * 64,
                "model_version": "f" * 64,
                "models": [{
                    "model_unique_id": model_id,
                    "model_selector": "fqn:pop_talk_dw.synthetic.synthetic_leaf",
                    "owned_test_unique_ids": [],
                    "owned_test_selectors": [],
                }],
                "test_ownership": [],
            }))
            invocation_digest = hashlib.sha256(canonical_json(invocation_body)).hexdigest()
            with admin.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO dw_control.dbt_invocation_registries
                       (deployment_id,graph_digest,manifest_sha256,model_version,
                        invocation_registry_digest,release_registry_digest,body)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (deployment_id, graph.graph_digest, "e" * 64, "f" * 64,
                     invocation_digest, release_registry.registry_digest, Jsonb(invocation_body)),
                )
                spec_body = {
                    "deployment_id": deployment_id,
                    "model_unique_id": model_id,
                    "model_selector": "fqn:pop_talk_dw.synthetic.synthetic_leaf",
                    "owned_test_unique_ids": [],
                    "owned_test_selectors": [],
                }
                cursor.execute(
                    """INSERT INTO dw_control.dbt_model_invocation_specs
                       (deployment_id,model_unique_id,model_selector,owned_test_unique_ids,
                        owned_test_selectors,body_sha256,invocation_registry_digest)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (deployment_id, model_id, spec_body["model_selector"], Jsonb([]), Jsonb([]),
                     hashlib.sha256(canonical_json(spec_body)).hexdigest(), invocation_digest),
                )
        self.registered_deployments.add(deployment_id)
        self.last_model_registry = model_registry
        source = create_source_snapshot(
            source_unique_id=source_id,
            source_batch_id=f"batch-{sequence}",
            dataset_revision=sequence,
            manifest_key=source_relation or f"synthetic/{sequence}.json",
            cutoff="2026-09-10T00:00:00Z",
            content_sha256="a" * 64,
        )
        plan = create_generation_plan(
            generation_sequence=sequence,
            graph=graph,
            source_snapshots=[source],
            changed_resources=[],
            previous_results={},
            required_serving_components=[],
        )
        self.store.register_plan(plan)
        catalog = register_plan(empty_manifest_catalog(), plan)
        cohort = create_model_cohort(plan=plan, model_unique_id=model_id, catalog=catalog)
        assert cohort is not None
        self.store.register_cohort(cohort)
        return plan, cohort, register_cohort(catalog, cohort)

    @staticmethod
    def _model_invocation(
        plan, cohort, claim, *, invocation_kind="MODEL",
        artifact_root="/opt/airflow/dbt-attempt-artifacts",
    ):
        if invocation_kind == "MODEL":
            ids = (cohort.model_unique_id,)
            selectors = ("fqn:pop_talk_dw.synthetic.synthetic_leaf",)
        else:
            ids = ()
            selectors = ()
        return create_dbt_invocation_contract(
            invocation_kind=invocation_kind,
            deployment_id=plan.deployment_id,
            manifest_sha256="e" * 64,
            model_version="f" * 64,
            plan_id=plan.plan_id,
            cohort_manifest_id=cohort.cohort_manifest_id,
            build_id=claim.build_id,
            attempt_no=claim.attempt_no,
            fence_token=claim.fence_token,
            claim_owner=claim.claim_owner,
            model_unique_id=cohort.model_unique_id,
            expected_unique_ids=ids,
            expected_selectors=selectors,
            artifact_root=artifact_root,
        )

    def _success_receipt(self, plan, cohort, claim) -> ExecutionReceipt:
        return ExecutionReceipt(
            build_id=claim.build_id,
            attempt_no=claim.attempt_no,
            fence_token=claim.fence_token,
            cohort_manifest_id=cohort.cohort_manifest_id,
            deployment_id=plan.deployment_id,
            parent_vector_sha256=parent_vector_sha256(cohort),
            claim_owner=claim.claim_owner,
            relation_id=claim.relation_id,
            row_count=3,
            content_sha256="b" * 64,
            model_invocation_id=f"model-{claim.fence_token}",
            model_unique_id=cohort.model_unique_id,
            model_run_results_sha256="c" * 64,
            test_invocation_id=None,
            test_run_results_sha256=None,
            executed_test_ids=(),
            tests_sha256=tests_sha256(()),
            relation_validation_query_id=f"query-{claim.fence_token}",
            status="EMPTY_TEST_SET",
        )

    def test_runtime_role_is_isolated_and_schema_contract_exists(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT r.rolsuper,r.rolcreaterole,r.rolcreatedb,
                          pg_has_role(current_user,'dw_control_owner','MEMBER'),
                          has_schema_privilege(current_user,'dw_control','USAGE'),
                          has_schema_privilege(current_user,'dw_control','CREATE')
                   FROM pg_roles r WHERE r.rolname=current_user"""
            )
            self.assertEqual(cursor.fetchone(), (False, False, False, False, True, False))
            cursor.execute("SELECT count(*) FROM dw_control.schema_migrations WHERE version=1")
            self.assertEqual(cursor.fetchone()[0], 1)
            cursor.execute(
                """SELECT has_table_privilege(current_user,'dw_control.schema_migrations','UPDATE'),
                          has_table_privilege(current_user,'dw_control.generation_plans','UPDATE'),
                          has_table_privilege(current_user,'dw_control.model_build_slots','UPDATE'),
                          has_column_privilege(current_user,'dw_control.model_build_slots','state','UPDATE'),
                          has_column_privilege(current_user,'dw_control.model_build_slots','reuse_result_manifest_id','UPDATE'),
                          has_table_privilege(current_user,'dw_control.release_delivery_specs','INSERT'),
                          has_table_privilege(current_user,'dw_control.dbt_model_invocation_specs','INSERT'),
                          has_table_privilege(current_user,'dw_control.dbt_invocation_artifacts','UPDATE'),
                          has_table_privilege(current_user,'dw_control.dbt_invocation_reservations','UPDATE'),
                          has_column_privilege(current_user,'dw_control.dbt_invocation_reservations','state','UPDATE'),
                          has_column_privilege(current_user,'dw_control.dbt_invocation_reservations','contract_body','UPDATE')"""
            )
            self.assertEqual(
                cursor.fetchone(),
                (False, False, False, True, False, False, False, False, False, True, False),
            )

    def test_fence_rejects_expired_worker_and_uses_distinct_relation(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        first = self.store.claim_build(build_id=cohort.build_id, claim_owner="worker-a")
        self.connection.execute(
            "UPDATE dw_control.model_build_slots SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE build_id=%s",
            (cohort.build_id,),
        )
        second = self.store.claim_build(build_id=cohort.build_id, claim_owner="worker-b")
        self.assertGreater(second.fence_token, first.fence_token)
        self.assertEqual(second.attempt_no, first.attempt_no + 1)
        self.assertNotEqual(second.relation_id, first.relation_id)
        with self.assertRaises(ControlStoreConflict):
            self.store.heartbeat(first)
        self.store.heartbeat(second)

    def test_result_and_recipient_work_commit_are_atomic_and_deduplicated(self) -> None:
        plan, cohort, catalog = self._plan_and_cohort()
        claim = self.store.claim_build(build_id=cohort.build_id, claim_owner="worker-a")
        receipt = self._success_receipt(plan, cohort, claim)
        self.store.record_receipt(receipt)
        result = create_model_result(
            plan=plan,
            cohort=cohort,
            relation_id=claim.relation_id,
            row_count=receipt.row_count,
            content_sha256=receipt.content_sha256,
            catalog=catalog,
        )
        event = self.store.commit_result(
            result=result,
            claim=claim,
        )
        # commit 응답 유실 뒤 같은 본문으로 재시도하면 중복 없이 성공으로 확정한다.
        self.store.commit_result(
            result=result,
            claim=claim,
        )
        claimed_events = self.store.claim_outbox(publisher_id="publisher-a")
        self.assertEqual([item.event.event_id for item in claimed_events], [event.event_id])
        old_publish_claim = claimed_events[0]
        self.connection.execute(
            "UPDATE dw_control.asset_outbox SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE event_id=%s",
            (event.event_id,),
        )
        new_publish_claim = self.store.claim_outbox(publisher_id="publisher-a")[0]
        self.assertGreater(new_publish_claim.claim_token, old_publish_claim.claim_token)
        with self.assertRaises(ControlStoreConflict):
            self.store.mark_outbox_sent(old_publish_claim)
        self.store.mark_outbox_sent(new_publish_claim)
        self.store.mark_outbox_sent(new_publish_claim)
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT send_attempts FROM dw_control.asset_outbox WHERE event_id=%s", (event.event_id,))
            self.assertEqual(cursor.fetchone()[0], 1)
        self.store.acknowledge_delivery(
            event_id=event.event_id,
            consumer_id="publication.consumer-a",
            actual_asset_uri=event.asset_uri,
            actual_partition_key=event.partition_key,
            actual_event_body=event.event_body,
        )
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT state FROM dw_control.asset_outbox WHERE event_id=%s", (event.event_id,))
            self.assertNotEqual(cursor.fetchone()[0], "COMPLETE")
        self.store.acknowledge_delivery(
            event_id=event.event_id,
            consumer_id="publication.consumer-b",
            actual_asset_uri=event.asset_uri,
            actual_partition_key=event.partition_key,
            actual_event_body=event.event_body,
        )
        # 같은 ACK는 같은 body/work일 때만 멱등이다.
        self.store.acknowledge_delivery(
            event_id=event.event_id,
            consumer_id="publication.consumer-b",
            actual_asset_uri=event.asset_uri,
            actual_partition_key=event.partition_key,
            actual_event_body=event.event_body,
        )
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT state FROM dw_control.asset_outbox WHERE event_id=%s", (event.event_id,))
            self.assertEqual(cursor.fetchone()[0], "COMPLETE")
        pending = self.store.pending_work()
        self.assertEqual(
            {row[0] for row in pending},
            {"publication.consumer-a", "publication.consumer-b"},
        )
        work = self.store.claim_work(
            consumer_id="publication.consumer-a",
            event_id=event.event_id,
            claim_owner="consumer-worker-a",
        )
        self.store.complete_work(work)
        self.store.complete_work(work)
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT state FROM dw_control.asset_deliveries WHERE event_id=%s AND consumer_id='publication.consumer-a'",
                (event.event_id,),
            )
            self.assertEqual(cursor.fetchone()[0], "COMPLETED")

        with self.assertRaises(ControlStoreConflict):
            self.store.acknowledge_delivery(
                event_id=event.event_id,
                consumer_id="invented-consumer",
                actual_asset_uri=event.asset_uri,
                actual_partition_key=event.partition_key,
                actual_event_body=event.event_body,
            )
        with self.assertRaises(ControlStoreConflict):
            self.store.acknowledge_delivery(
                event_id=event.event_id,
                consumer_id="publication.consumer-b",
                actual_asset_uri=event.asset_uri,
                actual_partition_key="gen-999999999999",
                actual_event_body=event.event_body,
            )

    def test_stale_receipt_and_conflicting_inbox_body_are_rejected(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        first = self.store.claim_build(build_id=cohort.build_id, claim_owner="worker-a")
        self.connection.execute(
            "UPDATE dw_control.model_build_slots SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE build_id=%s",
            (cohort.build_id,),
        )
        self.store.claim_build(build_id=cohort.build_id, claim_owner="worker-b")
        with self.assertRaises(ControlStoreConflict):
            self.store.record_receipt(self._success_receipt(plan, cohort, first))

    def test_same_owner_old_inbox_claim_cannot_complete_new_lease(self) -> None:
        plan, cohort, catalog = self._plan_and_cohort()
        claim = self.store.claim_build(build_id=cohort.build_id, claim_owner="builder")
        receipt = self._success_receipt(plan, cohort, claim)
        self.store.record_receipt(receipt)
        result = create_model_result(
            plan=plan,
            cohort=cohort,
            relation_id=claim.relation_id,
            row_count=receipt.row_count,
            content_sha256=receipt.content_sha256,
            catalog=catalog,
        )
        event = self.store.commit_result(result=result, claim=claim)
        self.store.acknowledge_delivery(
            event_id=event.event_id,
            consumer_id="publication.consumer-a",
            actual_asset_uri=event.asset_uri,
            actual_partition_key=event.partition_key,
            actual_event_body=event.event_body,
        )
        old = self.store.claim_work(
            consumer_id="publication.consumer-a", event_id=event.event_id, claim_owner="same-worker"
        )
        self.connection.execute(
            """UPDATE dw_control.asset_inbox
               SET claim_expires_at=clock_timestamp()-interval '1 second'
               WHERE consumer_id='publication.consumer-a' AND event_id=%s""",
            (event.event_id,),
        )
        new = self.store.claim_work(
            consumer_id="publication.consumer-a", event_id=event.event_id, claim_owner="same-worker"
        )
        self.assertGreater(new.claim_token, old.claim_token)
        with self.assertRaises(ControlStoreConflict):
            self.store.complete_work(old)
        self.store.complete_work(new)

    def test_runtime_cannot_register_delivery_registry(self) -> None:
        self._plan_and_cohort()
        with self.assertRaises(ControlStoreConflict):
            self.store.register_delivery_registry(self.last_model_registry)

    def test_cross_generation_reuse_references_previous_result(self) -> None:
        plan, cohort, catalog = self._plan_and_cohort()
        claim = self.store.claim_build(build_id=cohort.build_id, claim_owner="builder")
        receipt = self._success_receipt(plan, cohort, claim)
        self.store.record_receipt(receipt)
        result = create_model_result(
            plan=plan,
            cohort=cohort,
            relation_id=claim.relation_id,
            row_count=receipt.row_count,
            content_sha256=receipt.content_sha256,
            catalog=catalog,
        )
        self.store.commit_result(result=result, claim=claim)
        prior_catalog = register_result(catalog, result)
        graph = GraphSnapshot(
            deployment_id=plan.deployment_id,
            dependencies=plan.graph_dependencies,
            sources=tuple(item.source_unique_id for item in plan.source_snapshots),
            graph_digest=plan.graph_digest,
        )
        next_plan = create_generation_plan(
            generation_sequence=plan.generation_sequence + 1,
            graph=graph,
            source_snapshots=plan.source_snapshots,
            changed_resources=[],
            previous_results={result.model_unique_id: result},
            required_serving_components=[],
            previous_plan=plan,
            manifest_catalog=prior_catalog,
        )
        self.store.register_plan(next_plan)
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT mode,state,reuse_result_manifest_id,result_manifest_id
                   FROM dw_control.model_build_slots WHERE build_id=%s""",
                (next_plan.build_slots[0].build_id,),
            )
            self.assertEqual(
                cursor.fetchone(),
                ("REUSE", "REUSED", result.result_manifest_id, result.result_manifest_id),
            )

    def test_two_connections_cannot_hold_the_same_build_claim(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        self.connection.commit()
        barrier = threading.Barrier(2)

        def attempt(owner: str):
            connection = self._runtime_connection()
            try:
                barrier.wait(timeout=5)
                return PostgresControlStore(connection).claim_build(
                    build_id=cohort.build_id, claim_owner=owner
                )
            except ControlStoreConflict as exc:
                return exc
            finally:
                connection.close()

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(attempt, ("worker-a", "worker-b")))
            self.assertEqual(sum(not isinstance(item, Exception) for item in outcomes), 1)
            self.assertEqual(sum(isinstance(item, ControlStoreConflict) for item in outcomes), 1)
        finally:
            self._cleanup_committed_fixture(plan, cohort)

    def test_dbt_invocation_requires_model_first_and_records_immutable_artifact(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        claim = self.store.claim_build(build_id=cohort.build_id, claim_owner="strict-worker")
        empty = self._model_invocation(plan, cohort, claim, invocation_kind="EMPTY_TEST_SET")
        with self.assertRaises(ControlStoreConflict):
            self.store.reserve_dbt_invocation(empty)

        contract = self._model_invocation(plan, cohort, claim)
        reservation, owns = self.store.reserve_dbt_invocation(contract)
        self.assertTrue(owns)
        self.assertEqual(reservation.state, "RESERVED")
        self.store.set_dbt_invocation_state(
            contract, expected_state="RESERVED", new_state="RUNNING"
        )
        native_id = str(uuid.uuid4())
        self.store.set_dbt_invocation_state(
            contract,
            expected_state="RUNNING",
            new_state="FILE_SEALED",
            artifact_path="/opt/airflow/dbt-attempt-artifacts/model",
            dbt_native_invocation_id=native_id,
        )
        artifact = DbtInvocationArtifact(
            orchestration_invocation_id=contract.orchestration_invocation_id,
            dbt_native_invocation_id=native_id,
            build_id=contract.build_id,
            attempt_no=contract.attempt_no,
            fence_token=contract.fence_token,
            cohort_manifest_id=contract.cohort_manifest_id,
            deployment_id=contract.deployment_id,
            model_unique_id=contract.model_unique_id,
            invocation_kind="MODEL",
            expected_unique_ids=contract.expected_unique_ids,
            executed_unique_ids=contract.expected_unique_ids,
            run_results_sha256="1" * 64,
            argv_sha256="2" * 64,
            environment_sha256="3" * 64,
            artifact_path="/opt/airflow/dbt-attempt-artifacts/model",
            status="SUCCEEDED",
        )
        self.store.record_dbt_invocation_artifact(contract, artifact)
        self.store.record_dbt_invocation_artifact(contract, artifact)
        _, empty_owns = self.store.reserve_dbt_invocation(empty)
        self.assertTrue(empty_owns)

    def test_two_connections_reserve_one_dbt_model_execution(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        claim = self.store.claim_build(build_id=cohort.build_id, claim_owner="strict-worker")
        self.connection.commit()
        barrier = threading.Barrier(2)
        execution_count = 0
        count_lock = threading.Lock()

        def reserve(nonce: str):
            nonlocal execution_count
            connection = self._runtime_connection()
            try:
                contract = create_dbt_invocation_contract(
                    invocation_kind="MODEL",
                    deployment_id=plan.deployment_id,
                    manifest_sha256="e" * 64,
                    model_version="f" * 64,
                    plan_id=plan.plan_id,
                    cohort_manifest_id=cohort.cohort_manifest_id,
                    build_id=claim.build_id,
                    attempt_no=claim.attempt_no,
                    fence_token=claim.fence_token,
                    claim_owner=claim.claim_owner,
                    model_unique_id=cohort.model_unique_id,
                    expected_unique_ids=(cohort.model_unique_id,),
                    expected_selectors=("fqn:pop_talk_dw.synthetic.synthetic_leaf",),
                    artifact_root=f"/opt/airflow/dbt-attempt-artifacts/{nonce}",
                )
                barrier.wait(timeout=5)
                _, owns = PostgresControlStore(connection).reserve_dbt_invocation(contract)
                if owns:
                    with count_lock:
                        execution_count += 1
                return owns
            except ControlStoreConflict:
                return False
            finally:
                connection.close()

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(reserve, ("a", "b")))
            self.assertEqual(sum(outcomes), 1)
            self.assertEqual(execution_count, 1)
        finally:
            self._cleanup_committed_fixture(plan, cohort)

    def test_actual_main_exit_one_persists_failed_state(self) -> None:
        """실제 main/store/child 경로의 실패가 connection 종료 뒤에도 남아야 한다."""
        from scripts import dbt_strict_runner

        plan, cohort, _ = self._plan_and_cohort()
        claim = self.store.claim_build(build_id=cohort.build_id, claim_owner="main-worker")
        self.connection.commit()
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                artifact_root = root / "artifacts"
                artifact_root.mkdir()
                project = root / "deployment-project"
                project.mkdir()
                (project / "dbt_project.yml").write_text(
                    "name: pop_talk_dw\n", encoding="utf-8"
                )
                manifest = root / "manifest.json"
                manifest.write_text(
                    json.dumps({
                        "nodes": {
                            cohort.model_unique_id: {"resource_type": "model"}
                        }
                    }),
                    encoding="utf-8",
                )
                deployment = SimpleNamespace(
                    project_path=project,
                    manifest_path=manifest,
                    profile_name="pop_talk_dw",
                    target_name="dev",
                )
                executable = root / "fake-dbt-exit-one"
                executable.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
                executable.chmod(0o700)
                contract = self._model_invocation(
                    plan, cohort, claim, artifact_root=str(artifact_root)
                )
                arguments = [
                    "dbt_strict_runner.py", "run", "--select",
                    contract.expected_selectors[0], "--project-dir", "/tmp/cosmos",
                    "--profiles-dir", "/tmp/cosmos", "--profile", "pop_talk_dw",
                    "--target", "dev",
                ]
                environment = {
                    dbt_strict_runner.CONTRACT_ENV: json.dumps(contract.body),
                    "POP_TALK_DBT_ARTIFACT_ROOT": str(artifact_root),
                }
                with patch.dict(os.environ, environment, clear=False), patch.object(
                    dbt_strict_runner.sys, "argv", arguments
                ), patch.object(
                    dbt_strict_runner, "REAL_DBT_EXECUTABLE", str(executable)
                ), patch(
                    "pipelines.orchestration.dbt_deployment.validate_deployment",
                    return_value=deployment,
                ), patch.object(
                    dbt_strict_runner,
                    "_connect_control",
                    side_effect=self._runtime_connection,
                ):
                    with self.assertRaisesRegex(RuntimeError, "exit code 1"):
                        dbt_strict_runner.main()
            with self._admin_connection() as admin, admin.cursor() as cursor:
                cursor.execute(
                    """SELECT state,failure_reason
                       FROM dw_control.dbt_invocation_reservations
                       WHERE orchestration_invocation_id=%s""",
                    (contract.orchestration_invocation_id,),
                )
                self.assertEqual(cursor.fetchone(), ("FAILED", "RuntimeError"))
        finally:
            self._cleanup_committed_fixture(plan, cohort)

    def test_cohort_ready_ack_creates_durable_execute_model_work(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        event = self.store.register_cohort_and_outbox(cohort)
        replay = self.store.register_cohort_and_outbox(cohort)
        self.assertEqual(replay, event)
        self.assertEqual(event.aggregate_kind, "COHORT_READY")
        self.assertEqual(event.partition_key, plan.generation_id)

        self.store.acknowledge_cohort_delivery(
            event_id=event.event_id,
            expected_model_unique_id=cohort.model_unique_id,
            actual_asset_uri=event.asset_uri,
            actual_partition_key=event.partition_key,
            actual_event_body=event.event_body,
        )
        # ACK 뒤 worker가 사라져도 inbox claim으로 model 실행을 재개할 수 있다.
        work = self.store.claim_work(
            consumer_id=cohort.model_unique_id,
            event_id=event.event_id,
            claim_owner="model-worker",
        )
        self.assertEqual(work.work_kind, "EXECUTE_MODEL")
        self.store.complete_work(work)
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT d.state,i.state,o.state
                   FROM dw_control.asset_deliveries d
                   JOIN dw_control.asset_inbox i
                     ON i.event_id=d.event_id AND i.consumer_id=d.consumer_id
                   JOIN dw_control.asset_outbox o ON o.event_id=d.event_id
                   WHERE d.event_id=%s AND d.consumer_id=%s""",
                (event.event_id, cohort.model_unique_id),
            )
            self.assertEqual(cursor.fetchone(), ("COMPLETED", "COMPLETED", "COMPLETE"))

    def test_model_relation_contract_is_derived_from_ledger(self) -> None:
        _, cohort, _ = self._plan_and_cohort("POP_TALK_DW_DEV.CANDIDATE.SOURCE_FIXTURE")
        event = self.store.register_cohort_and_outbox(cohort)
        self.store.acknowledge_cohort_delivery(
            event_id=event.event_id, expected_model_unique_id=cohort.model_unique_id,
            actual_asset_uri=event.asset_uri, actual_partition_key=event.partition_key,
            actual_event_body=event.event_body,
        )
        bundle = self.store.prepare_model_execution(
            event_id=event.event_id, model_unique_id=cohort.model_unique_id,
            claim_owner="binding-worker", artifact_root="/opt/airflow/dbt-attempt-artifacts",
            bind_relations=True,
        )
        values = asdict(bundle.model_contract)
        values.pop("contract_version")
        values.pop("orchestration_invocation_id")
        bindings = json.loads(values.pop("relation_vars_json"))
        bindings["pop_talk_relations"][cohort.model_unique_id]["identifier"] = "FORGED_OUTPUT"
        forged = create_dbt_invocation_contract(**values, relation_vars=bindings)
        with self.assertRaisesRegex(ControlStoreConflict, "relation bindings"):
            self.store.reserve_dbt_invocation(forged)
        _, owns = self.store.reserve_dbt_invocation(bundle.model_contract)
        self.assertTrue(owns)

    def test_gate_arrival_rescan_releases_only_directly_ready_roots(self) -> None:
        """deployment+한 source gate만으로 unrelated source를 기다리지 않고 root를 연다."""
        project_root = (Path(__file__).resolve().parents[3] / "modules")
        deployment = load_current_deployment(DBT_DEPLOYMENT_ROOT)
        registry = registry_from_validated_deployment(
            deployment,
            explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
        )
        with self._admin_connection() as admin:
            admin_store = PostgresControlStore(admin)
            admin_store.register_delivery_registry(registry)
            admin_store.register_dbt_invocation_registry(
                deployment,
                explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
            )
        self.registered_deployments.add(deployment.deployment_id)
        with self._admin_connection() as admin, admin.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(max(generation_sequence),0)+3001 FROM dw_control.generation_plans"
            )
            sequence = int(cursor.fetchone()[0])
        snapshots_by_id = {
            source_id: create_source_snapshot(
                source_unique_id=source_id,
                source_batch_id=f"gate-batch-{sequence}-{index}",
                dataset_revision=sequence + index,
                manifest_key=f"synthetic/gate/{sequence}/{index}.json",
                cutoff="2026-09-11T00:00:00Z",
                content_sha256=f"{index + 1:x}" * 64,
            )
            for index, source_id in enumerate(registry.graph.sources)
        }
        plan = create_generation_plan(
            generation_sequence=sequence,
            graph=registry.graph,
            source_snapshots=snapshots_by_id.values(),
            changed_resources=registry.graph.sources,
            previous_results={},
            required_serving_components=[],
        )
        self.store.register_plan(plan)
        self.assertEqual(self.store.materialize_ready_cohorts(plan_id=plan.plan_id), ())

        ownership = registry.test_ownership
        deployment_owner = next(
            item.owner_unique_id for item in ownership if item.owner_kind == "DEPLOYMENT_GATE"
        )
        deployment_tests = tuple(
            sorted(
                item.test_unique_id
                for item in ownership
                if item.owner_kind == "DEPLOYMENT_GATE"
                and item.owner_unique_id == deployment_owner
            )
        )
        self.store.register_gate_receipt(
            create_gate_receipt(
                gate_kind="DEPLOYMENT_GATE",
                generation_id=plan.generation_id,
                plan_id=plan.plan_id,
                deployment_id=plan.deployment_id,
                owner_unique_id=deployment_owner,
                source_snapshot_id=None,
                executed_test_ids=deployment_tests,
                evidence_sha256="d" * 64,
            )
        )
        root_model = "model.pop_talk_dw.stg_successful_exchange_publications"
        root_source = registry.graph.dependency_map[root_model][0]
        source_tests = tuple(
            sorted(
                item.test_unique_id
                for item in ownership
                if item.owner_kind == "SOURCE_GATE"
                and item.owner_unique_id == root_source
            )
        )
        self.store.register_gate_receipt(
            create_gate_receipt(
                gate_kind="SOURCE_GATE",
                generation_id=plan.generation_id,
                plan_id=plan.plan_id,
                deployment_id=plan.deployment_id,
                owner_unique_id=root_source,
                source_snapshot_id=snapshots_by_id[root_source].snapshot_id,
                executed_test_ids=source_tests,
                evidence_sha256="e" * 64,
            )
        )
        events = self.store.materialize_ready_cohorts(plan_id=plan.plan_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_body["model_unique_id"], root_model)
        self.assertEqual(self.store.materialize_ready_cohorts(plan_id=plan.plan_id), ())

    def test_load_catalog_rejects_sql_identity_body_divergence(self) -> None:
        plan, cohort, _ = self._plan_and_cohort()
        self.connection.commit()
        try:
            with self._admin_connection() as admin:
                admin.execute(
                    """UPDATE dw_control.generation_plans
                       SET body=jsonb_set(body,'{generation_id}',to_jsonb(%s::text))
                       WHERE plan_id=%s""",
                    ("gen-999999999999", plan.plan_id),
                )
            with self.assertRaises(ControlStoreConflict):
                self.store.load_catalog()
        finally:
            self._cleanup_committed_fixture(plan, cohort)


if __name__ == "__main__":
    unittest.main()
