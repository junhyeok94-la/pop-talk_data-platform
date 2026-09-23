"""고정 source snapshot으로 generation을 만들고 source/deployment dbt gate를 실행한다."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

from pipelines.orchestration.dbt_model_registry import CURRENT_TEST_OWNER_OVERRIDES, registry_from_validated_deployment
from pipelines.orchestration.dbt_relation_bindings import relation_parts
from pipelines.orchestration.model_generation_contract import (
    SourceSnapshot, canonical_json, create_generation_plan, create_gate_receipt,
)
from pipelines.orchestration.postgres_control_store import PostgresControlStore

SERVING_MODELS = tuple(f"model.pop_talk_dw.{name}" for name in (
    "dim_movie", "fct_boxoffice_daily", "mart_data_quality",
))


def register_generation(connection, deployment, snapshots):
    """동일 snapshot의 재시도는 기존 plan을 반환한다. 최초에는 전체 모델을 계산한다."""
    registry = registry_from_validated_deployment(deployment, explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES)
    sources = tuple(sorted((SourceSnapshot(**item) for item in snapshots), key=lambda item: item.source_unique_id))
    # connection은 autocommit이다. session lock으로 번호 선택과 plan 등록을 직렬화한다.
    connection.execute("SELECT pg_advisory_lock(hashtext('pop_talk_generation_bootstrap'))")
    try:
        store = PostgresControlStore(connection)
        catalog = store.load_catalog()
        for plan in catalog.plans.values():
            if plan.deployment_id == deployment.deployment_id and plan.source_snapshots == sources:
                return plan
        plan = create_generation_plan(
            generation_sequence=max((p.generation_sequence for p in catalog.plans.values()), default=0) + 1,
            graph=registry.graph, source_snapshots=sources, changed_resources=registry.graph.sources,
            previous_results={}, required_serving_components=SERVING_MODELS,
        )
        store.register_plan(plan)
        return plan
    finally:
        connection.execute("SELECT pg_advisory_unlock(hashtext('pop_talk_generation_bootstrap'))")


def run_generation_gates(connection, deployment, plan, artifact_root: Path):
    """registry에 배정된 source/deployment test만 실행하고 정확한 성공 집합을 등록한다."""
    registry = registry_from_validated_deployment(deployment, explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES)
    groups = defaultdict(list)
    for test in registry.test_ownership:
        if test.owner_kind != "MODEL_GATE":
            groups[(test.owner_kind, test.owner_unique_id)].append(test)
    bindings = {source.source_unique_id: relation_parts(source.relation_id or "") for source in plan.source_snapshots}
    for (kind, owner), tests in sorted(groups.items()):
        directory = artifact_root / plan.plan_id / "gates" / hashlib.sha256(owner.encode()).hexdigest()
        directory.mkdir(parents=True, exist_ok=True)
        result_path = directory / "target" / "run_results.json"
        expected = sorted(test.test_unique_id for test in tests)
        if not result_path.exists():
            project = directory / "project"
            shutil.copytree(deployment.project_path, project, dirs_exist_ok=True)
            result = subprocess.run([
                "/opt/dbt-venv/bin/dbt", "test", "--project-dir", str(project), "--profiles-dir", str(project),
                "--target-path", str(directory / "target"), "--log-path", str(directory / "logs"),
                "--no-partial-parse", "--indirect-selection", "empty", "--select", *sorted(t.selector for t in tests),
                "--vars", canonical_json({"pop_talk_relations": bindings}).decode(),
            ], env={key: value for key, value in os.environ.items() if key != "PYTHONPATH" and not key.startswith("DBT_")},
                capture_output=True, text=True, timeout=1800)
            (directory / "console.log").write_text(result.stdout + result.stderr, encoding="utf-8")
            if result.returncode:
                # 실패 artifact는 성공으로 재사용하지 않는다. 다음 시도에서 다시 실행한다.
                result_path.unlink(missing_ok=True)
                raise RuntimeError(f"dbt {kind} 실패: {directory / 'console.log'}")
        raw = result_path.read_bytes()
        results = json.loads(raw)["results"]
        if sorted(item["unique_id"] for item in results) != expected or any(item["status"] != "pass" for item in results):
            raise RuntimeError("source/deployment gate의 성공 test 집합이 다릅니다")
        source = plan.source_map.get(owner)
        receipt = create_gate_receipt(
            gate_kind=kind, generation_id=plan.generation_id, plan_id=plan.plan_id,
            deployment_id=deployment.deployment_id, owner_unique_id=owner,
            source_snapshot_id=source.snapshot_id if source else None,
            executed_test_ids=expected, evidence_sha256=hashlib.sha256(raw).hexdigest(),
        )
        PostgresControlStore(connection).register_gate_receipt(receipt)
