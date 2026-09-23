"""승인 dbt manifest의 모델·source·test 소유권 전수 대사."""
from __future__ import annotations

import json
import unittest
from pathlib import Path
from pipelines.paths import DBT_DEPLOYMENT_ROOT

from pipelines.orchestration.dbt_model_registry import (
    CURRENT_TEST_OWNER_OVERRIDES,
    build_model_dag_registry,
    build_test_ownership,
    registry_from_validated_deployment,
)
from pipelines.orchestration.dbt_deployment import load_current_deployment
from pipelines.orchestration.model_generation_contract import GenerationContractError


class DbtModelRegistryTests(unittest.TestCase):
    def test_current_immutable_deployment_is_fully_owned(self) -> None:
        """stale target가 아니라 current pointer의 7-model manifest를 검사한다."""
        root = (Path(__file__).resolve().parents[3] / "modules")
        deployments = DBT_DEPLOYMENT_ROOT
        deployment = load_current_deployment(deployments)
        deployment_id = deployment.deployment_id
        manifest = json.loads(
            (deployments / deployment_id / "manifest.json").read_bytes()
        )
        registry = build_model_dag_registry(
            manifest,
            deployment_id=deployment_id,
            explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
        )
        self.assertEqual(len(registry.models), 7)
        self.assertEqual(len(registry.test_ownership), 43)
        counts = {
            kind: sum(item.owner_kind == kind for item in registry.test_ownership)
            for kind in ("MODEL_GATE", "SOURCE_GATE", "DEPLOYMENT_GATE")
        }
        self.assertEqual(
            counts,
            {"MODEL_GATE": 27, "SOURCE_GATE": 15, "DEPLOYMENT_GATE": 1},
        )
        self.assertEqual(
            sum(len(model.owned_test_unique_ids) for model in registry.models),
            counts["MODEL_GATE"],
        )
        self.assertEqual(
            {item.test_unique_id for item in registry.test_ownership},
            {
                unique_id
                for unique_id, node in manifest["nodes"].items()
                if node.get("resource_type") == "test"
            },
        )
        grain = next(
            item
            for item in registry.test_ownership
            if item.test_unique_id.endswith("assert_mart_boxoffice_preserves_fact_grain")
        )
        self.assertEqual(grain.owner_kind, "MODEL_GATE")
        self.assertEqual(
            grain.owner_unique_id,
            "model.pop_talk_dw.mart_boxoffice_daily",
        )
        parentless = next(
            item
            for item in registry.test_ownership
            if item.test_unique_id.endswith("assert_revision_precedes_completion_time")
        )
        self.assertEqual(parentless.owner_kind, "DEPLOYMENT_GATE")
        authoritative = registry_from_validated_deployment(
            deployment,
            explicit_owner_by_test_name=CURRENT_TEST_OWNER_OVERRIDES,
        )
        self.assertEqual(authoritative, registry)

    def test_ambiguous_and_parentless_tests_require_explicit_owner(self) -> None:
        manifest = {
            "nodes": {
                "model.p.a": {
                    "resource_type": "model",
                    "fqn": ["p", "a"],
                    "depends_on": {"nodes": []},
                },
                "model.p.b": {
                    "resource_type": "model",
                    "fqn": ["p", "b"],
                    "depends_on": {"nodes": ["model.p.a"]},
                },
                "test.p.multi": {
                    "resource_type": "test",
                    "name": "multi",
                    "fqn": ["p", "multi"],
                    "depends_on": {"nodes": ["model.p.a", "model.p.b"]},
                },
            },
            "sources": {},
        }
        with self.assertRaisesRegex(GenerationContractError, "명시적 owner"):
            build_test_ownership(manifest, explicit_owner_by_test_name={})
        owned = build_test_ownership(
            manifest,
            explicit_owner_by_test_name={"multi": ("MODEL_GATE", "model.p.b")},
        )
        self.assertEqual(owned[0].owner_unique_id, "model.p.b")
        with self.assertRaisesRegex(GenerationContractError, "manifest에 없는"):
            build_test_ownership(
                manifest,
                explicit_owner_by_test_name={
                    "multi": ("MODEL_GATE", "model.p.b"),
                    "deleted_test": ("DEPLOYMENT_GATE", "deployment"),
                },
            )

    def test_multi_parent_test_owner_must_have_every_parent_available(self) -> None:
        root = (Path(__file__).resolve().parents[3] / "modules")
        deployments = DBT_DEPLOYMENT_ROOT
        pointer = json.loads((deployments / "current.json").read_bytes())
        deployment_id = str(pointer["deployment_id"])
        manifest = json.loads(
            (deployments / deployment_id / "manifest.json").read_bytes()
        )
        wrong = {
            **CURRENT_TEST_OWNER_OVERRIDES,
            "assert_mart_boxoffice_preserves_fact_grain": (
                "MODEL_GATE",
                "model.pop_talk_dw.fct_boxoffice_daily",
            ),
        }
        with self.assertRaisesRegex(GenerationContractError, "준비할 수 없는 부모"):
            build_model_dag_registry(
                manifest,
                deployment_id=deployment_id,
                explicit_owner_by_test_name=wrong,
            )

    def test_dangling_source_and_invalid_owner_kind_are_rejected(self) -> None:
        manifest = {
            "nodes": {
                "model.p.a": {
                    "resource_type": "model",
                    "fqn": ["p", "a"],
                    "depends_on": {"nodes": []},
                },
                "test.p.source_check": {
                    "resource_type": "test",
                    "name": "source_check",
                    "fqn": ["p", "source_check"],
                    "depends_on": {"nodes": ["source.p.missing"]},
                },
            },
            "sources": {},
        }
        with self.assertRaisesRegex(GenerationContractError, "실제 source"):
            build_test_ownership(manifest, explicit_owner_by_test_name={})

        parentless = {
            "nodes": {
                "model.p.a": {
                    "resource_type": "model",
                    "fqn": ["p", "a"],
                    "depends_on": {"nodes": []},
                },
                "test.p.contract": {
                    "resource_type": "test",
                    "name": "contract",
                    "fqn": ["p", "contract"],
                    "depends_on": {"nodes": []},
                },
            },
            "sources": {},
        }
        with self.assertRaisesRegex(GenerationContractError, "owner kind"):
            build_test_ownership(
                parentless,
                explicit_owner_by_test_name={"contract": ("INVALID", "x")},  # type: ignore[arg-type]
            )


if __name__ == "__main__":
    unittest.main()
