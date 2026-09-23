"""모델 DAG 분할의 generation/result/cohort 불변 계약 테스트."""
from __future__ import annotations

import unittest
from dataclasses import asdict, replace

from pipelines.orchestration.model_generation_contract import (
    GenerationContractError,
    ManifestCatalog,
    ModelResultManifest,
    affected_models,
    build_cohort_ready_event,
    build_model_ready_event,
    content_id,
    create_generation_plan,
    create_gate_receipt,
    create_model_cohort,
    create_model_result,
    create_source_snapshot,
    empty_manifest_catalog,
    graph_snapshot_from_manifest,
    register_cohort,
    register_plan,
    register_result,
    scan_ready_cohorts,
    validate_model_ready_event,
    validate_cohort_ready_event,
    validate_result_provenance,
    validate_result_catalog,
)

DEPLOYMENT_ID = "d" * 64
MOVIE_SOURCE = "source.pop_talk_dw.staging.movie"
BOXOFFICE_SOURCE = "source.pop_talk_dw.staging.boxoffice"
STG_MOVIE = "model.pop_talk_dw.stg_movie"
STG_BOXOFFICE = "model.pop_talk_dw.stg_boxoffice"
DIM_MOVIE = "model.pop_talk_dw.dim_movie"
FCT_BOXOFFICE = "model.pop_talk_dw.fct_boxoffice"
MART_BOXOFFICE = "model.pop_talk_dw.mart_boxoffice"


def manifest() -> dict:
    dependencies = {
        STG_MOVIE: [MOVIE_SOURCE],
        STG_BOXOFFICE: [BOXOFFICE_SOURCE],
        DIM_MOVIE: [STG_MOVIE],
        FCT_BOXOFFICE: [STG_BOXOFFICE],
        MART_BOXOFFICE: [DIM_MOVIE, FCT_BOXOFFICE],
    }
    return {
        "nodes": {
            model_id: {
                "resource_type": "model",
                "fqn": ["pop_talk_dw", model_id.rsplit(".", 1)[-1]],
                "depends_on": {"nodes": parents},
            }
            for model_id, parents in dependencies.items()
        },
        "sources": {
            source_id: {"resource_type": "source"}
            for source_id in (MOVIE_SOURCE, BOXOFFICE_SOURCE)
        },
    }


def snapshots(*, movie_revision: int = 1, movie_digest: str = "1" * 64) -> list:
    return [
        create_source_snapshot(
            source_unique_id=MOVIE_SOURCE,
            source_batch_id=f"movie-r{movie_revision}",
            dataset_revision=movie_revision,
            manifest_key=f"manifests/movie/r{movie_revision}.json",
            cutoff="2026-09-10T00:00:00Z",
            content_sha256=movie_digest,
        ),
        create_source_snapshot(
            source_unique_id=BOXOFFICE_SOURCE,
            source_batch_id="boxoffice-r1",
            dataset_revision=1,
            manifest_key="manifests/boxoffice/r1.json",
            cutoff="2026-09-10",
            content_sha256="2" * 64,
        ),
    ]


def reseal_result(result: ModelResultManifest, **changes) -> ModelResultManifest:
    changed = replace(result, **changes, result_manifest_id="")
    body = {
        "contract_version": 2,
        **{
            key: value
            for key, value in asdict(changed).items()
            if key != "result_manifest_id"
        },
    }
    return replace(changed, result_manifest_id=content_id("result", body))


def materialize_all(plan) -> tuple[dict[str, ModelResultManifest], ManifestCatalog]:
    """테스트 graph를 dependency 순으로 끝까지 계산한다."""
    catalog = register_plan(empty_manifest_catalog(), plan)
    by_model: dict[str, ModelResultManifest] = {}
    while len(by_model) < len(plan.build_slots):
        ready = scan_ready_cohorts(plan=plan, catalog=catalog)
        if not ready:
            raise AssertionError("테스트 graph가 더 진행되지 않습니다")
        for index, cohort in enumerate(ready, start=len(by_model) + 1):
            catalog = register_cohort(catalog, cohort)
            result = create_model_result(
                plan=plan,
                cohort=cohort,
                relation_id=f"CANDIDATE.{cohort.model_unique_id.rsplit('.', 1)[-1].upper()}__G1",
                row_count=index,
                content_sha256=f"{index:x}" * 64,
                catalog=catalog,
            )
            catalog = register_result(catalog, result)
            by_model[result.model_unique_id] = result
    return by_model, catalog


class ModelGenerationContractTests(unittest.TestCase):
    def test_gate_receipt_binds_generation_target_and_evidence(self) -> None:
        receipt = create_gate_receipt(
            gate_kind="SOURCE_GATE",
            generation_id="gen-000000000001",
            plan_id="plan-synthetic",
            deployment_id="d" * 64,
            owner_unique_id="source.pop_talk_dw.synthetic.input",
            source_snapshot_id="src-" + "a" * 64,
            executed_test_ids=("test.pop_talk_dw.source_not_null",),
            evidence_sha256="e" * 64,
        )
        self.assertTrue(receipt.gate_receipt_id.startswith("gate-"))
        with self.assertRaises(GenerationContractError):
            create_gate_receipt(
                gate_kind="DEPLOYMENT_GATE",
                generation_id="gen-000000000001",
                plan_id="plan-synthetic",
                deployment_id="d" * 64,
                owner_unique_id="dbt_deployment_validation",
                source_snapshot_id="src-" + "a" * 64,
                executed_test_ids=("test.pop_talk_dw.deployment",),
                evidence_sha256="e" * 64,
            )
    def setUp(self) -> None:
        self.graph = graph_snapshot_from_manifest(
            manifest(), deployment_id=DEPLOYMENT_ID
        )

    def previous_generation(self):
        plan = create_generation_plan(
            generation_sequence=1,
            graph=self.graph,
            source_snapshots=snapshots(),
            changed_resources=[],
            previous_results={},
            required_serving_components=["movie", "boxoffice"],
        )
        by_model, catalog = materialize_all(plan)
        return plan, by_model, catalog

    def test_movie_change_rebuilds_descendants_but_reuses_fact_branch(self) -> None:
        """source revision이 아니라 graph 후손 영향으로 재계산 범위를 정한다."""
        expected = {STG_MOVIE, DIM_MOVIE, MART_BOXOFFICE}
        self.assertEqual(
            affected_models(self.graph, [MOVIE_SOURCE]),
            expected,
        )
        previous_plan, previous, previous_catalog = self.previous_generation()
        plan = create_generation_plan(
            generation_sequence=2,
            graph=self.graph,
            source_snapshots=snapshots(movie_revision=2, movie_digest="6" * 64),
            changed_resources=[],
            previous_results=previous,
            required_serving_components=["movie", "boxoffice"],
            previous_plan=previous_plan,
            manifest_catalog=previous_catalog,
        )
        self.assertEqual(
            {model for model, slot in plan.slot_map.items() if slot.mode == "REBUILD"},
            expected,
        )
        self.assertEqual(plan.slot_map[FCT_BOXOFFICE].mode, "REUSE")
        self.assertEqual(
            plan.slot_map[FCT_BOXOFFICE].reuse_result_manifest_id,
            previous[FCT_BOXOFFICE].result_manifest_id,
        )
        # 최초 plan의 build slot에는 미래 relation/count/digest placeholder가 없다.
        for slot in plan.build_slots:
            self.assertFalse(hasattr(slot, "content_sha256"))
            self.assertFalse(hasattr(slot, "relation_id"))

    def test_reused_fact_binding_completes_new_generation_mart_cohort(self) -> None:
        """g2 mart는 g2 dim과 carry-forward된 g1 fact를 정확히 함께 읽는다."""
        previous_plan, previous, catalog = self.previous_generation()
        plan = create_generation_plan(
            generation_sequence=2,
            graph=self.graph,
            source_snapshots=snapshots(movie_revision=2, movie_digest="6" * 64),
            changed_resources=[],
            previous_results=previous,
            required_serving_components=["movie", "boxoffice"],
            previous_plan=previous_plan,
            manifest_catalog=catalog,
        )
        catalog = register_plan(catalog, plan)

        stg_cohort = create_model_cohort(
            plan=plan, model_unique_id=STG_MOVIE, catalog=catalog
        )
        assert stg_cohort is not None
        catalog = register_cohort(catalog, stg_cohort)
        stg_result = create_model_result(
            plan=plan,
            cohort=stg_cohort,
            relation_id="CANDIDATE.STG_MOVIE__G2",
            row_count=2,
            content_sha256="3" * 64,
            catalog=catalog,
        )
        catalog = register_result(catalog, stg_result)

        dim_cohort = create_model_cohort(
            plan=plan, model_unique_id=DIM_MOVIE, catalog=catalog
        )
        assert dim_cohort is not None
        catalog = register_cohort(catalog, dim_cohort)
        dim_result = create_model_result(
            plan=plan,
            cohort=dim_cohort,
            relation_id="CANDIDATE.DIM_MOVIE__G2",
            row_count=2,
            content_sha256="4" * 64,
            catalog=catalog,
        )
        catalog = register_result(catalog, dim_result)

        mart_cohort = create_model_cohort(
            plan=plan, model_unique_id=MART_BOXOFFICE, catalog=catalog
        )
        assert mart_cohort is not None
        bindings = {item.parent_unique_id: item for item in mart_cohort.parent_bindings}
        self.assertEqual(bindings[DIM_MOVIE].binding_kind, "RESULT")
        self.assertEqual(bindings[DIM_MOVIE].origin_generation_id, plan.generation_id)
        self.assertEqual(bindings[FCT_BOXOFFICE].binding_kind, "REUSED_RESULT")
        self.assertEqual(bindings[FCT_BOXOFFICE].origin_generation_id, "gen-000000000001")

    def test_scan_recovers_ready_cohorts_and_deduplicates_publication(self) -> None:
        """알림 순서와 무관하게 ledger scan은 준비된 미발행 cohort를 다시 찾는다."""
        plan = create_generation_plan(
            generation_sequence=1,
            graph=self.graph,
            source_snapshots=snapshots(),
            changed_resources=[MOVIE_SOURCE, BOXOFFICE_SOURCE],
            previous_results={},
            required_serving_components=[],
        )
        catalog = register_plan(empty_manifest_catalog(), plan)
        first_wave = scan_ready_cohorts(plan=plan, catalog=catalog)
        self.assertEqual(
            {item.model_unique_id for item in first_wave},
            {STG_MOVIE, STG_BOXOFFICE},
        )
        existing = [item.cohort_manifest_id for item in first_wave]
        self.assertEqual(
            scan_ready_cohorts(
                plan=plan,
                catalog=catalog,
                existing_cohort_ids=existing,
            ),
            (),
        )

    def test_asset_event_requires_real_partition_key(self) -> None:
        plan = create_generation_plan(
            generation_sequence=1,
            graph=self.graph,
            source_snapshots=snapshots(),
            changed_resources=[MOVIE_SOURCE, BOXOFFICE_SOURCE],
            previous_results={},
            required_serving_components=[],
        )
        catalog = register_plan(empty_manifest_catalog(), plan)
        cohort = create_model_cohort(plan=plan, model_unique_id=STG_MOVIE, catalog=catalog)
        assert cohort is not None
        catalog = register_cohort(catalog, cohort)
        result = create_model_result(
            plan=plan,
            cohort=cohort,
            relation_id="CANDIDATE.STG_MOVIE__G1",
            row_count=0,
            content_sha256="5" * 64,
            catalog=catalog,
        )
        catalog = register_result(catalog, result)
        event = build_model_ready_event(
            plan=plan, model_unique_id=STG_MOVIE, result=result, catalog=catalog
        )
        validated = validate_model_ready_event(
            event.__dict__,
            actual_partition_key=plan.generation_id,
            expected_model_unique_id=STG_MOVIE,
        )
        self.assertEqual(validated.row_count, 0)
        with self.assertRaisesRegex(GenerationContractError, "partition key"):
            validate_model_ready_event(
                event.__dict__,
                actual_partition_key="gen-999999999999",
                expected_model_unique_id=STG_MOVIE,
            )
        stale = replace(result, relation_id="OTHER.RELATION")
        with self.assertRaisesRegex(GenerationContractError, "권위 catalog"):
            build_model_ready_event(
                plan=plan,
                model_unique_id=STG_MOVIE,
                result=stale,
                catalog=catalog,
            )

    def test_public_boundaries_reject_forged_source_cohort_and_result(self) -> None:
        previous_plan, previous, catalog = self.previous_generation()
        forged_source = replace(
            snapshots(movie_revision=2, movie_digest="6" * 64)[0],
            content_sha256="9" * 64,
        )
        with self.assertRaisesRegex(GenerationContractError, "source snapshot content ID"):
            create_generation_plan(
                generation_sequence=2,
                graph=self.graph,
                source_snapshots=[forged_source, snapshots()[1]],
                changed_resources=[],
                previous_results=previous,
                previous_plan=previous_plan,
                manifest_catalog=catalog,
                required_serving_components=[],
            )

        plan = create_generation_plan(
            generation_sequence=2,
            graph=self.graph,
            source_snapshots=snapshots(movie_revision=2, movie_digest="6" * 64),
            changed_resources=[],
            previous_results=previous,
            previous_plan=previous_plan,
            manifest_catalog=catalog,
            required_serving_components=[],
        )
        catalog = register_plan(catalog, plan)
        stg_cohort = create_model_cohort(
            plan=plan, model_unique_id=STG_MOVIE, catalog=catalog
        )
        assert stg_cohort is not None
        forged_cohort = replace(stg_cohort, parent_bindings=())
        with self.assertRaisesRegex(GenerationContractError, "parent 집합"):
            register_cohort(catalog, forged_cohort)

        catalog = register_cohort(catalog, stg_cohort)
        valid_stg = create_model_result(
            plan=plan,
            cohort=stg_cohort,
            relation_id="CANDIDATE.STG_MOVIE__G2",
            row_count=1,
            content_sha256="7" * 64,
            catalog=catalog,
        )
        catalog = register_result(catalog, valid_stg)
        forged_result = replace(valid_stg, relation_id="OTHER.RELATION")
        forged_catalog = replace(
            catalog,
            results={**catalog.results, forged_result.result_manifest_id: forged_result},
        )
        with self.assertRaisesRegex(GenerationContractError, "content ID"):
            create_model_cohort(
                plan=plan,
                model_unique_id=DIM_MOVIE,
                catalog=forged_catalog,
            )

    def test_scan_rejects_wrong_plan_duplicate_and_catalog_alias(self) -> None:
        plan, _, catalog = self.previous_generation()
        final = next(item for item in catalog.results.values() if item.model_unique_id == MART_BOXOFFICE)
        wrong_plan = reseal_result(
            final,
            plan_id="plan-wrong",
            generation_id="gen-999999999999",
        )
        catalog_without_final = {
            key: value for key, value in catalog.results.items() if value is not final
        }
        with self.assertRaisesRegex(GenerationContractError, "GenerationPlan"):
            scan_ready_cohorts(
                plan=plan,
                catalog=replace(
                    catalog,
                    results={**catalog_without_final, wrong_plan.result_manifest_id: wrong_plan},
                ),
            )

        duplicate = reseal_result(
            final,
            relation_id="CANDIDATE.MART_BOXOFFICE__DUPLICATE",
            content_sha256="8" * 64,
        )
        with self.assertRaisesRegex(GenerationContractError, "상충 result"):
            validate_result_catalog(
                {**catalog.results, duplicate.result_manifest_id: duplicate}
            )
        with self.assertRaisesRegex(GenerationContractError, "catalog key"):
            validate_result_catalog({"result-alias": final})

    def test_rehashed_result_requires_original_cohort_and_parent_vector(self) -> None:
        plan, previous, catalog = self.previous_generation()
        valid = previous[STG_MOVIE]
        bad = reseal_result(
            valid,
            cohort_manifest_id="cohort-does-not-exist",
            parent_binding_ids=(),
        )
        independent_results = {
            key: value
            for key, value in catalog.results.items()
            if value.model_unique_id not in {STG_MOVIE, DIM_MOVIE, MART_BOXOFFICE}
        }
        independent_cohorts = {
            key: value
            for key, value in catalog.cohorts.items()
            if value.model_unique_id not in {DIM_MOVIE, MART_BOXOFFICE}
        }
        bad_catalog = replace(
            catalog,
            cohorts=independent_cohorts,
            results={**independent_results, bad.result_manifest_id: bad},
        )
        with self.assertRaisesRegex(GenerationContractError, "원래 cohort"):
            validate_result_provenance(bad.result_manifest_id, catalog=bad_catalog)
        with self.assertRaisesRegex(GenerationContractError, "원래 cohort"):
            build_model_ready_event(
                plan=plan,
                model_unique_id=STG_MOVIE,
                result=bad,
                catalog=bad_catalog,
            )

        with self.assertRaisesRegex(GenerationContractError, "원래 cohort"):
            create_generation_plan(
                generation_sequence=2,
                graph=self.graph,
                source_snapshots=snapshots(),
                changed_resources=[],
                previous_results={**previous, STG_MOVIE: bad},
                previous_plan=plan,
                manifest_catalog=bad_catalog,
                required_serving_components=[],
            )

        other_cohort = next(
            cohort
            for cohort in catalog.cohorts.values()
            if cohort.model_unique_id == STG_BOXOFFICE
        )
        wrong_existing_cohort = reseal_result(
            valid,
            cohort_manifest_id=other_cohort.cohort_manifest_id,
            parent_binding_ids=tuple(
                binding.manifest_id for binding in other_cohort.parent_bindings
            ),
        )
        wrong_catalog = replace(
            catalog,
            cohorts=independent_cohorts,
            results={
                **independent_results,
                wrong_existing_cohort.result_manifest_id: wrong_existing_cohort,
            },
        )
        with self.assertRaisesRegex(GenerationContractError, "identity"):
            validate_result_provenance(
                wrong_existing_cohort.result_manifest_id,
                catalog=wrong_catalog,
            )

    def test_cohort_ready_event_is_bound_to_model_and_partition(self) -> None:
        plan = create_generation_plan(
            generation_sequence=1,
            graph=self.graph,
            source_snapshots=snapshots(),
            changed_resources=[MOVIE_SOURCE, BOXOFFICE_SOURCE],
            previous_results={},
            required_serving_components=[],
        )
        catalog = register_plan(empty_manifest_catalog(), plan)
        cohort = create_model_cohort(plan=plan, model_unique_id=STG_MOVIE, catalog=catalog)
        assert cohort is not None
        catalog = register_cohort(catalog, cohort)
        event = build_cohort_ready_event(plan=plan, cohort=cohort, catalog=catalog)
        self.assertEqual(
            validate_cohort_ready_event(
                asdict(event),
                actual_partition_key=plan.generation_id,
                expected_model_unique_id=STG_MOVIE,
            ),
            event,
        )
        with self.assertRaisesRegex(GenerationContractError, "다른 model"):
            validate_cohort_ready_event(
                asdict(event),
                actual_partition_key=plan.generation_id,
                expected_model_unique_id=STG_BOXOFFICE,
            )
        with self.assertRaisesRegex(GenerationContractError, "partition"):
            validate_cohort_ready_event(
                asdict(event),
                actual_partition_key="gen-000000000002",
                expected_model_unique_id=STG_MOVIE,
            )

    def test_g1_result_can_be_reused_by_g2_and_g3_without_reinterpretation(self) -> None:
        g1, results_by_model, catalog = self.previous_generation()
        g2 = create_generation_plan(
            generation_sequence=2,
            graph=self.graph,
            source_snapshots=snapshots(),
            changed_resources=[],
            previous_results=results_by_model,
            previous_plan=g1,
            manifest_catalog=catalog,
            required_serving_components=[],
        )
        self.assertTrue(all(slot.mode == "REUSE" for slot in g2.build_slots))
        catalog = register_plan(catalog, g2)
        g3 = create_generation_plan(
            generation_sequence=3,
            graph=self.graph,
            source_snapshots=snapshots(),
            changed_resources=[],
            previous_results=results_by_model,
            previous_plan=g2,
            manifest_catalog=catalog,
            required_serving_components=[],
        )
        self.assertTrue(all(slot.mode == "REUSE" for slot in g3.build_slots))
        catalog = register_plan(catalog, g3)
        ready = build_model_ready_event(
            plan=g3,
            model_unique_id=STG_MOVIE,
            result=results_by_model[STG_MOVIE],
            catalog=catalog,
        )
        self.assertEqual(ready.generation_id, "gen-000000000003")
        self.assertEqual(ready.origin_generation_id, "gen-000000000001")

    def test_g3_rejects_caller_object_that_impersonates_unbuilt_g2_result(self) -> None:
        g1, results_by_model, catalog = self.previous_generation()
        g2 = create_generation_plan(
            generation_sequence=2,
            graph=self.graph,
            source_snapshots=snapshots(movie_revision=2, movie_digest="6" * 64),
            changed_resources=[],
            previous_results=results_by_model,
            previous_plan=g1,
            manifest_catalog=catalog,
            required_serving_components=[],
        )
        catalog = register_plan(catalog, g2)
        g1_staging = results_by_model[STG_MOVIE]
        forged_caller = replace(
            g1_staging,
            plan_id=g2.plan_id,
            generation_id=g2.generation_id,
            build_id=g2.slot_map[STG_MOVIE].build_id,
            # result_manifest_id는 정상 g1 ID를 그대로 두어 catalog lookup을 속이려 한다.
        )
        with self.assertRaisesRegex(GenerationContractError, "권위 catalog 객체"):
            create_generation_plan(
                generation_sequence=3,
                graph=self.graph,
                source_snapshots=snapshots(movie_revision=2, movie_digest="6" * 64),
                changed_resources=[],
                previous_results={**results_by_model, STG_MOVIE: forged_caller},
                previous_plan=g2,
                manifest_catalog=catalog,
                required_serving_components=[],
            )

    def test_cycle_and_missing_source_snapshot_are_rejected(self) -> None:
        bad = manifest()
        bad["nodes"][STG_MOVIE]["depends_on"]["nodes"] = [DIM_MOVIE]
        with self.assertRaisesRegex(GenerationContractError, "cycle"):
            graph_snapshot_from_manifest(bad, deployment_id=DEPLOYMENT_ID)
        with self.assertRaisesRegex(GenerationContractError, "source snapshot"):
            create_generation_plan(
                generation_sequence=1,
                graph=self.graph,
                source_snapshots=snapshots()[:1],
                changed_resources=[MOVIE_SOURCE],
                previous_results={},
                required_serving_components=[],
            )

    def test_unsupported_seed_and_ephemeral_model_are_rejected(self) -> None:
        seeded = manifest()
        seeded["nodes"]["seed.pop_talk_dw.input"] = {
            "resource_type": "seed",
            "depends_on": {"nodes": []},
        }
        seeded["nodes"][STG_MOVIE]["depends_on"]["nodes"] = [
            "seed.pop_talk_dw.input"
        ]
        with self.assertRaisesRegex(GenerationContractError, "지원하지 않는"):
            graph_snapshot_from_manifest(seeded, deployment_id=DEPLOYMENT_ID)

        ephemeral = manifest()
        ephemeral["nodes"][STG_MOVIE]["config"] = {"materialized": "ephemeral"}
        with self.assertRaisesRegex(GenerationContractError, "ephemeral"):
            graph_snapshot_from_manifest(ephemeral, deployment_id=DEPLOYMENT_ID)


if __name__ == "__main__":
    unittest.main()
