# 단계 A catalog 도입 후 검토

판정: **CHANGES_REQUESTED — create_generation_plan의 caller/catalog 객체 불일치 1건**.

최신 catalog/register/provenance 구현과 추가 fixture를 확인하고 전체 pipelines 테스트를 독립 실행했다. **121개 모두 통과**했다(0.700초). 기존 nonexistent cohort, 다른 실제 cohort, 누락 부모 vector의 재해시 반례를 막는 재귀 검증과 정상 g1→g2→g3 재사용은 수용한다. 업무 코드·외부 데이터는 변경하지 않았다.

## [P1] catalog 객체를 검증한 뒤 다른 caller 객체로 REUSE를 결정함

위치: `model_generation_contract.py`의 `create_generation_plan`, previous_results 검증 루프 및 `_result_compatible_for_reuse` 호출.

`validate_result_provenance(result.result_manifest_id, catalog=...)`는 catalog에 저장된 객체를 검증한다. 하지만 반환값을 사용하지 않고 caller가 전달한 result를 이후 compatibility 검사에 사용한다. 둘의 equality 또는 caller 객체 content ID를 검사하지 않는다. READY 경계에는 같은 객체인지 대사가 있으나 이 경계에는 빠져 있다.

독립 재현:

1. 정상 g1의 모든 결과를 생성하고 catalog에 등록한다.
2. movie source revision/content가 변경된 g2 plan을 생성·등록한다. movie staging/dim/mart는 REBUILD이고 아직 실행하지 않는다.
3. caller의 previous_results에서 g1 result ID는 유지하면서 `plan_id`, `generation_id`, `build_id`만 g2 slot 값으로 `dataclasses.replace`한다. catalog는 정상 g1 객체 그대로 둔다.
4. g2와 동일한 source snapshot으로 g3 plan을 생성한다.

결과: g2에서 아직 계산하지 않은 movie staging이 g3에서 **REUSE(g1 result ID)**로 확정됐다. g3 plan 등록과 g1 result의 g3 READY 발행도 성공했다(origin_generation_id=g1). 변경 source를 처리하지 않은 과거 결과가 정상 결과처럼 재사용되는 데이터 정확성 문제다.

수정: provenance 검증이 반환한 권위 result와 caller result의 완전한 일치를 검사하거나, caller는 ID만 제공하고 이후 compatibility/slot 생성에는 catalog에서 조회한 객체만 사용한다. 동일한 원칙으로 previous_plan도 권위 catalog의 등록 객체와 대사한다. catalog를 검증한 뒤 caller의 중복 객체를 신뢰하는 경계를 남기지 않는다.

회귀 테스트는 단순 relation 변경뿐 아니라 위 **g2 변경 미처리→g3 stale g1 REUSE** 시나리오를 포함한다. caller/catalog 불일치는 plan 생성에서 거부되어야 하며, 정상 무변경 g1→g2→g3 REUSE와 g2 dim+g1 fact는 계속 통과해야 한다.

이 항목 외 기존 지적은 수용한다. 실제 저장소 commit/권한, relation/test 증명, outbox, partitioned DAG 및 Cosmos 실행은 후속 adapter 검증 범위로 유지한다.
