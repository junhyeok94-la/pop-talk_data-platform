# 모델 단위 DAG 단계 A 재검토

판정: **CHANGES_REQUESTED — result의 원래 cohort/부모 의미 검증 1건 남음**.

최신 두 모듈, 추가 테스트와 구현 기록을 확인했다. Airflow 컨테이너에서 pipelines 전체 **119개 테스트가 통과**했다(0.237초). 정상 g1 결과를 g2, 다시 g3에서 REUSE하는 경로도 독립 모형으로 실행해 성공했다. READY의 소비 generation은 각각 g2/g3이며 원본 result generation은 g1으로 유지됐다. 실제 DAG나 외부 데이터는 변경하지 않았다.

## 기존 수정 수용

- source/plan/cohort/result 본문을 바꾸고 기존 content ID를 유지하는 반례는 새 validator로 차단된다.
- cohort 생성/결과 생성 시 정확한 직접 부모와 catalog binding을 비교하는 보완을 수용한다.
- scan의 전체 identity 검사와 전역 conflicting result/catalog key 검사를 수용한다.
- seed/snapshot/ephemeral 및 미지원 부모 거부, MODEL_GATE의 자기/ancestor 입력 범위 검사와 source/owner kind 검사를 수용한다.
- 기본 current manifest의 7 models/43 tests ownership 검사는 전체 suite에서 통과했다.

## [P1] 재해시된 result가 존재하지 않는 cohort/누락된 부모를 가리켜도 완료·재사용됨

대상: `validate_model_result`, `validate_result_catalog`, `_result_compatible_for_reuse`, `build_model_ready_event` 및 result를 소비하는 cohort 경계.

현재 result validator는 본문과 content ID의 일치 및 기본 형식만 검사한다. result의 `cohort_manifest_id`가 실제 존재하는지, 그 cohort가 원래 plan의 실행 가능한 cohort인지, `parent_binding_ids`가 그 cohort의 부모 vector와 같은지는 확인하지 않는다. create_model_result의 정상 생성 경로에서는 검사하지만 저장소에서 읽은 공개 dataclass에는 그 보장이 없다.

독립 재현:

```python
bad = replace(valid_stg_result,
    cohort_manifest_id='cohort-does-not-exist', parent_binding_ids=())
bad = replace(bad,
    result_manifest_id=content_id('result', _model_result_body(bad)))
```

위 결과는 `validate_model_result`를 통과했다. 같은 result를 catalog에 넣으면 dim의 cohort가 이를 부모로 채택했고, staging READY 생성도 성공했다. 이전 정상 plan과 함께 next plan을 만들면 이 result를 **REUSE**했다. 이번 반례는 단순 hash 불일치가 아니라 hash와 본문이 일치하지만 의미상 유효하지 않은 lineage다. content-addressed ID는 자체적으로 원래 실행 증거를 제공하지 않는다.

수정: 결과의 원래 plan/cohort를 조회할 수 있는 권위 catalog 또는 resolver를 검증 경계에 제공한다. result의 plan/generation/deployment/model/build/cohort identity와 정확한 parent vector를 원래 cohort에 대조하고, 그 cohort도 원래 plan과 parent result/source에 대해 검증한다. g2/g3 REUSE는 현재 plan으로 g1 결과를 재해석하지 않고 g1의 provenance를 검증한 뒤 slot binding을 확인해야 한다. 완료 판정·자식 cohort·READY·REUSE 모두 이 검증을 통과한 결과만 소비하도록 한다.

내용 hash 검증과 provenance 검증을 별도 함수로 나누는 것은 가능하다. 다만 현재 `validate_model_result` 통과를 권위 result 검증 완료로 취급하지 않도록 API와 문서에서 구분해야 한다. 없는 cohort뿐 아니라 실제 존재하지만 다른 source 부모를 가진 cohort로 바꾸고 재해시한 경우도 거부해야 한다.

필수 회귀 확인:

- 재해시한 nonexistent cohort/누락 parent result를 완료·READY·REUSE·자식 cohort에서 거부한다.
- 실제 다른 cohort를 참조하거나 parent vector만 다른 재해시 result를 거부한다.
- 정상 g1→g2→g3 REUSE와 g2 dim+g1 fact binding은 계속 성공한다.

실제 relation digest와 test 실행 성공, 신뢰할 저장소의 immutable/원자 commit 증명은 미래 durable adapter 범위다. 그러나 저장된 manifest 간 논리 연결 검증은 이번 순수 계약에서도 제공해야 한다. 남은 항목 외 기존 지적은 수용하며 업무 코드는 수정하지 않았다.
