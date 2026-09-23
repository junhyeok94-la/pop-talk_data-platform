# 모델 단위 DAG 분할 단계 A 구현 기록

## 1. 범위

승인된 `model-granular-dag-split-plan-2026-09-10.md`의 단계 A 가운데 외부 시스템을
변경하지 않는 순수 계약과 manifest registry를 구현했다. 아직 신규 DAG, Snowflake relation,
S3 manifest 또는 PostgreSQL publication을 생성하지 않았다. 기존 전체 DAG도 변경하거나
실행하지 않았다.

## 2. 구현 파일

### `pipelines/orchestration/model_generation_contract.py`

다음 immutable value와 상태 전이 함수를 추가했다.

- `GraphSnapshot`: 승인 dbt manifest의 model/source 직접 의존 그래프와 digest
- `SourceSnapshot`: source batch, 독립 dataset revision, cutoff, manifest와 content digest
- `GenerationPlan`: 미래 결과 digest 없이 generation, graph, source와 REBUILD/REUSE slot 고정
- `ModelCohortManifest`: 한 모델 실행 전에 exact source/result/carry-forward 부모 vector 고정
- `ModelResultManifest`: 모델 실행·소유 test·relation digest 검증 뒤 물리 결과 봉인
- `ModelReadyEventV2`: 실제 Airflow `partition_key == generation_id`를 요구하는 작은 event
- `ManifestCatalog`: plan/cohort/result 사이의 원래 provenance를 재귀 대사하는 권위 집합

주요 함수는 다음 계약을 구현한다.

- manifest graph cycle/dangling parent 거부
- 공개 dataclass를 역직렬화한 모든 소비 경계에서 content ID와 의미 구조 재검증
- result 자체 hash와 별개로 원래 GenerationPlan, 실재 Cohort, exact parent vector 및 모든
  조상 result provenance를 재귀 검증
- plan 생성 시 caller가 전달한 previous plan/result 객체를 catalog 권위 객체와 완전 대사하고,
  이후 REUSE 판단에는 catalog에서 반환된 객체만 사용
- source snapshot 변경을 이전 plan과 직접 비교해 변경 source 자동 추론
- 변경 source/model에서 후손을 전이 탐색해 REBUILD 결정
- 이전 plan의 실제 slot에 속하고 같은 deployment/model인 result만 REUSE 허용
- 신규 result event가 없는 이전 generation result도 carry-forward binding으로 부모 readiness 인정
- durable ledger 재스캔을 가정해 현재 준비된 미발행 cohort 0..N개 결정
- 같은 build slot의 복수 result와 stale/다른 generation READY 거부
- 0행 result를 실패가 아닌 유효한 봉인 결과로 표현

계약 실행 순서는 다음과 같다.

```text
GenerationPlan
  -> 부모 ModelResult/carry-forward binding으로 ModelCohortManifest 봉인
  -> 정확한 모델 실행 + 소유 test
  -> ModelResultManifest 봉인
  -> 자식 cohort scan
  -> generation-partitioned READY event
```

### `pipelines/orchestration/dbt_model_registry.py`

승인된 불변 dbt manifest에서 다음을 전수 대사한다.

- model별 exact `fqn:` selector
- model/source 직접 부모
- model별 소유 test unique ID와 exact test `fqn:` selector
- source test의 source gate 소유권
- 부모 없는 test의 deployment gate 소유권
- 다중 부모 test의 명시적 model/quality gate 소유권
- manifest에 없는 stale owner override 거부

현재 다중 부모 `assert_mart_boxoffice_preserves_fact_grain`은
`mart_boxoffice_daily` gate가 소유하고, 부모 없는
`assert_revision_precedes_completion_time`은 deployment gate가 소유한다. 나머지 단일 model/source
test는 부모 gate에 자동 배정한다.

작업 디렉터리의 과거 `pipelines/dbt/target/manifest.json`은 사용하지 않는다. 테스트는
`pipelines/dbt_deployments/current.json`이 가리키는 content-addressed manifest를 읽는다.

## 3. 테스트

추가 파일:

- `pipelines/orchestration/tests/test_model_generation_contract.py`
- `pipelines/orchestration/tests/test_dbt_model_registry.py`

검증한 주요 반례:

1. movie source만 바뀌면 `stg_movie`, `dim_movie`, `mart_boxoffice`는 REBUILD하지만
   boxoffice staging/fact는 REUSE한다.
2. g2 mart cohort가 g2 dim result와 g1 carry-forward fact result를 함께 고정한다.
3. source가 둘 다 준비된 최초 wave만 staging cohort 2개로 나오며 이미 발행한 cohort는
   재스캔에서 중복되지 않는다.
4. Asset extra의 generation이 맞아도 실제 partition key가 다르면 거부한다.
5. stale result를 새 generation REBUILD READY로 위장할 수 없다.
6. graph cycle과 source snapshot 누락을 거부한다.
7. current immutable deployment의 실제 model 7개와 test 43개가 누락·중복 없이 소유된다.
8. 다중 부모/부모 없는 test는 명시 owner 없이는 registry 생성이 실패한다.
9. source/cohort/result 필드를 `dataclasses.replace`로 바꾸고 기존 ID를 유지한 위조 입력을
   공개 소비 경계에서 거부한다.
10. 다른 plan/generation result가 같은 build ID를 주장해 정상 실행 완료를 대신하거나,
    자식 없는 마지막 모델에 상충 result가 두 개 생기는 경우를 거부한다.
11. seed/snapshot 부모와 ephemeral 모델을 조용히 누락하지 않고 배포 단계에서 거부한다.
12. 다중 부모 test를 모든 입력을 준비할 수 없는 fact gate에 배정하는 경우를 거부한다.
13. result를 올바르게 재해시해도 존재하지 않는 cohort, 다른 모델 cohort 또는 누락된 parent
    vector를 가리키면 완료·READY·REUSE·자식 cohort에서 거부한다.
14. 정상 g1 result를 무변경 g2와 g3가 연속 REUSE할 때 소비 generation은 g3, 원본 result
    generation은 g1로 유지한다.
15. g2 변경 모델을 실행하지 않은 상태에서 caller가 g1 result ID를 유지하고 g2 plan/build처럼
    위장해도 g3가 stale g1 결과를 REUSE하지 못한다.

실행 결과:

```text
python -m unittest discover -s pipelines -p "test_*.py"
Ran 122 tests
OK
```

Airflow 컨테이너의 read-only project mount에는 `compileall`이 `__pycache__`를 쓸 수 없으므로,
bytecode write 없이 orchestration Python 15개를 `ast.parse`로 검사했고 모두 성공했다.

## 4. 의도적으로 아직 하지 않은 일

- 실제 durable generation/cohort ledger 저장 adapter와 테이블 DDL
- Airflow `PartitionedAssetTimetable` 및 `add_partitions(generation_id)` 적용
- Cosmos 단일 모델 DAG factory와 owned-test 실행 operator
- generation 변수를 읽는 dbt SQL/materialization
- Snowflake/Databricks/S3/serving DAG 분할

이 항목들은 단계 A 계약 검토 승인 뒤 구현한다. 특히 실제 DAG factory는 단계 B의 새
generation-aware dbt 모델명과 source 분할이 확정되기 전에 생성하면 registry가 즉시 낡기
때문에, 이번 검토에서는 공통 `ModelDagSpec`과 exact model/test selector까지만 고정했다.

## 5. 검토 요청

1. `GenerationPlan -> 부모 cohort -> result -> 자식 cohort` 순서가 코드에서 지켜지는가?
2. caller가 changed source를 누락해도 source snapshot 비교가 후손 rebuild를 강제하는가?
3. 이전 generation result의 REUSE slot 검증과 carry-forward binding이 충분히 엄격한가?
4. `scan_ready_cohorts`가 durable store adapter의 0..N recovery 기반으로 적합한가?
5. current deployment의 43개 test ownership과 exact selector 계약에 누락이 없는가?
6. 실제 저장 adapter와 Airflow factory를 구현하기 전에 보완할 계약이 있는가?
