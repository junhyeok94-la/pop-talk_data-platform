# 레이어·모델 단위 DAG 분할 계획

## 1. 결론

현재 `pop_talk_movie_databricks_daily` 한 DAG에 Raw 소비, Databricks
Bronze/Silver, S3 Exchange, Snowflake 적재, dbt 전체 그래프, PostgreSQL 게시가
모두 들어 있다. Cosmos가 dbt 모델을 태스크별로 표시하더라도
`confirm_loaded_batch` 뒤에서만 전체 dbt 그래프가 시작하므로 레이어 사이에는 전역
장벽이 존재한다.

목표 구조는 **하나의 업무 모델을 하나의 실행·재시도·게시 단위로 갖는 DAG**이다.
각 DAG는 직접 선행 모델이 발행한 Airflow Asset만 기다린다. 따라서 영화 차원에 필요한
Silver가 준비되면 박스오피스 계열을 기다리지 않고 `dim_movie`를 계산할 수 있다.

레이어 전체 완료 장벽은 두지 않는다. 여러 결과를 한 서비스 스냅샷으로 공개해야 하는
마지막 PostgreSQL publication 경계에만 의도적인 일관성 장벽을 둔다.

단, Asset event를 곧바로 모델 입력으로 간주하지 않는다. 검토에서 확인한 것처럼 Airflow의
일반 Asset AND는 동일 배치 join이 아니고 현재 Snowflake view/table도 실행 세대에 고정되어
있지 않다. 수정 설계는 **Asset partition + durable cohort ledger + generation별 불변 relation**을
함께 사용한다.

## 2. 현재 구조의 문제

현재 실행 관계는 다음과 같다.

```text
Raw READY 묶음
  -> Databricks Bronze/Silver 묶음
  -> S3 Exchange 영화+박스오피스 묶음
  -> Snowflake 두 observation+ledger 단일 트랜잭션
  -> confirm_loaded_batch (전역 장벽)
  -> Cosmos dbt 7개 모델 전체 그래프
  -> AFTER_ALL 전체 테스트
  -> PostgreSQL 게시
```

dbt 내부의 `dim_movie`와 `fct_boxoffice_daily`는 병렬 실행할 수 있지만, 두 모델 모두
모든 Raw 입력의 Snowflake 적재가 끝날 때까지 시작하지 못한다. 실패·재실행 단위도
`pop_talk_movie_databricks_daily`라는 큰 DAG run에 묶인다. 영화 차원만 다시 만들거나
특정 모델의 SLA·실패 원인을 확인하기 어렵다.

## 3. 분할·세대 일관성 원칙

### 3.1 실행 식별자

서로 다른 의미를 한 `batch_id`에 섞지 않는다.

| 식별자 | 발급자 | 의미 |
| --- | --- | --- |
| `source_batch_id` | Raw 수집 DAG | API 호출과 S3 Raw manifest 한 묶음 |
| `dataset_revision` | 각 데이터셋 loader | 같은 source batch를 정정·재게시한 독립 revision |
| `generation_id` | generation coordinator | downstream이 읽을 누적 snapshot과 부모 revision vector를 고정한 단조 증가 세대 |
| `deployment_id` | dbt 배포 도구 | SQL/YAML/macro/manifest의 불변 graph release |
| `publication_id` | serving coordinator | 한 서비스 snapshot에 포함할 component 집합의 content-addressed ID |

generation coordinator는 신규 source dataset READY를 ledger에 등록한 뒤 트랜잭션 안에서
증가하는 `generation_sequence`를 예약한다. 완료 시각이 아니라 **등록된 source watermark와
부모 revision vector**가 순서를 결정한다. 늦게 끝난 과거 source가 새 generation을 만들 수는
있지만 더 오래된 cutoff로 현재 세대를 되돌릴 수 없다.

manifest는 생성 시점이 다른 세 종류로 나눈다. 먼저 만드는 불변 `GenerationPlan`은 아직
계산되지 않은 결과 digest를 요구하지 않는다. 다음을 포함한다.

- `generation_id`, `generation_sequence`, `created_at`
- 모델 graph `deployment_id`
- 도메인별 source cutoff와 입력 manifest/digest 목록
- manifest 의존 그래프 snapshot과 required model/build slot 목록
- 모델별 `REBUILD(build_id)` 또는 `REUSE(previous_result_manifest_id)` 결정
- 새 generation의 각 논리적 parent slot에서 실제 immutable result로 향하는 carry-forward binding
- 최종 서비스 publication의 required component 목록

각 모델이 성공하면 별도의 불변 `ModelResultManifest`를 추가한다. 여기에는 build slot,
generation, 실제 relation ID, row count, canonical digest, deployment와 exact parent result
manifest vector가 들어간다. 직접 부모의 result 또는 carry-forward binding이 모두 확정된 뒤
별도의 불변 `ModelCohortManifest`를 생성한다. Cohort는 최초 GenerationPlan을 참조하지만 이를
수정하지 않는다.

영화 r2, 박스오피스 source r1처럼 독립 source revision은 허용한다. 다만 재사용은 source
도메인명이 아니라 승인된 manifest dependency graph의 변경 영향으로 판단한다. 변경 source에서
시작해 모든 후손을 표시하고, dependency가 하나라도 바뀐 모델은 다시 계산한다. deployment,
모델 계약과 exact parent result vector가 모두 같은 모델만 이전 `ModelResultManifest`를
carry-forward할 수 있다. 0행도 성공 result로 기록하며 무변경과 실패를 구분한다.

예를 들어 movie title/policy만 바뀌고 boxoffice source가 그대로라면
`fct_boxoffice_daily`는 재사용할 수 있지만 `mart_boxoffice_daily`는 새 `dim_movie`를 참조하므로
재계산한다. 새 generation의 mart parent slot은 g2 dim result와 g1 fact result를 각각 가리킨다.
여기서 generation은 소비 build 세대이며, 물리 부모 result의 원래 generation이 모두 같다는
뜻이 아니다.

### 3.2 모델과 Asset

1. DAG 하나는 업무 모델 하나만 생성하고 output Asset 하나를 성공 시 발행한다.
2. Raw fan-out, generation/cohort 준비, 최종 activation은 데이터 모델을 만들지 않는 작은
   control-plane DAG 예외다.
3. 소비 DAG는 직접 부모만 사용하되 여러 부모의 일반 Asset AND event를 직접 join하지 않는다.
4. Airflow 3.3.1 `PartitionedAssetTimetable`과 `generation_id` partition key를 사용해 서로 다른
   세대가 같은 DagRun으로 합쳐지지 않게 한다.
5. scheduler 판단만 신뢰하지 않고 durable cohort ledger에서 그 generation build slot의
   required parent result 또는 carry-forward binding이 모두 있는지 다시 확인한다. 준비된 cohort 하나를 가리키는 단일
   `cohort.<model>.ready` Asset이 실제 모델 DAG를 깨운다.
6. 중복·역순 event와 scheduler가 한 번에 전달한 여러 event는 모두 pending ledger에 upsert한다.
   incomplete cohort는 실패로 소비하지 않고 pending으로 유지한다. 새 parent 도착 또는 명시적
   replay가 readiness coordinator를 다시 깨운다.
7. Asset event에는 generation, cohort manifest key, relation ID, 행 수와 digest만 넣고 비밀값과
   데이터 본문은 넣지 않는다. Asset은 wake-up이며 ledger/manifest가 권위 원장이다.
8. 모델 DAG는 `(model, generation_id, deployment_id)` 멱등 key를 소유한다. 같은 입력 재실행은
   기존 결과 digest를 대사해 성공 no-op하고 필요한 Asset event를 안전하게 재발행한다.
9. 독립 branch는 동시에 진행한다. 공유 warehouse/API는 Airflow pool과 대상 ledger/lock으로
   제어한다.

### 3.3 generation별 불변 데이터

공유 view나 현재 상태 table을 downstream 입력으로 사용하지 않는다. Snowflake 모델 결과는
`generation_id`와 `deployment_id`가 포함된 append-only relation에 저장한다. 구현 기본안은
모델별 물리 테이블에 generation column을 두고, 최초 성공 행 집합을 immutable component로
등록하는 방식이다. 재실행은 같은 generation partition을 덮어쓰지 않고 canonical digest가
같은지 확인한다. digest가 다르면 새 generation/deployment가 필요하다.

dbt에는 `generation_id`와 generation manifest를 명시적으로 전달한다. source와 `ref()`는
manifest가 고정한 부모 generation/relation만 필터링한다. 사람이 조회하는 `*_CURRENT` view는
별도로 둘 수 있지만 downstream dbt 모델은 이를 참조하지 않는다.

모델 결과 READY에는 result manifest ID, relation, generation row count, canonical digest와 exact parent vector를
기록한다. downstream SQL, test, export는 모두 동일 relation snapshot을 사용한다. current
pointer 갱신은 generation sequence compare-and-swap과 model writer lock 아래에서만 수행한다.
따라서 g1 Asset 발행 후 g2가 완료돼도 늦은 g1 mart는 g1 부모만 읽는다.

### 3.4 dbt test 소유권

dbt model selector와 test selector를 암묵적으로 결합하지 않는다. immutable manifest의 모든
test unique ID를 다음 registry 중 하나에 정확히 한 번 배정한다.

- 단일 부모 schema/data test: 해당 모델 DAG의 output 발행 전 gate
- 다중 부모 singular test: 해당 부모 cohort가 준비된 전용 quality/test 모델 DAG
- source test: 그 source를 게시한 Snowflake load DAG의 READY 발행 전 gate
- 부모 없는 macro/fixture 계약 test: deployment build CI gate

Cosmos 모델 DAG는 `TestBehavior.NONE`으로 정확한 모델 하나만 실행하고, registry가 정한 test
unique ID만 별도 명령 태스크로 실행하는 것을 기본안으로 한다. source, model, test unique ID의
누락·중복 및 manifest `depends_on.nodes`와 registry/cohort의 불일치를 구조 테스트에서 막는다.
ephemeral/seed/snapshot이 registry에 나타나면 지원 구현 전까지 배포를 명시적으로 거부한다.

### 3.5 Serving publication

PostgreSQL 활성 publication 교체만 서비스 전체 일관성 장벽이다. 영화/박스오피스 게시 DAG는
서로를 기다리지 않고 content-addressed `component_id` 아래 candidate rows, generation,
result manifest, digest와 count를 먼저 적재·봉인한다. 이 단계에는 아직 전역 publication FK가
필요하지 않다.

required component가 모두 준비되면 coordinator가 GenerationPlan과 정렬된 component ID 목록의
digest로 최종 `publication_id`를 만들고 header 및 component junction을 한 번에 삽입한다.
activation은 row lock 아래 required component completeness, parent vector, deployment
compatibility, count/digest를 다시 계산한 후 active pointer 하나만 원자 교체한다. 따라서
boxoffice digest가 없어도 movie staging→dim→movie candidate component 적재가 진행되며, 과거
generation의 지연 component가 나중에 완료돼도 sequence CAS가 활성화를 거부한다.

## 4. 목표 DAG와 Asset 그래프

### 4.0 Control plane

| DAG | 역할 | 출력 |
| --- | --- | --- |
| `pop_talk_register_source_change` | 단일 source READY를 durable ledger에 등록하고 누적 cutoff/revision vector로 새 generation을 예약 | generation manifest |
| `pop_talk_prepare_model_cohort` 계열 factory | 모델별 직접 부모 event를 `(generation_id, parent_model)`로 upsert하고 완성 cohort만 READY 처리 | partitioned `cohort.<model>.ready` Asset |

control DAG는 여러 업무 모델을 계산하지 않는다. event를 놓쳐도 source/model ledger를 다시
scan하여 아직 소비되지 않은 완성 cohort 0..N개를 찾아 재발행할 수 있다. 정상 비동기 도착을
실패 DagRun이나 수동 backfill로 처리하지 않는다.

### 4.1 Raw

기존 `pop_talk_movie_raw_daily`는 원천 API 호출과 불변 S3 Raw/manifest 게시 역할을
유지한다. 수집 결과에서 도메인별 준비 이벤트를 별도로 발행한다.

| DAG | 출력 Asset |
| --- | --- |
| `pop_talk_movie_raw_daily` | `raw.movie_metadata.ready`, `raw.boxoffice_daily.ready` |

두 Asset은 같은 Raw run의 `source_batch_id`를 공유하지만, 해당 도메인 검증이 성공한 시점에 각각
발행할 수 있다. 영화 메타데이터 branch의 KOBIS/KMDb 상세와 박스오피스 branch가 서로를
불필요하게 기다리지 않는다.

### 4.2 Databricks Bronze

| DAG | 직접 입력 | 출력 Asset |
| --- | --- | --- |
| `pop_talk_bronze_movie_observation` | `raw.movie_metadata.ready` | `bronze.movie_observation.ready` |
| `pop_talk_bronze_boxoffice_observation` | `raw.boxoffice_daily.ready` | `bronze.boxoffice_observation.ready` |

기존 단일 `03_movie_bronze_silver_daily.py` 노트북은 공통 helper와 두 개의 얇은 모델
entrypoint로 분리한다. 각 Bronze DAG는 자기 Delta 테이블과 처리 ledger만 변경한다.

### 4.3 Databricks Silver와 S3 Exchange

| DAG | 직접 입력 | 출력 Asset |
| --- | --- | --- |
| `pop_talk_silver_movie_observation` | `bronze.movie_observation.ready` | `silver.movie_observation.ready` |
| `pop_talk_silver_boxoffice_observation` | `bronze.boxoffice_observation.ready` | `silver.boxoffice_observation.ready` |

각 Silver DAG는 정제 Delta 결과와 데이터셋별 S3 Exchange manifest를 게시한 뒤 Asset을
발행한다. 기존 영화+박스오피스 합본 `EXCHANGE_READY`는 다음 v2 계약에서 데이터셋별
READY로 분리한다. 한쪽 실패가 다른 쪽의 성공 artifact를 무효화하지 않는다.

### 4.4 Snowflake source/staging

| DAG | 직접 입력 | 생성 모델/테이블 | 출력 Asset |
| --- | --- | --- | --- |
| `pop_talk_sf_load_movie_observation` | `silver.movie_observation.ready` | `STAGING.MOVIE_OBSERVATIONS_RAW`, 데이터셋 ledger | `sf_source.movie_observation.ready` |
| `pop_talk_sf_load_boxoffice_observation` | `silver.boxoffice_observation.ready` | `STAGING.BOXOFFICE_OBSERVATIONS_RAW`, 데이터셋 ledger | `sf_source.boxoffice_observation.ready` |
| `pop_talk_dbt_stg_movie_observation` | `sf_source.movie_observation.ready` | `stg_movie_observations` | `dbt.stg_movie_observations.ready` |
| `pop_talk_dbt_stg_boxoffice_observation` | `sf_source.boxoffice_observation.ready` | `stg_boxoffice_observations` | `dbt.stg_boxoffice_observations.ready` |

현재 공통 `stg_successful_exchange_publications`가 두 도메인을 결합시키므로
`stg_movie_publications`와 `stg_boxoffice_publications`로 분리하거나 각 staging 모델 안에서
도메인 ledger를 직접 참조한다. 성공 publication 선택은 데이터셋마다 독립적이어야 한다.

### 4.5 Gold/DW와 Mart

직접 부모가 하나뿐인 모델도 generation manifest를 만들고, 부모가 둘 이상이면 작은
`pop_talk_prepare_<model>_cohort` control DAG가 parent event를 durable ledger에 등록한 뒤
동일 generation의 완성 cohort READY를 발행한다. 모델 DAG는 이 단일 READY를 소비한다.

| 모델 DAG | 권위 입력 cohort | 생성 모델 | 출력 Asset |
| --- | --- | --- | --- |
| `pop_talk_dbt_dim_movie` | `cohort.dim_movie.ready` | `dim_movie` | `gold.dim_movie.ready` |
| `pop_talk_dbt_fct_boxoffice_daily` | `cohort.fct_boxoffice_daily.ready` | `fct_boxoffice_daily` | `gold.fct_boxoffice_daily.ready` |
| `pop_talk_dbt_mart_boxoffice_daily` | `cohort.mart_boxoffice_daily.ready` | `mart_boxoffice_daily` | `mart.boxoffice_daily.ready` |
| `pop_talk_dbt_mart_movie_quality` | `cohort.mart_movie_quality.ready` | 영화 품질 mart | `mart.movie_quality.ready` |
| `pop_talk_dbt_mart_boxoffice_quality` | `cohort.mart_boxoffice_quality.ready` | 박스오피스 품질 mart | `mart.boxoffice_quality.ready` |

예를 들어 `stg_movie_observations`가 성공하면 `dim_movie`는 즉시 시작한다.
`fct_boxoffice_daily` 또는 다른 Silver 모델의 완료를 기다리지 않는다.
`mart_boxoffice_daily`만 자신의 직접 부모인 차원과 팩트 둘을 기다린다.

각 cohort manifest에는 실제 직접 부모 Asset 이름뿐 아니라 exact relation ID와 digest가 들어
있다. 기존 `mart_data_quality`는 영화·박스오피스·공통 publication을 모두 참조해 작은 실패도
전체 품질 모델을 막는다. 품질 mart를 도메인별로 나누고 각 도메인의 publication ledger를
직접 부모로 포함한다. 전체 publication 대사는 최종 게시 coordinator에서 수행한다.

### 4.6 Serving publication

| DAG | 직접 입력 | 결과 |
| --- | --- | --- |
| `pop_talk_publish_movie_serving_component` | 영화 component cohort | content-addressed 영화 candidate component 봉인 |
| `pop_talk_publish_boxoffice_serving_component` | 박스오피스 component cohort | content-addressed 박스오피스 candidate component 봉인 |
| `pop_talk_assemble_serving_publication` | required candidate component READY cohort | GenerationPlan+component ID 기반 publication header/junction 조립 |
| `pop_talk_activate_serving_publication` | 봉인된 publication READY | 동일 generation의 active pointer 원자 교체 |

영화와 박스오피스 candidate component 적재도 분리하되, 웹 애플리케이션에 두 데이터를 하나의
publication으로 보여주는 조립·활성화는 GenerationPlan의 exact component slot 대사를 통과한 뒤
수행한다.
이 마지막 장벽은 모델 계산 병렬성을 막지 않으면서 서비스 일관성을 보장한다.

## 5. dbt 실행 규칙

모델 DAG는 공통 factory를 사용하되 DAG ID, selector, 부모/출력 Asset 계약은 명시적인
registry로 관리한다. 모델 DAG 하나의 구조는 다음과 같다.

```text
resolve_generation_cohort_manifest
  -> validate_immutable_dbt_deployment
  -> Cosmos: dbt run --select <exact model>
  -> run_owned_test_unique_ids
  -> validate_relation_count_and_digest
  -> emit_output_asset
```

`+model`, `model+`, layer tag 전체 선택은 금지한다. 부모 모델은 부모 DAG가 이미 생성하고
cohort manifest로 고정했으므로 정확한 단일 모델만 실행한다. 작업 디렉터리의 stale
`pipelines/dbt/target/manifest.json`이 아니라 승인된 `dbt_deployments/current.json`의 불변
manifest를 사용한다. model `depends_on.nodes`의 model/source와 registry의 소유자·cohort를
CI/구조 테스트에서 대사해 새 `ref()` 또는 source가 생겼는데 schedule을 빼먹는 문제를
차단한다.

모든 모델은 `--vars`로 generation/cohort manifest identity를 받고, wrapper가 graph
deployment와 실제 부모 relation vector를 실행 직전에 대사한다. test도 같은 변수를 사용한다.
각 모델 DAG가 parse할 때 각자 `current.json`을 읽지 않도록 graph release registry가 승인한
단일 deployment ID를 generation manifest에 고정한다. blue/green 배포 전환 중에도 한
generation 안에서 구·신 manifest가 섞이지 않는다.

## 6. 실패·재실행 경계

- 모델 실패: 해당 모델 DAG만 재실행한다. 이미 성공한 다른 branch는 다시 실행하지 않는다.
- 코드 변경: 새 immutable artifact/dbt deployment ID를 발급하며 기존 READY를 덮어쓰지 않는다.
- 동일 이벤트 중복: `(model_name, generation_id, deployment_id)` ledger로
  동일 결과를 확인하고 Asset event를 안전하게 재발행하거나 no-op한다.
- 비동기 부모 도착: 이벤트는 pending ledger에 남고 같은 generation cohort가 완성될 때만
  ready가 된다. A(g1), A(g2), B(g1), B(g2) 역순도 g1/g2 두 cohort로 각각 처리한다.
- 늦게 끝난 과거 실행: generation/revision 조건을 통과하지 못하면 current relation 또는
  serving active pointer를 되돌리지 못한다.
- 최종 게시 실패: candidate snapshot은 남지만 active pointer는 이전 정상 publication을
  유지한다.
- g1 모델 완료 뒤 g2가 같은 논리 모델의 current view를 바꿔도 g1 downstream은 cohort가
  가리키는 g1 immutable relation만 읽는다.

## 7. 구현 순서

### 단계 A — 계약과 구조 테스트

1. source batch/dataset revision/generation/deployment/publication identity 계약을 작성한다.
2. immutable `GenerationPlan`, `ModelResultManifest`, `ModelCohortManifest` 생명주기와
   pending/cohort ledger 및 partitioned Asset event v2 계약을 순수 Python 모듈로 작성한다.
3. dbt manifest의 model/source 직접 부모와 DAG registry 부모가 정확히 일치하는 테스트를
   작성한다.
4. 모든 test unique ID의 단일 소유 gate registry와 누락·중복 검사를 작성한다.
5. 단일 dbt 모델 DAG factory와 정확한 selector/`TestBehavior.NONE` 계약을 작성한다.
6. 기존 전체 DAG는 pause 상태로 보존하고 신규 DAG도 기본 pause로 생성한다.

### 단계 B — Snowflake/dbt branch 분할

1. candidate namespace에 generation registry, pending/cohort ledger와 generation별 append-only
   relation 계약을 만든다.
2. 데이터셋별 Snowflake load ledger와 staging publication 모델을 분리한다.
3. `stg_movie_observations`, `stg_boxoffice_observations`, `dim_movie`,
   `fct_boxoffice_daily`, mart 모델을 generation-aware 모델로 바꿔 각각 DAG로 렌더링한다.
4. 영화 branch만 완료됐을 때 `dim_movie`가 먼저 실행되고, 박스오피스 branch 실패가 이를
   막지 않는 구조/격리 통합 테스트를 수행한다.
5. g1/g2 교차 완료 중 각 mart가 exact generation 부모만 읽는지 검증한다.

### 단계 C — Databricks Bronze/Silver 분할

1. 합본 노트북의 공통 변환을 helper로 추출한다.
2. Bronze와 Silver를 도메인별 entrypoint/DAG로 분리한다.
3. S3 Exchange v2를 영화/박스오피스 데이터셋별 READY로 전환한다.
4. 동일 Raw run에서 두 branch가 독립 실행·재실행되는지 검증한다.

### 단계 D — Serving publication 분할

1. 영화와 박스오피스 candidate를 독립 content-addressed component로 적재·봉인한다.
2. required component가 모인 뒤 GenerationPlan과 ordered component ID로 publication header/junction을
   조립한다.
3. exact component vector 대사와 active pointer 교체만 담당하는 activation DAG를 만든다.
4. 한 branch 실패, stale generation, 재실행, activation 직전 실패를 검증한다.
5. A 예약→영화 완료→더 최신 B 활성화→A 박스오피스 지연 완료 반례에서 A 활성화를
   거부하는지 검증한다.

### 단계 E — 병행 검증과 전환

1. 신규 Asset/registry/dbt target/relation을 production과 분리된 candidate namespace에서 한
   바퀴 실행한다.
2. 기존 전체 DAG 결과와 행 수·키·canonical digest를 비교한다.
3. cutover watermark, 마지막 성공 generation, 미처리 Raw/Asset queue와 bootstrap할 기존
   observation/ledger 범위를 불변 manifest로 고정한다.
4. 구 writer를 pause하고 active/queued/retry DagRun을 drain한다. Databricks 원격 run 종료와
   Snowflake/PostgreSQL 미완료 transaction 부재도 확인한다.
5. production writer ownership epoch/lock 또는 전용 권한을 새 writer로 CAS 전환한다. pause나
   schedule 제거만 writer fence로 인정하지 않는다.
6. 승인된 graph release ID를 모든 신규 모델 DAG에 blue/green 방식으로 한 번에 활성화하고,
   watermark 이후 backlog를 generation ledger로 재생한다.
7. 신규 production generation 활성화 직전에 구 writer가 production relation/ledger/active
   pointer에 쓸 권한이 없음을 확인한다.
8. 실패 시 신규 writer를 fence하고 신규 active pointer를 더 올리지 않은 상태에서 구 release와
   마지막 정상 generation으로 되돌린다. 이미 발행한 generation ID는 재사용하지 않는다.
9. 검토 세션 승인 뒤 기존 `pop_talk_movie_databricks_daily`를 archive 대상으로 표시한다.

## 8. 완료 기준

- 한 모델이 실패해도 관련 없는 branch의 downstream Gold 모델은 실행 가능하다.
- `dim_movie`는 영화 staging 성공 직후 시작하며 박스오피스 완료를 기다리지 않는다.
- Airflow UI에서 모델 하나의 실행·테스트·검증·Asset 발행 상태를 한 DAG run으로 확인한다.
- dbt manifest의 model/source 직접 의존성, Asset/cohort 부모와 test unique ID 소유권이 자동
  대사된다.
- 역순·중복·coalesced 이벤트가 generation별 pending/cohort ledger에서 누락 없이 처리된다.
- g1/g2가 교차 완료돼도 downstream은 자기 generation의 immutable relation/digest만 읽는다.
- 최초 GenerationPlan은 미래 결과 digest 없이 확정되고 이후 result/cohort manifest가 이를
  수정하지 않고 추가된다.
- movie title/policy 변경·boxoffice source 무변경 fixture에서 fact는 재사용되지만
  `mart_boxoffice_daily`와 영향받은 serving component는 재계산된다.
- 이전 generation result를 재사용한 parent slot도 carry-forward binding만으로 cohort가
  완성되며 새 가짜 result event를 요구하지 않는다.
- 데이터셋별 S3/Snowflake ledger와 멱등 재실행이 검증된다.
- 동일 generation의 검증된 결과만 PostgreSQL active publication이 된다.
- 신규 candidate 한 바퀴와 기존 결과 canonical digest 비교가 통과한다.
- drain/writer fence/watermark/backlog/bootstrap/rollback을 포함한 cutover rehearsal이 통과한다.
- 계획, 구현, 통합 결과가 검토 세션에서 승인된다.

## 9. 검토 질문

1. 모델 하나를 DAG 하나의 재실행·게시 단위로 삼는 기준이 사용자 요구와 운영 비용 사이에
   적절한가?
2. 전역 레이어 장벽을 제거하고 직접 부모 Asset만 기다리는 그래프가 충분히 명확한가?
3. 공통 publication ledger와 `mart_data_quality`를 도메인별로 나누는 것이 실제 조기 실행을
   위해 필요한가?
4. 마지막 PostgreSQL activation에만 동일 generation 전역 장벽을 남기는 설계가 안전한가?
5. 단계 A→E의 migration이 기존 production writer와 충돌하지 않도록 충분한가?
6. generation별 append-only relation과 cohort ledger가 g1/g2 교차 완료 반례를 닫는가?
7. 명시적 test ownership registry가 현재 source test, 다중 부모 singular test, 부모 없는
   계약 test를 빠짐없이 보존하는가?
8. GenerationPlan→ModelResult→ModelCohort 생명주기가 미래 digest placeholder와 불변 manifest
   수정을 모두 제거했는가?
9. 독립 serving component 선적재가 영화 branch의 조기 실행을 유지하면서 최종 publication만
   content-addressed로 봉인하는가?
