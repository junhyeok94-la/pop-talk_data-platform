# Airflow Asset 기반 Raw → Main 자동 연결 설계

## 1. 목표

`pop_talk_movie_raw_daily`가 S3의 `DAILY_READY.json` 검증을 끝낸 경우에만
`pop_talk_movie_databricks_daily`를 자동 실행한다. scheduler 지연이나 pause 때문에 여러
READY가 한 DagRun에 묶여도 모두 처리한다. 실제 데이터는 계속 S3를 통해
이동하고, Airflow Asset은 데이터 자체가 아니라 **검증된 Raw 실행이 준비됐다는 제어
이벤트**와 lineage metadata만 전달한다.

Airflow 3.3.1 공식 Asset API를 사용하며 `TriggerDagRunOperator`나 polling sensor는
사용하지 않는다.

## 2. Asset 계약

고정 Asset URI는 다음과 같다.

```text
s3://amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an/manifests/movie_api_daily/v1/DAILY_READY
```

이 URI는 개별 파일 key가 아니라 “검증된 일일 영화 Raw 실행” 스트림을 나타낸다. 실행마다
다른 S3 key를 Asset URI로 만들지 않으므로 DAG schedule이 안정적이고 Airflow Asset 목록이
무한히 늘어나지 않는다.

`validate_daily`가 성공할 때 발행하는 event `extra`는 다음 필드를 가진다.

| 필드 | 의미 |
| --- | --- |
| `contract_version` | 이벤트 payload 계약 버전. 최초 값은 `1` |
| `bucket` | READY manifest가 있는 S3 bucket |
| `ready_manifest_key` | 검증 완료된 실제 `DAILY_READY.json` key |
| `source_dag_id` | 생산 DAG ID |
| `source_run_id` | 생산 Airflow DAG run ID |
| `collection_date` | Raw plan의 업무 기준일 |
| `raw_run_id` | Raw 수집 plan의 불변 실행 식별자 |

자격 증명, API key, 원천 본문은 Asset URI와 event metadata에 넣지 않는다. metadata는
Airflow metadata DB에 평문으로 저장된다.

## 3. 생산 DAG

`validate_daily`에 위 Asset을 `outlets`로 선언한다. `_collector(plan).validate_run()`이
모든 stage를 대사하고 `DAILY_READY.json`을 성공적으로 게시한 뒤에만 `outlet_events`에
metadata를 설정한다.

Airflow는 outlet 태스크가 성공해야 Asset update를 기록하므로, 수집·상세·KMDb·검증 중
하나라도 실패하면 메인 DAG는 실행되지 않는다. 다만 이는 exactly-once 발행 보장이 아니다.
성공한 태스크를 clear하여 다시 성공시키거나 같은 READY를 다시 게시하면 별도의 Asset event가
생길 수 있으므로 소비자가 논리적 중복을 허용하고 제거한다.

## 4. 소비 DAG와 정확한 입력 선택

메인 DAG의 schedule을 `[DAILY_MOVIE_RAW_READY_ASSET]`으로 변경한다. 첫 제어 태스크
`resolve_ready_inputs`을 추가하여 실행 유형별 입력 목록을 결정한다.

- Asset-triggered run: `triggering_asset_events`에서 **이번 DAG run을 발생시킨 모든 이벤트**를
  읽는다. bucket, key pattern, contract version, 생산 DAG ID와 필수 lineage를 검증한다.
  실제 AssetEvent의 source DAG/run provenance와 event extra의 source 값도 서로 대사한다.
- Manual run: 기존 `params.ready_manifest_key`를 사용한다. 기존 장애 주입과 재처리 절차도
  그대로 유지한다.
- Scheduled run 등 정의하지 않은 실행 유형, Asset event 0건, 잘못된 payload는 외부 시스템을
  변경하기 전에 실패시킨다.

검증된 이벤트는 `(bucket, ready_manifest_key, raw_run_id)`를 논리 identity로 중복 제거하고
`collection_date`, key 순으로 결정적으로 정렬한다. 같은 key인데 lineage 값이 충돌하면 중복으로
숨기지 않고 실패한다. 서로 다른 READY 여러 건은 아래 mapped task로 전부 처리한다.

`stage_bundle`과 Databricks notebook parameter는 모두 `resolve_ready_inputs` XCom의 각
`ready_manifest_key`를 사용한다. Asset 실행 중 `params` 기본값이나 가장 최근 Asset
이벤트를 대신 읽지 않는다. 따라서 이벤트 A가 실행시킨 DAG가 뒤늦게 시작돼도 이벤트 B의
manifest를 잘못 처리하지 않는다.

`resolve_ready_inputs` 결과는 bucket, key와 lineage만 담는 작은 JSON dict 목록이다. event
객체나 원천 데이터는 XCom에 넣지 않는다. 목록은 Airflow의 `max_map_length`를 넘지 않는지
검사한다. 초과 시에는 모든 key를 triggering event에서 복구할 수 있게 실패 run을 보존하고,
운영자가 READY 목록을 분할해 manual run으로 처리·대사한다. 어떤 key도 자동으로 버리지 않는다.

## 5. 의존 관계와 멱등성

흐름은 다음처럼 바뀐다.

```text
Raw.validate_daily
  -- Asset event(s) --> Main.resolve_ready_inputs
                         -> identify_gold_build -> deploy_notebook
                         -> stage_bundle.expand(each READY)
                         -> Databricks.expand(each staged READY)
                         -> S3 Exchange.expand -> Snowflake.expand
                         -> dbt Gold once -> PostgreSQL once
```

`resolve_ready_inputs`은 mapped `stage_bundle`의 데이터 입력이며, 기존 dbt graph identity
고정 태스크도 계속 전체 외부 처리의 선행 조건이다. 메인 DAG의 `max_active_runs=1`은 유지한다.
각 READY는 stage → Databricks → Exchange → Snowflake까지 READY identity와 동일 map index로
추적한다. `confirm_loaded_batch`가 resolve 입력, stage 결과와 Snowflake load 결과의
`raw_run_id` 집합과 건수를 대사하며 ALL_SUCCESS barrier가 된다. 이 태스크가 성공한 뒤에만
Cosmos dbt 7개 모델을 한 번 계산하고 PostgreSQL serving snapshot을 한 번 게시한다. 실패한
map index만 재시도할 수 있으며 dbt/serving은 일부 입력만 성공한 상태에서 실행되지 않는다.

`max_active_runs=1`은 서로 다른 DagRun만 직렬화하므로 mapped task의 같은 run 내 병렬성을
막지 않는다. 기존 표본 파이프라인의 단일 외부 쓰기 경계를 보존하기 위해
`stage_bundle`, Databricks submit, `publish_exchange`, `load_snowflake` 각각에
`max_active_tis_per_dag=1`을 설정한다. 즉 READY가 여러 개여도 각 외부 단계는 map index를
하나씩 처리하며, Cosmos는 기존 `pop_talk_dbt_pool` 1 slot을 계속 사용한다. 첫 운영 표본의
안전성과 API/warehouse 비용을 우선한 설정이고, 병렬 확대는 별도 부하 검증 후 조정한다.

Databricks mapped operator 입력은 준비 태스크가 READY마다 완성된 `json`과
`idempotency_token`을 하나의 dict로 만들어 `partial(...).expand_kwargs(...)`에 전달한다.
두 필드를 별도로 `expand`해 Cartesian product를 만들거나 mapped 값 안에서 Jinja로 다른
map index를 조회하지 않는다.

동일 READY가 운영자의 수동 재실행 또는 중복 이벤트로 다시 처리되어도 resolver의 논리 중복
제거와 기존 불변 S3 key,
Databricks idempotency token, Snowflake ledger/transaction, PostgreSQL publication 계약으로
중복·부분 게시를 방지한다. Asset은 기존 멱등성 계층을 대체하지 않는다.

자동 실행의 신규 READY는 `publication_revision=1`, `processing_attempt=1`을 사용한다. 중복
event는 이 값을 올리지 않는다. 이미 처리한 READY를 변경된 코드로 다시 게시하거나 원격 실패
뒤 새 처리가 필요하면 자동 event 재발행에 의존하지 않고 manual run에서 revision/attempt를
명시적으로 올린다.

## 6. pause와 운영 규칙

- 코드 배포 및 구조 테스트 중 두 운영 DAG는 pause 상태를 유지한다.
- 통합 검증에서는 메인 DAG만 먼저 unpause하고 Raw DAG를 작은 범위로 수동 실행한다.
- 메인 DAG가 `asset_triggered` run으로 자동 생성되고 이벤트의 key를 사용했는지 확인한다.
- 전체 성공 및 serving publication을 확인한 뒤 두 DAG를 다시 pause한다.
- 운영 전환 시에는 Raw와 메인 DAG를 의도적으로 함께 unpause한다. pause 중 쌓인 queued
  Asset event가 있는지는 Airflow Asset 화면/API에서 확인한다. queue는 소비 성공 확인이 아니라
  DagRun 생성 시 제거될 수 있으므로, 연결된 DagRun의 triggering events와 각 READY의
  Snowflake load ledger/최종 publication을 대사한다. 처리되거나 의도적 제외 사유가 별도 기록된
  event만 queue에서 수동 제거할 수 있다.
- Asset schedule 등록 전에 이미 존재하던 S3 READY는 자동 backlog가 아니다. 최초 전환 시
  기존 승인 표본 key는 manual run 완료 기록으로 유지하고, 이후 Raw가 새로 발행하는 event부터
  자동 범위로 삼는다. 추가 과거 READY는 명시적인 manual backfill 목록으로 처리한다.

## 7. 구현 파일과 테스트

- 공통 Asset 선언: `orchestration/airflow/dags/pop_talk_assets.py`
- event payload·중복 제거·정렬 순수 검증: `pipelines/orchestration/asset_contract.py`
- 생산/소비 DAG 변경: Raw daily, Databricks daily DAG
- 단위 테스트: 정상 payload, 수동 fallback, 필수값 누락, 잘못된 bucket/key/version,
  이벤트 0건 거부, 동일 READY 중복 제거, 서로 다른 READY 복수 정렬, 충돌 중복 거부
- 구조 테스트: 생산 outlet과 소비 schedule URI 동일, 신규 task/edge, Databricks parameter가
  완성된 mapped request를 참조함, 네 mapped 외부 task의 직렬 동시성, load batch barrier,
  parse/import 오류 0건
- 통합 테스트: Raw 수동 실행 → Asset 자동 DagRun → 전체 pipeline 성공 → PostgreSQL active
  publication 확인. 별도 격리 테스트에서는 scheduler 지연 복수 READY, 동일 READY 중복,
  resolver 실패/재시도와 triggering event 보존을 확인한다.

## 8. 검토 질문

1. 고정 스트림 Asset + 실행별 event metadata가 이 일일 파이프라인에 적절한가?
2. Asset run의 모든 triggering event를 논리 중복 제거한 뒤 mapped task로 빠짐없이 처리하고,
   수동 run만 Param fallback하는 경계가 안전한가?
3. pause/queue/DagRun/처리 ledger 대사와 최초 backlog 경계가 복구 절차로 충분한가?
4. `resolve_ready_inputs`와 dynamic task mapping이 디버깅 및 DAG 개발 표준에 맞는가?
