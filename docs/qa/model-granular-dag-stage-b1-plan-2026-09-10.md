# 모델 단위 DAG 분할 단계 B1 계획 — Durable control store와 DAG factory

## 1. 범위와 결론

승인된 단계 A의 순수 `GenerationPlan`/`ModelCohortManifest`/`ModelResultManifest` 계약을
실제 PostgreSQL 트랜잭션과 Airflow 3.3.1 partitioned Asset에 연결한다. 이 단계에서는 아직
production dbt model SQL이나 기존 전체 DAG를 교체하지 않는다.

구현 대상은 다음 두 가지다.

1. generation, slot, cohort, result와 Asset outbox를 보존하는 durable control store
2. 정확한 model 하나와 소유 test만 실행하는 공통 Airflow/Cosmos DAG factory

## 2. 저장 위치

동일 Docker PostgreSQL 인스턴스의 **서비스 DB에 별도 `dw_control` schema**를 사용한다.

- Airflow metadata DB의 core/Alembic table에는 섞지 않는다.
- 서비스용 `dw_serving` schema와도 분리한다.
- migration owner `dw_control_owner`와 runtime role `dw_control_runtime`을 B1부터 분리한다.
  runtime은 non-superuser/non-owner이며 `dw_control`의 필요한 table/sequence 권한만 가진다.
- DAG task는 같은 host/database를 사용하되 `POP_TALK_CONTROL_POSTGRES_*` 전용 credential로
  접속한다. `search_path=dw_control`은 편의를 위한 이름 해석 설정일 뿐 권한 경계로 간주하지
  않는다.
- runtime role은 `dw_serving`, `public`, Airflow database object에 쓰기 권한 및 이를 우회할
  role membership이 없어야 한다. migration 후 privilege catalog 조회와 별도 격리 sentinel
  table의 거부 시험으로 이를 검증한다. 서비스 업무 데이터에는 쓰기 시험을 하지 않는다.

Airflow가 로컬에서 실행되고 최종 serving PostgreSQL도 이 인스턴스에 있으므로 별도 cloud
control service를 추가하지 않는다. Snowflake는 업무 데이터 relation과 dbt 결과를 소유하고,
PostgreSQL `dw_control`은 오케스트레이션의 작은 metadata만 소유한다.

## 3. 스키마

DDL은 versioned migration으로 관리하며 Airflow 시작 때 자동 destructive migration을 하지
않는다. `scripts/migrate_dw_control.py`를 명시적으로 실행한다.

### 3.1 generation과 build slot

```text
generation_plans
  plan_id PK
  generation_id UNIQUE
  generation_sequence UNIQUE
  deployment_id
  graph_digest
  body JSONB
  created_at

model_build_slots
  (plan_id, model_unique_id) PK
  build_id UNIQUE
  mode CHECK(REBUILD|REUSE)
  reuse_result_manifest_id NULL/FK
  state CHECK(PLANNED|CLAIMED|SUCCEEDED|REUSED|FAILED)
  current_attempt_no
  current_fence_token
  claim_owner / claim_expires_at / heartbeat_at
  result_manifest_id NULL/FK
```

generation sequence 예약과 plan/slot 삽입은 schema advisory transaction lock 아래 한 번에
수행한다. `GenerationPlan`은 insert 전에 단계 A validator로 전수 검사하고 body JSON을 다시
읽어 동일 객체인지 확인한다. 동일 content ID replay는 no-op, 같은 generation/build ID의 다른
본문은 conflict다.

REUSE slot은 등록 시 권위 result provenance를 검증하고 `REUSED`로 기록한다. REBUILD slot은
lease 기반 claim을 사용한다. claim 때 attempt 번호를 증가시키고 전역 sequence에서 단조 증가
`fence_token`을 발급한다. heartbeat, receipt 작성, result commit은 모두
`(plan_id, model, build_id, attempt_no, fence_token, claim_owner)`와 미만료 lease가 현재 slot과
일치할 때만 성공한다. worker 이름이 재사용돼도 예전 token은 통과할 수 없다.

Snowflake 쓰기는 generation 공통 relation이 아니라
`candidate_<build-id>_<attempt-no>_<fence-token>`처럼 **attempt별 물리 relation**에만 수행한다.
이 relation은 검증 후 더 이상 쓰지 않는 봉인 객체이고 result가 정확한 fully-qualified relation
ID와 digest를 참조한다. A의 lease가 만료되어 B가 새 relation을 봉인·commit한 뒤 A 쿼리가
완료돼도 A relation만 바뀌므로 B result의 데이터는 변하지 않는다. SQL 시작 전 fence 확인만으로
보호했다고 간주하지 않는다.

### 3.2 cohort와 result

```text
model_cohorts
  cohort_manifest_id PK
  plan_id
  model_unique_id
  build_id UNIQUE
  body JSONB
  created_at
  FK(plan_id, model_unique_id, build_id) -> model_build_slots

model_results
  result_manifest_id PK
  plan_id
  cohort_manifest_id UNIQUE
  model_unique_id
  build_id UNIQUE
  attempt_no / fence_token
  relation_id
  row_count
  content_sha256
  body JSONB
  created_at
  FK(plan_id, model_unique_id, build_id) -> model_build_slots
  FK(cohort_manifest_id, plan_id, model_unique_id, build_id) -> model_cohorts
```

cohort는 `REPEATABLE READ` transaction에서 plan과 필요한 result catalog를 읽고 단계 A
validator가 만든 exact object만 삽입한다. unique/composite FK conflict는 rollback 후 권위 row를
다시 읽어 body까지 같을 때만 replay 성공으로 처리한다. result commit은 current fenced claim,
cohort, relation 검증 receipt와 소유 test 성공 receipt를 대사한 뒤 result/slot/outbox delivery를
한 transaction에서 반영한다.

DB 제약과 transaction validator가 `REBUILD`/`REUSE`, state, nullable result/reuse-result 조합을
함께 강제한다. 예를 들어 REBUILD-PLANNED에는 result가 없고, REUSE-REUSED에는 검증된 과거
result가 있으며, SUCCEEDED에는 정확히 하나의 같은-slot result가 필요하다. 순환 참조는 slot을
NULL result로 만든 뒤 cohort/result를 등록하고 마지막에 slot을 완료하는 순서로 처리한다.

Snowflake relation 생성과 PostgreSQL metadata commit은 분산 transaction이 아니다. relation은
먼저 attempt별 candidate 이름에 작성·검증·봉인한다. metadata commit 전에 실패하면
orphan candidate이며 GC 대상일 뿐 READY가 아니다. metadata commit 후 응답을 잃으면 같은
build/result content ID로 재조회해 성공을 확정한다.

GC는 권위 result가 참조하는 relation, 현재 lease의 relation, 보존 기간 안 attempt를 삭제하지
않는다. response-loss 재시도는 새 attempt를 만들기 전에 동일 attempt/result를 조회하여 이미
commit된 성공인지 확인한다.

### 3.3 실행 receipt

```text
model_execution_receipts
  (build_id, attempt_id) PK
  cohort_manifest_id
  deployment_id
  parent_vector_sha256
  attempt_no / fence_token / claim_owner
  relation_id
  row_count
  content_sha256
  model_invocation_id / model_unique_id / model_run_results_sha256
  test_invocation_id / test_run_results_sha256
  executed_test_ids JSONB
  tests_sha256
  relation_validation_query_id
  status CHECK(RUNNING|SUCCEEDED|FAILED)
  started_at / completed_at
```

receipt는 slot/build/attempt composite identity를 참조한다. dbt model과 test는 attempt별 빈
`target`/`log` 경로에서 실행하며 시작 시 기존 artifact가 없음을 확인한다. subprocess가 0으로
끝난 것만으로 성공하지 않고 이번 invocation이 새로 만든 `run_results.json` digest와 invocation
ID를 고정한다. model 결과는 기대한 model unique ID 하나뿐이어야 하고, test 결과는 registry의
owned test unique ID 집합과 정확히 같으며 각 status가 허용 성공 상태여야 한다. error, fail,
skip, 누락, 예상 test가 있는데 결과 0건인 경우를 거부한다. 원래 owned test가 0개인 경우만
별도 EMPTY_TEST_SET receipt로 허용한다.

result 등록 시 receipt의 cohort/deployment/parent vector/fence token과 current claim, 같은 봉인
relation을 모두 대사한다. relation ID는 candidate namespace allowlist와
build/attempt/fence/deployment naming 규칙을 통과해야 한다. canonical row digest는 Snowflake에서
계산한 `relation_validation_query_id`의 검증 결과와 대사한다.

## 4. Asset outbox

Airflow metadata의 AssetEvent commit과 PostgreSQL business transaction은 원자화할 수 없다.
따라서 exactly-once를 주장하지 않고 at-least-once + downstream dedup을 사용한다.

```text
asset_outbox
  event_id PK                 # event body content ID
  aggregate_kind CHECK(COHORT_READY|MODEL_READY|...)
  aggregate_id
  asset_uri
  partition_key              # generation_id
  event_body JSONB
  state CHECK(PENDING|SENDING|COMPLETE)  # delivery들의 집계 상태
  lease_owner / lease_expires_at
  send_attempts
  last_sent_at / completed_at

asset_deliveries
  (event_id, consumer_id) PK
  state CHECK(PENDING|SENDING|ACKED|COMPLETED)
  lease_owner / lease_expires_at
  acknowledged_at / completed_at

asset_inbox
  (consumer_id, event_id) PK
  asset_uri / partition_key / aggregate_id
  event_body JSONB
  work_kind / work_identity
  state CHECK(PENDING|CLAIMED|COMPLETED|FAILED)
  claim_owner / claim_expires_at
  created_at / completed_at
```

흐름:

1. release registry가 event별 `required_recipient` 집합을 고정한다. cohort/result 등록 transaction은
   PENDING outbox와 recipient별 PENDING delivery를 함께 insert한다. recipient가 없는 terminal
   event는 생성 즉시 전달 완료로 간주하되 권위 result는 그대로 보존한다.
2. publisher가 `FOR UPDATE SKIP LOCKED`로 미완료 delivery가 있는 outbox를 claim한다.
3. Airflow task가 `outlet_events[asset].add_partitions(generation_id)`로 **실제 partition key**와
   event body를 발행한다.
4. publisher task 성공만으로 ACK하지 않는다. downstream resolver가 event ID, recipient,
   Asset URI, 실제 partition, aggregate body를 권위 outbox와 대사하고, **그 recipient가 수행할
   durable pending work를 inbox에 등록하는 것과 같은 transaction에서** delivery를 ACKED로 만든다.
5. resolver 뒤 실제 cohort/slot claim 전에 실패해도 inbox PENDING이 남는다. recovery가 ACK 여부와
   무관하게 미완료 inbox, 아직 cohort가 없는 준비 가능한 slot, PLANNED slot, 만료 claim을 scan해
   다시 구동한다. work 완료 시 inbox/delivery를 COMPLETED로 만든다.
6. emit 직전/직후 process가 죽으면 lease 만료 후 같은 event ID를 재발행한다. 두 recipient 중
   하나만 ACK한 경우에도 다른 delivery가 남으므로 재전송이 계속된다. inbox PK와 body comparison,
   model/build ledger가 중복을 제거하며 같은 ID의 다른 body는 conflict다.
7. 주기적 reconciliation task는 미완료 delivery, READY인데 outbox/delivery가 없는 aggregate,
   미완료 inbox/cohort/slot/claim을 모두 복구한다. 수동 실행 가능성만 두지 않고 factory의 공통
   recovery DAG로 제공한다.

event extra는 힌트이며 resolver는 `aggregate_id`로 control store를 다시 읽고 단계 A provenance
validator를 통과해야 한다. ACK 소유자는 release registry의 required recipient여야 하며, 다른
consumer가 대신 ACK할 수 없다.

## 5. 단일 모델 DAG factory

factory 입력은 승인 graph release의 `ModelDagSpec` 하나다. helper나 test마다 DAG를 만들지
않는다. 생성되는 모델 DAG 한 개의 구조는 다음과 같다.

```text
resolve_partitioned_cohort
  -> claim_build_slot
  -> validate_fixed_deployment
  -> build_<model>.<model>_run          # Cosmos, 정확한 fqn 하나, fenced attempt relation
  -> run_owned_tests                   # registry의 exact test fqn 목록
  -> verify_relation_receipt
  -> commit_result_and_outbox
  -> emit_model_ready_partition
```

- schedule은 단일 `cohort.<model>.ready` Asset을 받는
  `PartitionedAssetTimetable(..., IdentityMapper())`다.
- resolver는 `dag_run.partition_key`와 실제 event partition, generation/cohort body를 대사한다.
- manual run도 `generation_id`와 `cohort_manifest_id`를 명시해야 하며 latest fallback은 없다.
- Cosmos `RenderConfig.select=[exact_model_fqn]`, `TestBehavior.NONE`을 사용한다. `+`, tag,
  layer selector는 거부한다.
- 소유 test는 manifest의 exact test `fqn:` selector를 subprocess argv로 전달하며
  `--indirect-selection empty`를 명시한다. wrapper가 task ID, deployment, allowed selector 집합과
  실제 argv를 다시 대사한다. model/test 모두 claim이 부여한 attempt별 target/log 경로와
  invocation ID를 쓰며 완료 artifact를 receipt에 귀속시킨다.
- model SQL은 attempt별 candidate schema/relation 이름을 dbt vars 또는 검증된 macro 계약으로
  주입한다. downstream `ref()`도 cohort의 fully-qualified parent relation vector를 명시적으로
  resolve하며 mutable current view를 읽지 않는다. 이 generation-aware SQL 계약의 업무 모델 적용은
  B2이지만 B1 synthetic model에서 같은 메커니즘을 검증한다.
- output Asset은 owned test와 relation digest 검증 뒤 result/outbox commit이 끝난 경우에만
  발행한다.
- 모든 신규 DAG는 기본 pause이고 candidate schema만 쓴다.

graph release는 factory parse마다 임의의 `current.json`을 따로 읽지 않는다. 배포 단계가 승인한
하나의 release descriptor에서 전체 registry를 만들고, 같은 serialized DAG bundle의 모든 모델
DAG가 같은 `deployment_id`를 가진다. execution plan의 deployment와 다르면 외부 SQL 전에
실패한다.

## 6. test ownership 실행

단계 A registry의 43개 test는 다음처럼 실행한다.

- source gate 소유 test: 후속 Snowflake load/source READY factory에서 실행
- model gate 소유 test: 해당 model DAG의 `run_owned_tests`
- deployment gate 소유 test: release 생성/승인 명령에서 실행

각 실행은 selector 문자열뿐 아니라 dbt `run_results.json`의 실제 test unique ID 집합과 예상
집합을 대사한다. 0개를 성공으로 오인하거나 indirect selection으로 sibling test가 추가되는
경우 실패한다. 다중 부모 test는 owner model cohort가 모든 입력 relation을 고정했는지 다시
검사한다.

## 7. 구현과 검증 순서

1. `dw_control` migration SQL/Python과 schema contract test
2. psycopg store adapter의 plan/cohort/result/claim transaction 및 read-back validator
3. outbox/inbox claim·lease·ACK·reconciliation adapter
4. exact model/test 실행 contract와 기존 `pop-talk-dbt` wrapper 확장
5. 공통 Airflow/Cosmos factory 생성
6. fan-out과 다중 부모 join을 포함한 synthetic diamond graph로 source/model/deployment/test gate,
   partitioned Asset g1/g2 역순·중복·ACK/emit crash 통합 테스트
7. current 7-model manifest의 factory render snapshot 검사. 실제 업무 model SQL 실행은 B2에서 수행
8. 구현 검토 승인 후 B2 generation-aware Snowflake/dbt 모델 분할로 이동

## 8. 필수 부정 시나리오

- 같은 generation sequence/plan/build ID에 다른 JSON 본문 삽입
- lease 도중 worker 사망 후 만료 claim 복구와 구 worker의 stale commit 거부
- A lease 만료→B attempt relation 봉인/commit→A 원격 완료 후에도 B digest 불변
- relation 성공 뒤 metadata commit 전 실패로 orphan이 생겨도 READY 미발행
- metadata/outbox commit 뒤 emit 전·후 실패에서 동일 event 재발행
- 두 required recipient 중 한 곳만 ACK한 fan-out의 나머지 delivery 재발행
- ACK와 pending work 등록 직후 claim 전 crash의 inbox 기반 복구
- consumer 없는 terminal event의 완료와 영구 SENDING 방지
- 중복 event inbox upsert와 같은 build result no-op, 같은 ID/다른 body conflict
- extra generation과 실제 Asset partition key 불일치
- g1/g2 event 역순 및 g1 미ACK 상태에서 g2 도착
- 다른 deployment의 DAG bundle이 generation을 실행하려는 경우
- 모델 selector graph 확장, 미소유 test 추가/소유 test 누락
- result/outbox commit 후 늦은 과거 worker가 다른 digest로 덮어쓰기 시도
- 과거 attempt의 `run_results.json` 재사용, stale/누락 artifact, skip/error test
- 실제 PostgreSQL concurrent claim/cohort/result transaction 충돌과 body comparison

## 9. 완료 기준

- DB PK/FK/CHECK와 단계 A validator가 함께 잘못된 manifest/중복을 거부한다.
- claim/result/outbox 상태 전이가 동시 worker와 응답 유실에 멱등이다.
- stale attempt는 PostgreSQL commit뿐 아니라 권위 Snowflake relation도 바꿀 수 없다.
- Asset event 유실과 ACK 후 실행 전 실패를 recipient별 delivery/inbox reconciliation으로 자동 복구한다.
- 실제 partition key로 generation별 DagRun이 분리된다.
- factory가 exact model 1개와 정확한 owned test ID만 렌더링·실행한다.
- current manifest의 model/source/test registry 전수 대사가 유지된다.
- production relation, 기존 전체 DAG와 serving active publication은 변경되지 않는다.
- 구현 및 synthetic integration 결과가 검토 세션에서 승인된다.

## 10. 검토 질문

1. `dw_control`을 Airflow metadata가 아닌 서비스 DB의 별도 schema에 두는 경계가 적절한가?
2. relation 선봉인→metadata/result/outbox transaction 순서가 분산 transaction 실패를 안전하게
   다루는가?
3. recipient별 delivery와 pending-work inbox를 같은 transaction에서 ACK하는 방식이 fan-out 및
   emit/ACK/claim 전후 crash를 복구하는가?
4. 단조 fence token과 attempt별 불변 relation이 늦은 Snowflake worker까지 충분히 격리하는가?
5. 전용 runtime role과 권한 검증이 서비스/Airflow metadata 쓰기를 실제 차단하는가?
6. attempt별 artifact와 exact test selector/`run_results.json` 대사가 Cosmos
   `TestBehavior.NONE`의 빈틈을 닫는가?
7. B2 업무 SQL 전 단계로 synthetic diamond partitioned Asset/factory 검증 범위가 충분한가?
