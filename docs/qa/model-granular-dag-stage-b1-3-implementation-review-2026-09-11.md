# B1-3 구현 검토 요청 — 모델별 DAG와 직접 의존성 전달

## 이번 구현 결과

- current dbt deployment의 모델 7개를 각각 `pop_talk_model_<name>` DAG로 생성했다.
- 각 DAG는 자기 `COHORT_READY` Asset과 generation partition만 소비한다.
- task 구조는 `resolve_cohort → claim_build → validate_deployment → model_run →
  owned_tests → verify_receipt → commit_result → emit_ready` 8단계다.
- model과 test는 Cosmos `DbtRunLocalOperator`/`DbtTestLocalOperator`를 사용하지만,
  B1-2 strict wrapper와 registry의 exact FQN selector만 실행한다.
- dispatcher는 모든 `MODEL_READY` Asset 중 하나가 도착할 때마다 PostgreSQL catalog를
  재스캔한다. direct parent 전체가 준비된 모델의 cohort만 생성하므로 layer barrier가 없다.
- reconciler는 pending/만료 outbox 또는 중단된 inbox event를 한 run에 하나씩 재발행한다.
- v7 `generation_gate_receipts`를 추가해 deployment gate와 모델이 직접 사용하는 source
  snapshot gate가 없으면 cohort를 만들지 않는다. 관계없는 source gate는 해당 root를 막지 않는다.
- REUSE slot은 원본 result와 `origin_generation_id`를 보존한 채 현재 generation partition의
  `MODEL_READY` outbox를 만든다.

## 주요 파일

- `pipelines/orchestration/model_generation_contract.py`
- `pipelines/orchestration/postgres_control_store.py`
- `pipelines/orchestration/model_dag_runtime.py`
- `pipelines/orchestration/sql/007_generation_gate_receipts.sql`
- `orchestration/airflow/dags/pop_talk_model_factory.py`
- `orchestration/airflow/dags/pop_talk_model_dispatcher.py`
- `orchestration/airflow/dags/pop_talk_model_asset_reconciler.py`
- `pipelines/orchestration/tests/test_model_dag_runtime.py`
- `pipelines/orchestration/tests/test_model_generation_contract.py`
- `pipelines/orchestration/tests/test_postgres_control_store.py`

## 검증 증거

- v7 적용 digest:
  `80c23a458e547db92804c9670add5bddd3a87d53fa8f51a78b55998d69746c4e`
- Airflow 3.3.1 전체 DAG 폴더 DagBag: import error 0
- model factory 직접 parse: 모델 DAG 7개, 각 8 task, 모두
  `PartitionedAssetTimetable` 확인
- orchestration 전체 + 실제 PostgreSQL 통합 suite: 74 tests, OK
- 실제 PostgreSQL에서 확인한 항목:
  - gate 없음: root cohort 0개
  - deployment gate + 필요한 source gate 도착 후 재스캔: 해당 root 1개
  - 다른 두 source gate 미도착: 위 root 진행을 막지 않음
  - 같은 재스캔 replay: 추가 cohort 0개
  - COHORT_READY ACK 뒤 inbox `EXECUTE_MODEL` 복구 및 완료

## 의도적으로 남긴 B2 경계

`verify_receipt`는 현재 `allow_b1_3_synthetic_result=true`인 제어 흐름 시험만 허용한다.
기본값은 false라 실제 relation을 검증하지 않은 result는 commit되지 않는다. Snowflake candidate
relation, row count/content digest 검증과 실제 source/deployment dbt gate 실행은 B2에서 연결한다.

## 검토 요청 범위

다음 항목에서 B1-3을 막는 correctness 문제만 판정해 달라.

1. direct-parent fan-out/fan-in에 layer-wide barrier가 숨어 있는가?
2. cohort/outbox, ACK/inbox, result/outbox transaction 경계가 crash-safe한가?
3. g1/g2 partition 또는 REUSE origin을 섞을 수 있는 경로가 있는가?
4. gate 누락/교환 또는 registry와 다른 test 증거를 허용하는가?
5. Cosmos strict task가 selector나 deployment를 우회할 수 있는가?
6. reconciler가 정상 실행을 중복 claim하거나 event를 유실할 수 있는가?

알림, dashboard, 실제 Snowflake SQL/vars, 운영 cutover와 일반 backfill UI는 B2 이후 항목으로
분류하고 이번 승인 조건에 추가하지 않는다.
