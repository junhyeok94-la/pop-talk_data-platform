# B1-3 계획 — 모델별 DAG factory와 직접 의존성 전달

## 목표와 제외 범위

B1-1의 generation/cohort/result/outbox 원장과 B1-2의 exact dbt invocation을 Airflow 3.3.1
모델별 DAG에 연결한다. 현재 manifest의 모델 7개는 각각 별도 DAG가 되고, 직접 부모 결과가
준비된 모델부터 generation partition 단위로 실행 가능해진다.

이번 단계는 synthetic relation/executable로 제어 흐름을 검증한다. 실제 Snowflake 업무 relation
vars와 7개 SQL 실행은 B2, 운영 cutover와 기존 전체 DAG 제거는 후속 단계다. 알림·대시보드·일반화된
backfill UI도 추가하지 않는다.

## 생성 DAG

1. `pop_talk_model_generation_dispatcher`
   - generation plan 등록 후 root/REUSE 준비 상태를 스캔한다.
   - `MODEL_READY` delivery의 수신자를 승인 release registry로 확인한다.
   - 부모 하나의 완료만으로 자식 cohort를 만들지 않고 `scan_ready_cohorts`가 direct parent 전체의
     exact result binding을 확인한 경우에만 cohort를 등록한다.
   - 등록된 cohort의 `COHORT_READY` outbox를 generation partition으로 발행한다.
2. `pop_talk_model_<model_name>` 7개
   - 각 DAG는 자기 `pop-talk://cohort/<model_unique_id>/ready` Asset 하나를
     `PartitionedAssetTimetable(..., IdentityMapper())`로 소비한다.
   - task 구조는 `resolve_cohort → claim_build → validate_deployment → model_run → owned_tests →
     verify_receipt → commit_result → emit_ready`로 고정한다.
   - `model_run`과 `owned_tests`는 B1-2 strict executable만 사용한다. selector는 registry의 정확한
     model fqn 하나와 해당 model 소유 test만 사용하고 Cosmos 자동 test/Asset은 끈다.
3. `pop_talk_model_asset_reconciler`
   - PENDING/만료 SENDING outbox를 다시 발행하고, pending/만료 inbox work를 dispatcher/model DAG가
     다시 받을 수 있게 한다. 새 업무 계산은 하지 않는다.

Airflow UI의 DAG 간선은 `parent MODEL_READY → dispatcher → child COHORT_READY → child model DAG`다.
레이어 전체 barrier는 없으며 `dim_movie`는 자기 직접 부모만, `mart_boxoffice_daily`는
`dim_movie + fct_boxoffice_daily`가 모두 고정되는 즉시 시작한다.

## durable 상태 전이

- `register_cohort_and_outbox`는 cohort row, COHORT_READY outbox, model DAG delivery를 한 PostgreSQL
  transaction에서 만든다. 같은 content body replay만 no-op이다.
- model DAG resolver는 실제 Asset URI/partition/event body를 DB outbox와 대사하고 ACK와
  `EXECUTE_MODEL` inbox 등록을 한 transaction에서 수행한다.
- MODEL_READY를 받은 dispatcher는 기존 `PREPARE_MODEL_COHORT` inbox를 claim한다. parent event 하나를
  처리한 뒤 catalog 전체를 재스캔하므로 fan-in의 마지막 부모가 완료될 때만 child cohort가 생긴다.
- REUSE slot은 과거 result를 현재 실행으로 재표기하지 않는다. 현재 generation partition의
  MODEL_READY event가 원본 result와 `origin_generation_id`를 보존하도록 별도 outbox를 만든다.
- model result와 MODEL_READY outbox는 기존 `commit_result` transaction을 유지한다. Asset emit 실패는
  계산 재실행이 아니라 outbox 재발행으로 복구한다.
- delivery/work 완료는 실제 downstream durable 인계 또는 model result commit 뒤에만 기록한다.

필요한 DDL 보정은 적용된 v1–v6를 수정하지 않고 v7에만 추가한다. 가능한 기존 table/constraint를
재사용하고 runtime role에는 필요한 열의 최소 권한만 준다.

## source/deployment gate 경계

현재 registry의 15 SOURCE_GATE와 1 DEPLOYMENT_GATE는 생략하지 않는다.

- deployment gate는 validated deployment ID/manifest/model version과 deployment 소유 test 증거를
  묶은 receipt다.
- source gate는 plan의 exact source snapshot ID/cutoff와 source 소유 test 증거를 묶은 receipt다.
- root cohort와 최종 publication 준비 판정은 필요한 gate receipt가 없으면 실패한다.
- 동일 binding의 기존 실행 증거를 재사용할 때 현재 generation 확인 row와 원본 증거 ID를 함께
  보존한다.

B1-3에서는 synthetic fake dbt gate 증거까지 구현·검증하며 실제 Snowflake source test는 B2에서
실행한다.

## 코드 구성

- `pipelines/orchestration/model_dag_runtime.py`: event resolve, registry lookup, cohort/inbox/result 조립
- `orchestration/airflow/dags/pop_talk_model_factory.py`: parse-time 고정 deployment에서 7 DAG 생성
- `orchestration/airflow/dags/pop_talk_model_dispatcher.py`: plan/cohort 준비와 parent delivery 처리
- `orchestration/airflow/dags/pop_talk_model_asset_reconciler.py`: outbox/inbox 재구동
- `pipelines/orchestration/sql/007_*.sql`: gate 또는 cohort delivery 무결성에 꼭 필요한 보정만 추가
- 순수 계약, 실제 PostgreSQL, DAG parse/snapshot, Airflow partition dispatch 테스트를 분리한다.

## 구현 순서와 완료 기준

1. runtime/event/store 계약과 synthetic diamond 테스트
2. 공통 factory로 current 7-model DAG 렌더링 및 task/selector snapshot 검사
3. dispatcher/reconciler와 실제 Airflow 3.3.1 partition g1/g2 dispatch 검사
4. 전체 기존 suite와 실제 PostgreSQL 동시성 검사
5. 검토 승인

완료 기준은 다음과 같다.

- current deployment에서 정확히 7개 model DAG가 생성되고 각 DAG에 model task 하나와 owned-test
  task 하나만 존재한다.
- 전체 layer barrier 없이 direct parent fan-out/fan-in이 동작한다.
- g1/g2 역순·중복 event가 서로의 cohort/result를 오염시키지 않는다.
- REBUILD/REUSE/source/deployment binding 누락·교환·stale fence를 거부한다.
- emit/ACK/claim 전후 crash에서 dbt 실행 또는 result commit이 중복되지 않는다.
- 기존 production relation, serving publication, `pop_talk_movie_databricks_daily`는 변경하지 않는다.
- 설치된 Airflow 3.3.1에서 실제 partition key가 DagRun에 전달되는 것을 확인한다.

## 구현 중단 기준

검토에서 발견된 문제가 B1-3 제어 흐름의 데이터 오염·중복 SQL·증거 누락을 만들 때만 차단 수정한다.
운영 편의 기능, B2 업무 모델링, 일반적인 알림/관측성 개선은 후속 목록에 기록하고 B1-3을 확장하지
않는다.
