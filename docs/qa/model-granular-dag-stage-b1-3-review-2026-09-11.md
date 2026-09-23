# B1-3 구현 검토

판정: **CHANGES_REQUESTED**. direct-parent readiness와 REUSE event 본문의 방향은 타당하지만 실제 DAG/runtime 연결에 아래 차단 문제가 있다.

## 검증 범위

요청 문서와 변경된 runtime, factory/dispatcher/reconciler, gate/cohort/outbox/inbox store 및 v7, 관련 테스트를 읽었다. 새 Airflow 컨테이너에서 PostgreSQL 검사를 활성화한 pipelines 전체 suite는 **157 tests / 22.774s / OK**, 종료 코드 0이었다. 설치본 Airflow 3.3.1의 task-success outlet 등록 함수도 확인하고 DB 쓰기 없는 mock으로 동작을 재현했다.

검토 중 store에 REUSE의 PENDING event만 반환하는 변경과 동일 publisher SENDING 재claim 변경이 추가된 것을 마지막 읽기에서 확인했다. 따라서 157개 통과를 이 후속 변경까지 재실행한 결과로 표현하지 않는다. 아래 지적은 마지막 읽기에서 여전히 존재하는 경로다. 구현/DDL을 수정하거나 실제 업무 Snowflake SQL을 실행하지 않았다.

## 1. [P1] parent-event dispatcher와 receipt 검증이 implicit transaction 때문에 중단됨

근거: model_dag_runtime.py:152, :174 부근 및 :266–270; postgres_control_store.py의 delivery_consumers, load_dbt_invocation_artifact, _repeatable_read.

connect_control은 기본 격리 수준의 connection을 만든다. dispatcher가 delivery_consumers의 SELECT를 호출하면 implicit transaction이 열린다. 이후 register_reuse_outbox/materialize_ready_cohorts는 _repeatable_read에서 이미 열린 transaction의 격리 수준을 검사하고 거부한다. parent event가 있는 정상 경로가 root 수동 스캔과 달리 실패한다.

실제 runtime DB 연결에서 같은 호출 순서를 읽기 전용 재현했다.

```text
delivery_consumers(...) -> ()
register_reuse_outbox(...) -> ControlStoreConflict:
상위 transaction은 REPEATABLE READ여야 합니다
```

존재하지 않는 event/plan ID를 사용했지만 실패는 plan 조회 이전의 transaction 검사에서 발생했다. 실제 event가 있으면 중간 ACK/claim도 같은 외부 transaction에 들어가므로 이 문제를 해소하지 않는다.

verify_synthetic_execution도 artifact SELECT 두 번 후 load_catalog()를 호출하므로 같은 이유로 실패한다. 성공 model/test 증거가 있어도 result 검증까지 진행하지 못한다.

수정: runtime에서 필요한 RR transaction을 첫 조회 전에 명시적으로 소유하거나, 독립 read를 종료한 뒤 의도된 RR 단위에 들어간다. ACK/claim/cohort 생성/work 완료의 commit 경계를 유지하고 연결만 RR로 바꾼 뒤 장시간 transaction을 방치하지 않는다. 실제 runtime 함수로 parent event 처리와 성공 artifact→receipt 경로를 검증한다.

## 2. [P1] 선언된 모든 outlet에 빈 READY 이벤트가 자동 발행됨

근거: pop_talk_model_dispatcher.py:69의 outlets=ALL_OUTLETS; pop_talk_model_asset_reconciler.py:50의 outlets=READY_ASSETS.

두 task는 모든 model/cohort Asset을 outlets에 선언하고 그중 실제 event만 outlet_events에 채운다. 하지만 설치본 TaskInstance.register_asset_changes_in_db는 payload가 없는 선언 outlet에도 extra=None 이벤트를 등록한다(taskinstance.py:1587–1599, :1628–1639).

설치본 함수에 두 선언 outlet과 빈 payload 목록을 넣고 session/asset_manager만 mock으로 바꿔 실행했다. 결과는 두 Asset 모두 **extra=None, partition='gen-review'**로 발행됐다. 실제 Airflow metadata DB에는 쓰지 않았다.

따라서 dispatcher가 한 child만 준비했어도 다른 model/cohort에도 빈 READY가 발생한다. reconciler가 처리할 event 없이 return None으로 성공해도 같은 문제가 있다. resolver는 exact event envelope를 요구하므로 가짜 신호가 실패 DagRun을 만들거나 정상 event와 섞여 처리를 방해한다. dispatcher 자신이 소비하는 MODEL_READY도 발행 대상에 포함된다.

수정: 실제 선택한 Asset만 발행하는 설치본 지원 경로를 사용한다. 예를 들어 Asset별 emit task에서 미선택 task를 skip하거나, partition을 보존하는 명시적 event 발행 경로를 쓴다. 단순히 outlet_events를 채우지 않는 것으로 발행이 생략된다고 가정하지 않는다. 실제 task-success 처리에서 발행 0건/1건/여러 서로 다른 Asset을 검증한다.

## 3. [P1] 중복 parent event를 완료 work로 재claim해 dispatcher가 실패함

근거: model_dag_runtime.py:152–168; postgres_control_store.py의 delivery_consumers 및 claim_work.

delivery_consumers는 이미 완료한 PREPARE_MODEL_COHORT 수신자도 반환한다. rescan_generation은 매번 ACK 후 모든 수신자의 work를 무조건 claim하지만 claim_work는 COMPLETED를 허용하지 않는다. ACK/emit 응답 유실로 동일 event가 재전달되면 이미 처리한 recipient에서 실패한다.

특히 한 dispatcher run이 여러 parent event를 처리하다 일부만 완료하고 중단되는 경우, 재시도에서 완료된 첫 event 때문에 아직 처리하지 않은 event까지 진행하지 못할 수 있다. 여러 recipient 중 하나만 완료된 복구도 같은 경계다. 이는 SQL 조건과 runtime 호출 순서로 확인한 결함이며 전체 Airflow 중복 전달 재현은 하지 않았다. 위 RR 문제를 수정한 뒤 드러나는 독립 경로다.

수정: exact event/registry 대사는 유지하면서 recipient별 COMPLETED는 멱등 성공으로, 유효한 다른 claim은 비소유 상태로 처리하고, pending/만료 work만 claim한다. 어느 recipient가 이미 완료됐다는 이유로 전체 readiness 재평가를 차단하지 않는다. event 중복, 일부 recipient 완료, 여러 입력 event 중간 실패를 runtime 통합으로 확인한다.

## 4. [P1] synthetic 결과 플래그가 실제 업무 SQL 실행 이후에만 검사됨

근거: pop_talk_model_factory.py:169–202; scripts/dbt_strict_runner.py의 REAL_DBT_EXECUTABLE 및 prepare_strict_argv 경로.

factory의 model_run/owned_tests는 current deployment의 실제 project/profile과 strict executable에 연결된다. strict runner는 해당 SQL을 복사하여 실제 /opt/dbt-venv/bin/dbt를 실행한다. allow_b1_3_synthetic_result=false 검사는 두 Cosmos task가 끝난 뒤 verify_receipt에서만 수행된다. true여도 synthetic인 것은 최종 row-count/content 증거 생성이며 앞선 SQL 실행 대상은 바뀌지 않는다.

따라서 B1-3 제어 흐름 시험을 위해 이 DAG를 실행하면 현재 업무 SQL이 먼저 실행될 수 있다. 기본 false가 이를 막아주지 않는다. is_paused_upon_creation은 자동 시작을 막지만 실행 자체의 synthetic 격리 장치는 아니다. 이 경로를 확인하기 위해 실제 업무 SQL을 실행하지는 않았다.

수정: B1-3 시험은 명시적으로 격리된 synthetic deployment/profile/executable에 연결하고, 현재 업무 deployment에 대한 실행 차단은 model_run 이전에 둔다. 7개 current DAG 렌더링과 synthetic diamond 실행 검증을 분리해도 된다. Cosmos invocation_mode도 SUBPROCESS로 명시해 strict executable 경계를 고정한다. 이는 B2 SQL 구현 요구가 아니라 B1-3에서 제외한 업무 계산을 실행하지 않도록 하는 경계 보정이다.

## 수용한 부분과 최소 재검증

- cohort 후보는 각 model의 direct-parent binding으로 계산하며 unrelated source gate는 해당 root를 막지 않는다. 이 부분에 layer 전체 barrier는 발견하지 못했다.
- COHORT_READY row/outbox/delivery 원자 생성, ACK/EXECUTE_MODEL inbox 원자 인계의 store 구조는 타당하다.
- REUSE event는 소비 generation partition과 원본 result/origin_generation_id를 분리한다. 마지막 읽기의 PENDING-only 반환 보완도 확인했다.
- gate 등록의 plan/source snapshot 및 registry exact test 집합 대사는 존재한다. 다만 현재 제시된 증거는 DB gate receipt 처리이며 실제 gate 도착→dispatcher 재개까지 실행한 증거로 확대하지 않는다.

수정 후에는 factory parse/snapshot만 반복하기보다 synthetic parent→dispatcher→COHORT_READY→model runtime을 한 번 연결해 검증한다. g1/g2 partition 전달, 선택하지 않은 Asset 발행 0건, duplicate/부분 ACK 복구를 실제 Airflow 경계에서 확인해야 한다. 이번 증거 문서에는 실제 partition DagRun dispatch 결과가 없으며 import 성공과 이를 구분한다.

Snowflake 업무 SQL/vars, 운영 cutover, 알림·대시보드는 이번 승인 조건에 추가하지 않는다. 적용된 v1–v7은 보존하고 DDL 수정이 필요하면 후속 migration으로 보정한다.
