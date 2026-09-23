# B1-3 계획 검토

판정: **APPROVED** — 요청된 다섯 경계에서 구현을 차단할 구조적 문제는 발견하지 않았다. B1-3 제어 흐름 구현으로 진행할 수 있다.

## 검토 근거

계획 전체와 기존 scan_ready_cohorts, ModelReadyEventV2/build_model_ready_event, outbox/inbox API 및 B1-2 승인 범위를 대조했다. 설치된 Airflow 3.3.1에서 PartitionedAssetTimetable과 IdentityMapper import 및 생성자 서명을 직접 확인했다. 이번은 설계 검토이며 실제 scheduler dispatch나 업무 SQL 실행, 전체 suite 재실행은 하지 않았다. 코드와 DDL은 수정하지 않았다.

## 다섯 구조적 경계의 판단

| 검토 대상 | 판단과 근거 |
| --- | --- |
| dispatcher fan-in | 승인. parent event는 재평가 계기이고 실행 가능성은 plan/catalog의 exact direct-parent binding으로 결정한다. 전체 catalog 재스캔 자체는 layer barrier가 아니다. 각 child가 준비되는 즉시 cohort를 만들면 독립 branch가 느린 branch를 기다리지 않는다. |
| COHORT_READY 전달 | 승인. cohort/outbox/delivery 원자 등록과 resolver의 ACK/EXECUTE_MODEL inbox 원자 등록은 계산 준비와 durable 인계를 분리한다. Asset 전송 재시도를 모델 계산 재실행으로 취급하지 않는 구조도 타당하다. |
| REUSE provenance | 승인. 현재 소비 generation의 partition/plan과 원본 result/origin_generation_id를 함께 보존하는 방식은 기존 ModelReadyEventV2 계약과 맞는다. REUSE에 새 build 실행 증거를 만들 필요가 없다. |
| source/deployment gate | 승인. source snapshot/cutoff 및 deployment/manifest에 증거를 묶고 필요한 root 준비와 publication 판정에서 요구하므로 모델 소유 27개만 검사하고 나머지 16개를 생략하는 문제가 없다. synthetic gate 증거로 제어 흐름을 검증하는 범위도 적절하다. |
| 7 DAG factory / strict Cosmos | 승인. 고정 deployment registry에서 모델마다 단일 COHORT_READY Asset을 소비하고 exact model/owned-test task를 생성하는 방식은 기존 strict runner와 연결 가능하다. multi-parent Asset AND 조건을 각 model DAG에 직접 구현할 필요가 없다. |

## 구현·검증에서 유지할 최소 조건

아래는 새 범위 추가나 설계 재작성 요청이 아니라 계획의 인계/복구 계약을 구현할 때 확인할 항목이다.

- parent 하나의 inbox work를 완료했을 때 아직 준비되지 않은 child도, 마지막 parent 또는 필요한 gate 완료 뒤 다시 평가돼야 한다. gate 도착 후 재스캔과 plan 최초 등록 후 root/REUSE 스캔을 durable work 또는 주기적 reconciler로 연결한다. 준비되지 않은 다른 branch를 기다리는 task로 dispatcher를 점유하지 않는다.
- resolver ACK 뒤 task/worker가 죽은 경우에는 이미 ACK된 Asset을 기다리는 것만으로 복구하지 않는다. 남은 EXECUTE_MODEL inbox를 다시 claim해 동일 cohort를 이어가는 경로를 검증한다. MODEL_READY와 COHORT_READY의 URI/body/consumer/work-kind 계약은 구분한다.
- 같은 child를 여러 parent delivery가 동시에 발견해도 cohort/outbox는 한 번만 만들어져야 한다. g1/g2 역순·중복 및 REUSE event는 소비 plan 기준으로 대사한다.
- root gate는 해당 root가 실제 사용하는 source binding에 한정한다. unrelated source gate 전체 완료를 기다리는 공통 barrier로 바꾸지 않는다. 최종 publication의 전체 required set 검사는 별도다.
- 설치본 timetable의 두 번째 positional 인자는 mapper 객체가 아니라 asset→mapper dict다. 계획의 예시를 코드로 옮길 때 `PartitionedAssetTimetable(asset, default_partition_mapper=IdentityMapper())` 또는 올바른 mapper dict를 사용한다. import 성공과 실제 generation partition DagRun 전달 검증은 구분한다.
- synthetic diamond에서 빠른 branch의 진행, 마지막 direct parent fan-in, gate 도착 후 root 재개, ACK 후 worker 종료·inbox 재개를 확인한다. 기존 B1-2에서 넘긴 worker 경쟁/실행 중 종료 검증도 B1-3 완료 판단에 포함한다.

## 승인 범위

dispatcher → COHORT_READY → 모델별 DAG 및 reconciler의 제어 흐름 계획을 승인한다. 실제 구현과 Airflow partition dispatch 결과는 구현 완료 후 검토한다. B2의 실제 Snowflake SQL/vars, 운영 cutover, 알림 및 대시보드 요구는 추가하지 않는다.
