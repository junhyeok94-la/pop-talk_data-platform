# 단계 B1 계획 재검토

판정: **APPROVED — B1 설계 범위**.

최신 B1 계획 전체를 이전 검토의 네 필수 지적과 대조했다. 네 항목 모두 계획 수준에서 해결됐으며 미해결 차단 지적은 없다. 실제 구현·장애 복구·권한 검증이 완료됐다는 의미는 아니다. 코드와 설계 원문은 수정하지 않았고 외부 쓰기도 수행하지 않았다.

## 반영 확인

| 이전 지적 | 수정 계약과 판정 |
| --- | --- |
| stale worker가 권위 relation 변경 | 단조 fence token, current lease 조건, build/attempt/fence별 물리 relation 분리로 A 지연 완료가 B 결과를 변경하는 반례 해결 |
| 단일 ACK와 ACK 후 작업 유실 | required recipient별 delivery와 pending-work inbox의 원자 ACK, 미완료 inbox/slot/claim 복구, terminal no-consumer 완료 정책으로 해결 |
| search_path를 권한 경계로 사용 | B1부터 owner/runtime 분리, 전용 credential, non-owner/non-superuser 및 membership/privilege 검사로 해결 |
| 과거 receipt/artifact로 성공 위장 | cohort/deployment/parent vector/fence/invocation/artifact/query identity, fresh 출력 및 status/정확한 test 집합 대사로 해결 |

composite FK와 state/result 조합, REPEATABLE READ와 충돌 후 권위 본문 대사, diamond graph와 실제 동시 transaction/crash 검증 확대도 수용한다. 단계 A의 논리 manifest 검증을 저장 제약 및 claim transaction으로 연결하는 방향이 일관적이다.

## 구현 시 확인할 구체 경계

다음은 새 차단 요구가 아니라 승인된 설계를 실제 코드로 입증할 조건이다.

- 봉인된 attempt relation을 같은 attempt 재시도가 다시 쓰지 않도록 한다. 응답 유실 후에는 성공 result/receipt를 먼저 조회하고, 현재 lease/token을 모든 heartbeat·receipt·commit에 적용한다. GC 역시 봉인 참조와 진행 중 작업을 보호한다.
- ACKED는 durable work 인계이고 COMPLETED는 그 work 완료다. 재전송과 inbox recovery는 이를 구분하며, 다른 recipient의 ACK가 미완료 작업을 제거하지 않아야 한다. 모든 recipient가 인계받은 뒤에는 작업 recovery를 중심으로 처리해 불필요한 Asset 재발행을 제한할 수 있다.
- DB의 composite FK 참조 대상 UNIQUE와 attempt receipt 보존 구조를 migration에서 완성한다. 현재 attempt가 바뀌어도 과거 receipt의 참조 무결성이 깨지지 않게 한다.
- model과 test 실행의 run_results는 각각 별도 invocation으로 보존한다. 동일 attempt의 model 실행 뒤 test가 출력 파일을 덮어쓰는 경우도 피하거나 이전 artifact를 봉인해 유지한다. 명시적 EMPTY_TEST_SET은 예상 집합이 비었을 때만 허용한다.
- recovery publisher는 실제 partition key를 발행할 수 있는 producer 설정과 concrete Asset을 사용한다. 여러 generation을 한 번에 처리할 때 event body와 partition을 일대일로 연결한다. timetable/API import 성공을 실제 scheduler dispatch 검증으로 대체하지 않는다.
- claim 성공 후 별도 Cosmos 태스크가 실행되는 동안 lease 갱신이 지속되는지, task retry/clear가 같은 성공 attempt를 재실행하지 않는지 검증한다.
- runtime의 schema 격리는 실제 credential/role 권한으로 확인한다. migration owner 권한을 runtime이 상속하거나 전환할 수 없어야 한다.

## B1 승인 범위와 종료 조건

durable store, receipt/claim/outbox/inbox, 공통 factory 및 synthetic diamond 구현으로 진행할 수 있다. B1 완료에는 계획 8절의 실패 시나리오와 실제 PostgreSQL 동시성·Airflow partition dispatch가 필요하다. Snowflake 물리 relation 격리를 모형으로만 검증했다면 그 한계를 명시하고 실제 격리 실행을 별도 통합 gate로 유지한다.

업무 7-model SQL의 generation 적용과 실제 test 실행은 B2 범위다. 현재 registry 렌더링 통과나 B1 승인으로 기존 production DAG 교체·serving activation까지 완료했다고 간주하지 않는다. 이전 B1 CHANGES_REQUESTED 보고서는 검토 이력으로 보존한다.
