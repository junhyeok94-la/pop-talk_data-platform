# B1-2 2차 보완 최종 검토

판정: **APPROVED** — 남은 두 P2가 해결됐고, 추가 독립 wrapper 복구 검증을 포함하여 B1-2 기반 구현을 승인한다. B1-3 통합 단계로 진행할 수 있다.

## 두 P2 해결 확인

- lease 조회가 명시적 transaction context에서 종료된다. main의 해당 호출 시점에는 남은 외부 transaction이 없어 이후 실패 CAS가 독립 commit된다. 실제 main + runtime PostgreSQL + exit 1 fake executable 테스트에서 별도 connection으로 **FAILED / RuntimeError**가 유지됨을 확인했다. 호출자가 이미 transaction을 가진 경우에는 savepoint로 동작하며 caller transaction을 임의 commit하지 않는다는 범위도 유지된다.
- no-follow reader가 filesystem root dirfd부터 모든 부모를 O_DIRECTORY/O_NOFOLLOW로 열고 최종 파일도 같은 경계에서 읽는다. 부모 logs symlink 반례가 거부된다. 지원하지 않는 플랫폼에서는 실행을 거부하며, 이번 검증 대상 Linux/Airflow에서는 정상 파일 읽기 회귀도 통과했다.

## 독립 검증

새 Airflow 컨테이너에서 실제 PostgreSQL 검사를 활성화한 전체 suite를 실행했다. **150 tests / 14.738s / OK**, 종료 코드 0. 두 P2 회귀와 기존 selector·취소·heartbeat·registry·artifact·Cosmos 검증을 포함한다. 이번 수정에서 migration 변경은 없으며 DDL을 실행하거나 수정하지 않았다.

완료 판단을 위해 별도로 실제 main/store/PostgreSQL에 임시 fake dbt executable을 연결했다. synthetic fixture의 배포 filesystem 검증만 테스트 대역으로 대체했고, child 실행과 예약·상태·artifact DB 경로는 실제 구현을 사용했다. 파일 봉인 후 artifact 등록 직전에 예외를 주입하고 같은 invocation을 재호출했다.

| 경계 | 별도 DB 조회 결과 | fake executable 누적 실행 |
| --- | --- | --- |
| 파일 봉인 후 DB 등록 실패 | FILE_SEALED | 1 |
| 같은 invocation 복구 | COMPLETED | 1 |
| 완료된 invocation 재조회 | COMPLETED | 1 |
| 완료 후 receipt 파일 유실 | COMPLETED / ARTIFACT_MISSING | 1 |

즉 실제 wrapper에서 증거 복구와 완료 replay가 SQL 재실행으로 이어지지 않음을 확인했다. 마지막 파일 유실도 완료 증거를 유지하고 복구 대상으로 표시했다. 임시 파일 및 synthetic DB fixture는 exact-ID cleanup으로 정리했다. 업무 Snowflake SQL은 실행하지 않았으며 구현 코드도 수정하지 않았다.

## 검증 조합과 승인 범위

실제 main 실패 통합 테스트와 helper 검사만으로 모든 crash 상황이 검증됐다고 보지는 않는다. 다만 이번 독립 main 복구/실행 횟수 검사, 기존 실제 PostgreSQL 예약 경쟁·immutable replay, supervisor 장애 회귀를 합치면 **B1-2의 예약·실행기·불변 증거 기반을 다음 단계로 넘기기에 충분하다**. 전체 crash matrix를 모두 B1-2 종료 조건으로 유지하지 않는다. 직전 검토에서 요구한 통합 증거를 이번 최소 추가 검사로 보강한 판단이다.

남은 필수 통합 검증은 B1-3 완료 및 실제 cutover 전에 다음 최소 범위로 수행한다.

1. 실제 Airflow task/worker 두 개의 경쟁·재시작에서 동일 attempt의 executable이 한 번만 실행되는지 확인한다. 현재 PostgreSQL 경쟁 테스트는 예약 소유권을 검증하며 두 실제 wrapper의 동시 실행까지 확인한 것은 아니다.
2. 실행 전/실행 중 worker 강제 종료 후 lease 만료·새 fence 전환과 성공 증거 오인 방지를 확인한다. 이번 검사는 파일 봉인 후 등록 실패를 주입한 것이며 모든 process kill 시점을 검증한 것은 아니다.
3. 실제 dbt run/test 한 invocation이 생성한 JSON log와 run-results v6를 함께 대사한다. 현재 serializer + 별도 parse 검사는 이 통합 검증과 구분한다. 업무 Snowflake 전체 실행은 B2 범위를 유지한다.

SOURCE/DEPLOYMENT gate, REBUILD/REUSE publication barrier 및 실제 Snowflake relation 격리는 예정된 후속 단계에서 검증한다. 이번 승인은 운영 배포나 업무 7-model 전체 실행 완료 판정이 아니다.
