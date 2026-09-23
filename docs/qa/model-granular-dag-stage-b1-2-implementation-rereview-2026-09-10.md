# B1-2 구현 1차 보완 재검토

판정: **CHANGES_REQUESTED**. 이전 핵심 반례는 보완됐지만 실제 wrapper의 실패 상태 영속화와 부모 경로 no-follow 처리가 남았다. 실행·복구 통합 검증도 완료 기준을 아직 충족하지 않는다.

## 독립 검증 결과

- 새 Airflow 컨테이너에서 실제 PostgreSQL 검사를 활성화한 전체 suite: **148 tests / 16.826s / OK**, 종료 코드 0.
- DB migration ledger와 로컬 001–006 SHA-256 모두 일치. v6 digest는 요청된 `41476deeeacaa74540ac8971fa4601f34eca5eaf9f0285c3187b5eacba34b916`이며 변경하지 않았다.
- 이전 extra bare selector 반례: 이제 실행 전 거부.
- 취소 signal 후 child exit 0 반례: 이제 성공 반환 대신 거부.
- 0.35초 멈춘 heartbeat, 0.05초 deadline 반례: 약 **0.053초**에 child 중단. 이전 동기 I/O 지연 문제가 해결됐다.
- FILE_SEALED 복구에 원본 run_results/log 및 ID/status/digest 대사가 추가됐고 관련 회귀가 통과했다.
- registry가 validated deployment에서 재생성되며 v6의 manifest/model-version FK 연결이 추가됐다. caller의 두 test 목록을 함께 비우는 기존 경계는 제거됐다.

추가 검증은 임시 파일과 기존 synthetic PostgreSQL fixture를 사용했다. 실제 dbt 업무 SQL은 실행하지 않았다. fixture는 exact-ID cleanup으로 정리했고 구현/DDL은 수정하지 않았다.

## 1. [P2] 실제 wrapper 실패 기록이 외부 transaction rollback으로 사라짐

근거: postgres_control_store.py:919의 current_dbt_invocation_lease_expires_at; scripts/dbt_strict_runner.py:436, :506, :552–564.

lease 조회 함수가 transaction context 없이 cursor SELECT를 실행하므로 psycopg 기본 모드에서 implicit transaction을 연다. main은 이 connection을 subprocess 실행 동안 유지한다. 이후 except에서 set_dbt_invocation_state로 기록한 FAILED는 중첩 transaction/savepoint 안에 들어간다. 예외를 다시 던져 connection context를 빠져나오면 외부 transaction이 rollback되어 FAILED 변경이 사라진다.

독립 재현: 실제 runtime DB 연결과 main()을 사용하고 dbt executable만 exit 1인 임시 실행 파일로 교체했다. 배포 filesystem 검증은 synthetic fixture에 맞춘 테스트 대역을 사용했지만, 예약·lease 조회·실패 CAS·connection 종료는 실제 store와 PostgreSQL 경로다. 결과는 다음과 같다.

```text
ACTUAL_MAIN_FAILURE dbt가 실패했습니다: exit code 1
DURABLE_RESERVATION_AFTER_EXIT_1 ('RUNNING', None)
```

실행은 끝났는데 reservation은 RUNNING으로 남고 failure_reason도 없어, 복구자가 실행 중/실패를 잘못 판단한다. 같은 invocation 재요청도 실행 중이라는 경로로 거절된다. lease가 만료된 뒤 새 attempt로 넘길 수는 있지만 승인한 명시적 실패 상태 전이가 보존되지 않는다.

수정: lease 조회의 독립 transaction을 닫아 subprocess 동안 transaction을 열어두지 않는다. main의 실패 처리에서는 남은 transaction을 먼저 정리하고 별도 commit 경계/connection에서 current fence 조건의 FAILED 또는 ABANDONED를 확정한다. 다른 호출자의 외부 transaction을 helper가 임의 commit하지 않도록 store API와 main의 transaction 소유권을 명시한다.

회귀: main을 실제 DB로 실행하여 exit 1, validation failure, heartbeat/cancel failure 후 별도 connection에서 상태와 failure_reason을 읽는다. 파일 봉인 이후 응답 유실 복구 상태까지 같은 transaction 경계에서 확인한다. 이 수정 자체에 기존 v6 DDL 편집은 필요하지 않다.

## 2. [P2] no-follow가 최종 파일만 검사해 부모 symlink를 따라감

근거: dbt_invocation_contract.py:206–212; strict runner의 validate_recovery_artifact 호출 경로.

os.open(path, O_NOFOLLOW)는 마지막 파일의 symlink만 거부한다. invocation/logs 또는 target 디렉터리가 symlink면 그 부모를 따라가 regular file을 연다. invocation_directory의 사전 검사는 invocation 디렉터리까지이며 그 아래 logs/target에는 적용되지 않는다.

임시 invocation/logs를 과거 증거 디렉터리로 연결한 뒤 read_regular_file_nofollow를 호출해 **PARENT_SYMLINK_READ old evidence**를 확인했다. 이는 전체 복구 성공을 재현한 것은 아니지만 계획의 부모/파일 no-follow 보장이 빠졌음을 직접 보여준다. target과 logs를 함께 과거 파일로 연결하면 bytes/native ID 대사만으로 현재 invocation 경로 격리를 보장할 수 없다.

수정: 신뢰된 root directory descriptor에서 각 부모를 O_DIRECTORY/O_NOFOLLOW 및 dir_fd로 순차 열고 최종 파일도 같은 방식으로 읽는다. 지원 환경의 동등한 안전한 경로 API도 가능하다. receipt/run_results/log를 모두 같은 경계에 둔다.

회귀: 최종 파일 symlink뿐 아니라 logs/target 및 중간 부모 symlink, 생성 이후 경로 교체를 포함한다.

## 실제 wrapper/fake executable 및 crash matrix가 필요한가?

**실행과 복구의 의미를 입증하는 통합 검증은 B1-2 완료에 필요하며 현재 보완만으로 대체되지 않는다.** 특정 테스트 이름이나 구현 방식 자체를 요구하는 것은 아니다. 동등한 증거를 내는 통합 harness로 바꿀 수 있다. 그러나 reservation 반환값 count, 개별 helper 테스트, fake JSON serializer 검사만으로 실제 command 실행 횟수와 transaction/파일 경계를 입증할 수는 없다. 이번 exit 1 반례가 그 차이를 보여준다.

최소 검증 범위:

1. 실제 PostgreSQL에 두 wrapper가 같은 attempt/kind로 진입할 때 fake executable의 실제 실행 표식이 한 번만 생긴다. 동일 ID replay와 다른 ID conflict를 구분한다.
2. RESERVED 뒤 실행 전, RUNNING 중, 파일 봉인 뒤 DB 등록 전, DB commit 응답 유실, COMPLETED 뒤 파일 유실 각각에서 상태·재실행 횟수·성공 증거 유무를 확인한다. 실패/만료 상태를 성공으로 복원하지 않는다.
3. 실제 wrapper의 실패·취소·heartbeat 처리 후 별도 DB connection으로 durable 상태를 확인한다.
4. 동일한 실제 dbt invocation이 만든 JSON log와 run-results v6를 함께 대사하는 로컬 fixture를 실행한다. 현 테스트의 serializer 생성 결과와 별도 dbt parse 로그는 각각 유용하지만 그 조합을 대신하지 않는다. 업무 Snowflake 실행은 필요 없다.

현재 테스트 증가분은 앞선 반례를 막는 유효한 회귀다. 다만 전체 wrapper/crash matrix가 없는 상태에서 B1-2 구현 완료로 승인하기에는 위 추가 근거가 부족하다.

## 결론

수정된 allowlist, cancellation latch, 독립 heartbeat deadline, 원본 artifact 재검증, validated registry와 v6 바인딩은 수용한다. 남은 두 코드 문제를 고치고 실행·복구 통합 검증을 보강한 뒤 재검토한다. 새 schema 보정이 필요해지면 적용된 v6를 보존하고 v7에 반영한다.
