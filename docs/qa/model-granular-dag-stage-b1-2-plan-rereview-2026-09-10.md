# B1-2 수정 계획 재검토

판정: **CHANGES_REQUESTED**. 이전 지적의 주요 실행 경계는 반영됐다. 아래 두 항목을 보완하면 구현으로 진행할 수 있다. B1-1 승인은 유지한다.

## 검토 범위

수정 계획 전체를 이전 검토 및 승인된 REUSE 계약과 대조했다. 추가로 컨테이너의 dbt CLI 로그 옵션 기본값을 읽기 전용으로 확인했다. 이번에는 설계 검토만 수행했으며 구현 코드 수정, 업무 SQL 실행, 전체 테스트 재실행은 하지 않았다.

## 1. [P1] 동일 generation의 실행 증거 요구가 REUSE와 충돌함

근거: 계획 §8의 “publication assembler가 동일 generation의 27+15+1 검증 증거를 모두 요구”한다는 문장.

실패 시나리오: g1에서 성공한 모델을 무변경 g2가 REUSE하면 g2에서는 새 모델 실행 cohort 및 MODEL/OWNED_TESTS invocation을 만들지 않는다. 기존 model_generation_contract.py도 REUSE 모델의 새 cohort 생성을 거부한다. 그런데 assembler가 모든 증거의 실행 generation을 g2로 제한하면, 정상 무변경 배치와 일부 모델만 재구축하는 배치의 publication이 영원히 대기하거나 불필요한 재실행을 요구한다. 이는 단계 A와 B1-1에서 승인·검증한 과거 result 재사용 계약과 맞지 않는다.

구체 수정안:

- publication 검증의 기준은 소비 generation의 plan과 exact result/source binding이다. 모든 증거가 그 generation에서 새로 실행됐어야 한다는 조건으로 표현하지 않는다.
- REBUILD slot은 현재 attempt/fence의 result 및 model/test 증거를 요구한다.
- REUSE slot은 plan.reuse_result_manifest_id가 지정한 원본 result의 검증된 receipt와 test 증거를 provenance 경로로 참조한다. 소비 generation과 원본 실행 generation을 모두 보존하며 과거 receipt를 현재 generation으로 재표기하지 않는다.
- SOURCE_GATE는 소비 plan의 exact source snapshot/cutoff에, DEPLOYMENT_GATE는 검증된 deployment/manifest에 연결한다. 동일한 binding의 증거 재사용 여부를 명시한다. 현재 generation의 gate 확인 row를 별도로 만들더라도 원본 증거를 참조하는 확인 기록과 새 실행 증거를 구분한다.
- “27+15+1”은 현재 deployment의 테스트 소유권 분포다. assembler는 고정 숫자가 아니라 해당 deployment registry의 exact required test 집합을 검사한다.

필수 검증: 무변경 g1→g2→g3 publication 준비, 변경 branch와 과거 REUSE branch 결합, 잘못된 과거 result/source/deployment 증거 거부. B1-2에서는 계약/계획을 확정하고 실제 assembler 검증은 B1-3에서 수행해도 된다.

## 2. [P2] native ID 관측에 필요한 JSON 파일 로그 설정이 빠짐

근거: 계획 §3의 고정 DBT_* 환경 목록, §5의 JSON log event 대사, §7의 stdout/stderr 상속.

설치본 dbt/cli/params.py:333–338에서 DBT_LOG_FORMAT_FILE의 기본값은 debug다. 수정 계획은 DBT_*를 고정 목록 외 전부 제거하지만 그 목록에 JSON log format이 없다. stdout도 wrapper가 읽지 않고 Airflow로 상속한다. 따라서 명시된 설정만 구현하면 wrapper가 파싱할 JSON 이벤트가 logs/dbt.log에 보장되지 않아 native ID 대사가 실패한다. fake executable이 항상 JSON을 쓰면 이 차이를 놓칠 수 있다.

구체 수정안: wrapper 소유의 DBT_LOG_FORMAT_FILE=json을 강제하고 caller의 관련 CLI override를 거부한다. 종료 후 exclusive invocation 경로의 dbt.log에서 native ID를 읽는다고 명시한다. 필요한 이벤트를 남길 log level도 고정한다. stdout 형식은 Airflow 로깅 목적에 맞게 별도로 정할 수 있다. 로그 내 ID 누락·복수 ID·run_results와 불일치는 실패로 처리한다.

필수 검증: 정리된 환경으로 실제 설치본 fixture를 실행해 JSON 파일 로그의 native ID와 fresh run-results v6 ID가 일치하는지 확인한다. 기본 debug 설정 또는 caller log format 변경이 성공 경로로 들어오지 않는지도 검사한다.

## 수용한 수정

- orchestration/native ID 분리와 manifest 기반 resource type 판정, EMPTY의 nullable native/artifact 계약.
- 별도 strict/legacy entrypoint와 contract 누락 시 실행 전 실패, sanitized env 및 vars/옵션 제한.
- 실행 전 unique reservation, 같은 ID 복구·다른 ID 거부, MODEL 증거 후 TEST 예약. 동시 요청 시 executable 1회 검증이 완료 기준에 포함됐다.
- Popen supervisor, 별도 heartbeat connection의 timeout, 신호 전달·grace 후 kill/wait, 불확실 상태에서 성공 증거 금지, queue delay 만료 시 새 attempt 전환.
- 공통 영속 mount, no-follow 경로 검사, 파일 봉인 후 DB 등록 및 crash/response-loss 복구, DB 완료 후 artifact 유실 시 SQL 재실행 금지. 신규 등록은 current 미만료 claim을 요구하고 완료된 동일 증거의 read-only replay와 구분하는 원칙으로 구현해야 한다.
- reservation과 immutable artifact 분리 및 SQL 열/JSON body 대사, artifact만으로 READY를 commit하지 않는 범위.
- 27 MODEL / 15 SOURCE / 1 DEPLOYMENT 구분, 실제 dbt fixture 및 Cosmos command/env smoke 추가. source/deployment gate를 후속 단계에 명시한 방향은 적절하며 증거 binding은 위 P1에 맞게 보완한다.

이번 판정은 위 두 설계 항목에 한정한 수정 요청이다. 이전 여섯 지적을 전부 다시 열거나 B1-2에서 업무 Snowflake 실행까지 요구하는 것은 아니다.
