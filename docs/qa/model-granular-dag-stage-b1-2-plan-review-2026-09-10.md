# B1-2 구현 전 설계 검토

판정: **CHANGES_REQUESTED**. invocation artifact와 최종 execution receipt를 분리하는 방향은 수용한다. 다만 아래 실행 ID, 실행 환경 및 중복 실행 경계를 먼저 명확히 해야 한다. B1-1 승인은 유지한다.

## 검토 근거와 범위

계획 문서, scripts/dbt_cosmos_runner.py, dbt_deployment.py, dbt_model_registry.py, B1-1 store 및 migration, 현재 Cosmos DAG와 compose volume 구성을 읽었다. 컨테이너에 설치된 dbt-core/dbt-common과 Cosmos 소스에서 CLI invocation reset, run-results 직렬화, 환경변수 옵션, subprocess 및 후처리 동작을 직접 확인했다. 실제 Snowflake query나 업무 DAG는 실행하지 않았다. 구현 코드도 수정하지 않았다.

현재 불변 deployment `bf08df4b8ce3a5412d7931e5ed391e32d9772b0bb2cc624d70cbf2d5005ba2a8`을 validate_deployment로 검증하고 registry를 재생성했다. 모델 7개, 테스트 43개이며 소유권은 MODEL_GATE 27개 / SOURCE_GATE 15개 / DEPLOYMENT_GATE 1개다.

아래는 아직 구현되지 않은 설계의 실패 시나리오다. 현재 코드에서 모두 재현한 구현 결함이라고 해석하지 않는다.

## 1. [P1] 사전 contract ID와 dbt native invocation ID가 혼용됨

계획 §3은 subprocess 이전 invocation_id를 지정하고 §5는 run_results metadata ID가 그 값과 같도록 요구한다. 그러나 설치본 dbt_common/invocation.py는 UUID를 자체 생성하며 dbt/cli/requires.py:96–97은 CLI 호출마다 reset_invocation_id를 실행한다. 현재 제시한 CLI/env 계약에는 이 값을 사전 지정하는 지원 경로가 없다. fake executable이 contract ID를 그대로 쓰면 통과하지만 실제 dbt 성공 결과는 ID 불일치로 거절될 수 있다.

수정: orchestration invocation ID와 dbt_invocation_id를 별도 필드로 정의한다. 전자는 실행 예약·디렉터리·멱등성 ID, 후자는 해당 자식 프로세스의 JSON 로그/이벤트에서 관측한 native ID로 취급하고 fresh run_results.metadata.invocation_id와 대사한다. 지원되는 방식으로 native ID를 미리 고정하려면 설치본에서 실제로 가능한 경로를 입증해야 한다. artifact에서 읽은 값을 같은 artifact 값과 비교하는 자기 대사는 피한다.

추가로 설치본 run-results v6의 RunResultOutput은 unique_id/status를 포함하지만 resource_type 필드는 포함하지 않는다. resource type은 검증된 deployment manifest의 해당 unique_id에서 조회해야 한다. EMPTY_TEST_SET에는 native subprocess ID/run_results가 없다는 nullable/discriminated 계약도 명시한다.

필수 검증: 설치본 native ID 생성과 실제 v6 직렬화 모양에 맞춘 fixture 또는 warehouse 접속 없는 호환성 검사. resource_type을 임의로 추가한 fake JSON만으로 완료하지 않는다.

## 2. [P1] argv 제한만으로는 동일 selector의 실행 의미를 고정하지 못함

계획 §3–4는 target/log/state/defer argv 제한과 두 path 환경변수 강제만 구체화했다. 설치본 dbt/cli/params.py에는 DBT_DEFER, DBT_STATE, DBT_DEFER_STATE, DBT_INDIRECT_SELECTION, DBT_WRITE_JSON 등이 있고 Cosmos local은 append_env=True가 기본이다. 따라서 argv가 exact해도 상속된 defer/state가 ref 해석을 다른 manifest로 바꿀 수 있다. 모델 unique_id/status는 그대로여서 결과 ID 검사로 이 우회를 찾을 수 없다.

수정: subprocess에 넘길 환경을 명시적으로 구성하고 dbt 실행 의미를 바꾸는 환경·옵션을 제거하거나 계약값으로 고정한다. DBT_TARGET_PATH/DBT_LOG_PATH 강제는 설치본에서 지원되므로 타당하되, write-json=true, indirect-selection=empty, defer/state/partial-parse 및 vars의 허용 정책까지 정한다. 인증·연결에 필요한 환경은 보존하되 argv hash만으로 환경까지 봉인됐다고 보지 않는다. vars를 허용하면 cohort/source/relation에 영향을 주는 값은 권위 계약에서 파생한다.

strict/legacy 분기도 contract 환경변수 존재 여부만으로 정하지 않는다. 별도 strict entrypoint 또는 고정된 실행 모드로 신규 task를 식별하고, contract 누락·빈 문자열·파싱 실패는 dbt 실행 전 실패시킨다. legacy는 명시된 기존 task만 허용하고 strict ledger 증거를 생성할 수 없게 한다. 기존 wrapper의 os.execv 경로는 legacy에 유지할 수 있다.

필수 검증: argv가 정상인 상태에서 오염된 DBT_DEFER/STATE 등 환경을 주입한 경우, strict contract 제거, 여러 selector 값·별칭·중복 옵션. subprocess 실행 후 결과 거절만이 아니라 실행 전 차단을 확인한다.

## 3. [P1] 성공 증거 유일성만으로 동일 attempt의 중복 SQL 실행을 막지 못함

계획 §4는 invocation_id마다 다른 디렉터리를 만들고 §6은 같은 attempt/kind의 성공 증거를 하나로 제한한다. 서로 다른 invocation ID를 가진 두 MODEL 호출이 동일한 유효 claim/fence를 사용하면 각 디렉터리를 만들고 동시에 같은 attempt relation에 SQL을 실행할 수 있다. 실행 후 두 번째 성공 INSERT를 거절해도 첫 번째 결과/테스트를 뒤늦은 SQL이 변경할 수 있다. attempt 간 relation 격리로 해결되지 않는 attempt 내부 경합이다.

수정: subprocess 시작 전에 `(build_id, attempt_no, fence_token, kind)`의 실행 예약을 원자적으로 확정한다. 같은 ID는 기존 상태를 조회하고 다른 ID는 실행 전에 거절한다. 실패하거나 결과가 불확실한 MODEL을 같은 attempt에서 새 ID로 재실행할 수 있는지 명시하되, 원격 query 종료가 불확실하면 새 fence/relation의 attempt로 넘긴다. OWNED_TESTS는 같은 attempt의 성공 MODEL 증거를 선행 조건으로 둔다.

예약/진행 상태가 필요하면 immutable artifact 표와 별도 mutable invocation 상태 표 또는 명확한 기존 ledger 경계를 사용한다. 최종 성공 artifact의 불변성은 유지한다. 성공 INSERT의 unique 제약만 예약으로 대체하지 않는다.

필수 검증: 두 connection/두 wrapper가 같은 attempt/kind에 다른 ID로 동시에 진입할 때 fake executable 실행 횟수가 1회인지 확인한다. test가 MODEL 증거 전에 실행되지 않는지도 확인한다.

## 4. [P2] supervisor의 종료·갱신 실패를 유한 시간 안에 처리할 계약이 필요함

계획 §5의 subprocess.run과 §7의 주기 heartbeat는 함께 구현할 수 있지만, 별도 thread/connection인지 또는 Popen poll loop인지 정해져 있지 않다. heartbeat DB 호출 자체가 멈추거나 stdout drain이 막히면 lease를 잃은 채 자식이 계속 실행될 수 있다. 설치본 Cosmos hook은 wrapper를 setsid로 시작하고 on_kill은 process group에 SIGINT/SIGTERM을 전달한다. wrapper가 새 세션을 만들거나 신호를 삼키면 기존 취소 동작도 달라진다.

수정: Popen 기반 supervisor 또는 동등한 실행 수명 구조를 명시한다. 실행 전 claim 검사, 별도 heartbeat 연결과 connect/statement/lock timeout, 마지막 갱신 기준 deadline, 비차단 로그 drain, SIGINT/SIGTERM 전달, 종료 grace 후 kill/wait, 취소·갱신 실패 이후 성공 증거 등록 금지를 정의한다. 원격 query 취소 미확인은 새 attempt relation 격리로 처리한다. 모델 task와 test task 사이 queue 대기 중 lease가 만료되면 이전 fence를 재사용하지 않고 새 attempt로 전환하는 정책도 적는다.

필수 검증: heartbeat 예외뿐 아니라 응답 지연, 신호 무시 자식, 외부 SIGTERM, 큰 stdout, subprocess 종료와 heartbeat 실패 경합. 이는 B1-2 wrapper 수명 검증이며 Airflow Asset factory 구현을 앞당길 필요는 없다.

## 5. [P2] artifact 영속 위치와 DB commit 응답 유실의 복구 순서가 빠짐

계획의 /opt/airflow/dbt-attempt-artifacts는 현재 compose에서 영속 volume으로 연결돼 있지 않다. 컨테이너 재생성/다른 worker에서 DB 성공 row는 남지만 artifact가 사라질 수 있다. 또한 '기존 디렉터리 + DB 성공 receipt면 replay' 규칙은 디렉터리가 없는 상태의 DB 성공 replay, 파일 봉인 후 DB 미등록, DB commit 성공 후 응답 유실을 각각 정의하지 않는다. atomic rename은 파일 교체 원자성일 뿐 불변성이나 crash durability를 자동 보장하지 않는다.

수정: B1-2에서 사용할 공유 영속 volume 또는 immutable object 저장소와 보존 기간을 정한다. wrapper는 실행 전에 DB 예약/완료 상태를 우선 조회하고, 성공 증거가 있으면 파일 유무와 관계없이 SQL을 재실행하지 않는다. artifact 유실은 증거 조회 실패/복구 대상으로 구분한다. 파일 봉인→DB artifact 등록 순서와 각 crash 지점의 상태 전이를 정의하며, read-only exact replay와 만료 claim의 신규 성공 등록은 분리한다.

symlink 거부는 resolve 결과가 allowlist 안인지 보는 것만으로 끝내지 않고 생성·열기 대상의 링크 여부를 검사한다. 신뢰된 비공유 writable 디렉터리를 전제하는지, 동시 교체까지 막는 no-follow/open 방식이 필요한지도 명시한다. 최종 receipt를 overwrite하지 않는 봉인 방식과 crash durability 수준을 적는다.

필수 검증: 성공 DB row/로컬 파일 없음, 파일 봉인 후 DB 실패, commit 응답 유실, 같은 ID/다른 body, 재시작·다른 worker, symlink/기존 파일. 실제 외부 업무 데이터는 필요 없다.

## 6. [P2] fake 실행과 7-model snapshot만으로 실제 호환성 및 43-test gate를 보장할 수 없음

현재 모델별 owned test 합계는 27이다(dim 4, fact 4, mart_boxoffice 3, mart_quality 5, stg_boxoffice 4, stg_movie 5, stg_publications 2). source test 15개와 deployment test 1개는 새 MODEL/OWNED_TESTS 계약 대상이 아니다. 이 16개가 별도 gate에서 증명되는 시점·범위를 적지 않으면 기존 project_test 제거 시 검증이 빠질 수 있다.

수정: 43개 전체 소유권 대사와 B1-2의 모델 test 실행 대상 27개를 구분한다. source/deployment gate 증거의 담당 후속 단계와 READY/publication 전제 조건을 명시한다. B1-2에서 이 gate 전체를 구현할 필요는 없지만 cutover 완료 기준에서는 누락될 수 없어야 한다.

또한 Cosmos local.py:610–613의 run_results XCom 조회는 `<temporary_project>/target/run_results.json`을 사용하고, compiled SQL 및 lineage 후처리도 임시 target을 기대하는 경로가 있다. 외부 artifact 경로를 강제하면 dbt 자체는 성공해도 활성화된 Cosmos 후처리가 실패하거나 누락될 수 있다. strict operator의 SUBPROCESS 고정, 자동 deps/partial parse/자동 test/자동 Asset 및 후처리 옵션을 명시하고, receipt는 wrapper/ledger에서 읽도록 연결한다. 필요하면 기존 Cosmos 후처리를 비활성화하거나 읽기 경로를 조정한다.

필수 검증: 설치본 Cosmos가 조립한 command/env를 strict wrapper에 전달하는 작은 synthetic smoke test, dbt v6 형식 fixture 및 native ID 확인, 43개 ownership 합계 검사. fake executable로 장애 경계를 검사하는 것은 적절하지만 자체 fake 형식만 확인하는 테스트로 호환성 완료를 선언하지 않는다. 실제 Snowflake 업무 실행은 계획대로 B2에 남겨도 된다.

## 수용 사항

- MODEL/OWNED_TESTS artifact 분리, exact ID 집합 및 중복 검사, model success/test pass만 허용하는 정책은 적절하다.
- immutable invocation 증거와 relation 검증을 포함하는 최종 execution receipt의 분리는 타당하다. 최종 receipt는 같은 attempt/fence의 MODEL + OWNED_TESTS/EMPTY 증거를 참조해야 한다.
- current 미만료 claim, exact cohort/deployment/model 및 registry expected 집합을 DB 등록에서 다시 검사하는 방향은 수용한다. self-seal은 입력 일관성 검사이며 승인 manifest의 권위를 대체하지 않는다.
- B1-2 artifact만으로 result READY를 commit하지 않는 범위 설정은 적절하다.

위 항목을 계획에 반영한 뒤 구현으로 진행하는 것을 권고한다. 이 문서는 구현 승인이나 실제 dbt/Snowflake end-to-end 실행 성공 판정이 아니다.
