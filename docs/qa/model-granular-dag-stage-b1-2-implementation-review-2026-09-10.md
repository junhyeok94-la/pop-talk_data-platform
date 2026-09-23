# B1-2 구현 검토

판정: **CHANGES_REQUESTED**. 구조는 승인 계획을 따르지만 실행 범위, 취소 처리 및 복구 증거 검증에 차단 문제가 남았다.

## 독립 검증

구현 기록, v5 migration, invocation contract/store/strict runner, 관련 테스트, Dockerfile·compose·prepare script를 검토했다. 새 Airflow 컨테이너에서 RUN_DW_CONTROL_POSTGRES_TESTS=1로 전체 suite를 실행해 **144 tests / 17.345s / OK / 종료 코드 0**을 확인했다. DB ledger와 로컬 migration 001–005 SHA-256은 모두 일치했다. 적용된 v5는 변경하지 않았으며 DDL 보정이 필요하면 v6을 추가해야 한다.

별도 반례는 임시 디렉터리, 가짜 process/store, 설치본 CLI parser로 실행했다. 추가 업무 SQL 또는 잘못된 registry DB 등록은 하지 않았다. 통합 테스트의 fixture는 기존 rollback/exact-ID cleanup 경로를 사용한다. 코드 수정 없이 이 보고서만 추가했다.

## 1. [P1] 추가 selector 토큰이 검사를 통과해 다른 모델까지 실행됨

근거: scripts/dbt_strict_runner.py:130–132 및 _option_values.

_option_values는 --select 다음 토큰 하나만 읽는다. 예를 들어 `run --select fqn:p.a fqn:p.unexpected ...`에서 계약이 fqn:p.a이면 wrapper는 통과시키고 두 번째 bare token을 그대로 dbt에 전달한다. 임시 project로 prepare_strict_argv를 호출해 **EXTRA_SELECTOR_FORWARDED=True**를 확인했다. 설치된 실제 dbt Click parser도 이 argv를 **('fqn:p.a', 'fqn:p.unexpected')**로 해석했다. 실행 뒤 extra result를 거절해도 다른 모델 SQL은 이미 실행된 후다.

수정: dbt와 동일한 다중 값/별칭 규칙으로 전체 argv를 소비하고 미해석 토큰을 거부하거나, 검증된 계약에서 허용 command를 새로 구성한다. 원본 caller argv를 그대로 전달하는 blacklist 방식은 피한다. --no-write-json 같은 반대 옵션도 정책에 포함한다. 별도 경로인 contract.relation_vars_json도 현재는 self-seal만 거쳐 그대로 --vars에 추가되고 예약에서 권위 값과 대사하지 않는다. B2의 권위 vars 생성이 구현되기 전에는 비어 있지 않은 vars를 거부한다.

회귀: --select 뒤 여러 argv 토큰, 공백으로 합친 값, 별칭/중복/등호 형태, 모르는 옵션과 비권위 vars를 실행 전에 검증한다.

## 2. [P1] 취소 신호와 child 종료가 겹치면 성공 0을 반환함

근거: scripts/dbt_strict_runner.py:268–298.

forwarded 검사는 `while process.poll() is None` 본문에만 있다. SIGTERM 처리로 child가 종료되면 다음 poll이 종료 코드를 반환하여 본문을 건너뛴다. failure가 None이면 취소를 확인하지 않고 wait 결과를 반환한다. 신호 수신 시 0으로 종료하는 가짜 process로 **CANCELLED_SUPERVISOR_RETURN=0**을 재현했다. 실제 dbt가 이미 성공 artifact를 쓴 직후 취소되는 경합에서도 성공 증거가 등록될 수 있다.

수정: loop 종료와 wait 이후에도 cancellation latch를 검사하고 취소가 한 번이라도 관측됐으면 성공을 반환하지 않는다. 예외·신호 경로 전체에서 child 정리를 보장한다. signal handler 설치 전후의 작은 구간도 실패 정리 범위에 포함한다.

회귀: 신호 직후 exit 0, 이미 작성된 성공 artifact와 취소 경합, SIGINT/SIGTERM, 신호 무시 후 grace/kill. fake뿐 아니라 별도 로컬 child process로도 확인한다.

## 3. [P1] DB 미등록 파일 복구가 receipt의 자기 주장만 신뢰함

근거: scripts/dbt_strict_runner.py:362–372, _load_sealed_artifact; postgres_control_store.py:890 이후.

RESERVED/RUNNING/FILE_SEALED 복구는 receipt JSON을 dataclass로 읽고 DB 등록으로 넘긴다. run_results/log의 존재와 digest, native ID, exact 실행 결과를 다시 검증하지 않는다. receipt에는 contract_sha256 또는 검증할 로그 digest도 없다. store는 식별자·결과 목록·hash 형식만 검사하므로 이것이 실제 봉인된 실행 증거인지 확인하지 못한다.

독립 main 경로 검사에서 FILE_SEALED를 반환하는 가짜 store와 임의의 정상 형식 receipt만 준비했다. run_results와 dbt.log가 전혀 없는데도 **artifact 등록 호출 1회**로 정상 반환했다. 이는 실제 DB에 위조 row를 쓴 재현이 아니라 wrapper 복구 분기의 검증 누락 재현이다. DB에 이미 검증 완료된 증거가 있는 read-only replay와, DB에 없는 증거를 새로 승인하는 복구를 구분해야 한다.

수정: 신규 등록 복구에서는 contract/예약에 봉인된 실행 정보와 파일 경로를 대사하고 no-follow regular-file 방식으로 run_results/log를 읽어 native ID·exact IDs/status·digest를 재검증한다. receipt와 실제 bytes가 다르거나 없으면 성공 승격하지 않는다. 실행 종료/취소 결과 등 재검증으로 복원할 수 없는 정보의 봉인 경계도 명시한다. 완료 DB row의 재조회는 기존 immutable 증거를 권위로 유지한다.

경로 검사는 현재 사전 is_symlink/resolve 뒤 일반 read_bytes를 사용하는 구간이 있어 계획의 no-follow 읽기를 충족하지 않는다. 복구 경로와 함께 보완한다.

## 4. [P1] invocation registry의 test 권위가 승인 manifest까지 연결되지 않음

근거: postgres_control_store.py:556–642, 특히 564–586.

등록 함수는 model spec의 owned test 목록을 같은 caller 객체의 test_ownership과 대사한다. 양쪽에서 같은 test를 제거하면 내부 대사는 성공한다. release registry와 비교하는 graph_digest는 model/source 의존 관계를 나타내며 전체 test 목록·selector를 보장하지 않는다. model selector도 fqn 접두사만 검사한다. wrapper의 validate_deployment는 배포 파일을 검증하지만 그 manifest에서 registry를 재생성해 DB spec과 대사하지 않는다.

따라서 새 deployment 최초 등록에서 test_ownership과 각 owned-test 목록이 함께 누락되면 필요한 테스트가 EMPTY로 취급될 수 있다. 기존 등록과 다르면 거부하는 불변성 검사나 runtime INSERT 제한으로 최초 의미 검증을 대신할 수 없다. 이 항목은 코드 경로 검토 결과이며 잘못된 registry를 DB에 등록하지 않았다.

수정: deployment writer가 검증한 manifest와 승인 ownership override에서 registry를 재생성하고 model selector, 전체 test ID/selector/owner, owned set을 전수 대사한다. descriptor/manifest digest 연결을 저장하고 실행 전 DB spec을 해당 권위와 연결한다. graph의 model 집합 대사만으로 test 전체성을 선언하지 않는다. schema 추가가 필요하면 v5를 보존하고 v6에 반영한다.

회귀: 양쪽 목록에서 같은 test 삭제, owner 동시 변경, 다른 model의 fqn 대입, 전체 owned test 제거를 신규 deployment 등록 시 거부한다.

## 5. [P2] heartbeat callback이 멈춘 동안 lease deadline을 감시하지 못함

근거: scripts/dbt_strict_runner.py:274–285.

heartbeat를 supervisor poll thread에서 동기 호출한다. deadline은 callback 반환 이후에만 검사한다. deadline 0.05초, heartbeat가 0.35초 지연되는 로컬 반례에서 child 중단은 **약 0.362초 뒤**에 발생했다. DB statement/lock/connect timeout은 유용하지만 연결 이후 네트워크 응답 정지까지 supervisor의 wall-clock 종료 시한을 보장하지 않는다.

수정: heartbeat I/O와 독립적인 deadline 감시로 child를 정해진 시각에 중단한다. background heartbeat의 늦은 성공 응답이 실패 상태를 되돌리지 않게 하고 연결 정리도 보장한다. 시작 시 임의의 새 240초를 부여하기보다 실제 확인된 claim 잔여 시간과 안전 여유를 사용한다.

회귀: callback이 deadline보다 오래 반환하지 않는 동안 child가 먼저 종료되는지, 늦은 성공 응답이 와도 성공 artifact를 만들지 않는지 확인한다.

## 테스트 통과의 의미와 필요한 보완

- PostgreSQL unique reservation 경쟁과 권한 제한, MODEL 선행 조건, immutable artifact replay는 현재 suite에서 통과했다.
- 하지만 예약 경쟁 테스트의 execution_count는 소유권 반환 후 Python 정수를 증가시킨 것이다. 승인 계획의 두 wrapper/fake executable 실제 실행 횟수 검증은 아니다.
- serializer 테스트는 직접 구성한 성공 결과를 dbt serializer로 직렬화하고 log ID도 테스트가 직접 복사한다. 실제 dbt parse 테스트는 JSON log만 확인하며 run/test의 run_results를 생성하지 않는다. 실제 한 invocation의 log↔run_results end-to-end 대사는 아직 확인되지 않았다.
- RESERVED/RUNNING/FILE_SEALED/COMPLETED crash matrix, main의 응답 유실/파일 유실 복구, 지연 heartbeat와 취소 경합은 현재 테스트 파일에서 전수 검증되지 않는다. 위 반례를 포함해 실제 wrapper 진입점을 실행하는 검증을 추가해야 한다.
- 현재 deployment의 7-model/43-test 및 27/15/1 소유권 분류와 Cosmos full-command parser smoke는 유효한 부분 검증이다. publication/REUSE barrier 및 실제 Snowflake 업무 실행은 B1-3/B2 범위로 유지한다.

위 항목 보완과 계획된 실행 경계 검증 후 재검토한다. B1-1 및 B1-2 설계 승인을 취소하는 것이 아니라 B1-2 구현의 완료 판정을 보류한다.
