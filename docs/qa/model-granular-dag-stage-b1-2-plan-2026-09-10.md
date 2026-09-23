# 모델 단위 DAG 단계 B1-2 계획 — exact dbt invocation과 실행 증거

## 1. 범위

B1-1이 보존하는 build attempt에 dbt model/test 실행 증거를 연결한다. 아직 Airflow Asset
factory와 업무 7-model generation SQL은 만들지 않는다.

현재 설치본은 dbt-core 1.10.11, dbt-snowflake 1.10.3, Cosmos 1.15.1이다. dbt 1.10 CLI에서
`--target-path`, `--log-path`, `--write-json`, `--indirect-selection empty`를 직접 확인했다.

## 2. 실행 단위

한 REBUILD attempt에는 다음 invocation이 최대 두 개 존재한다.

1. `MODEL`: exact model `fqn:` selector 하나로 `dbt run`
2. `OWNED_TESTS`: registry가 소유시킨 exact test `fqn:` selector 집합으로 `dbt test`

owned test가 0개면 subprocess를 실행하지 않고 `EMPTY_TEST_SET` 증거를 명시적으로 만든다.
MODEL과 OWNED_TESTS는 같은 `target/run_results.json`을 덮어쓰지 않는다.

## 3. strict invocation contract

새 model DAG는 JSON contract를 wrapper 환경변수로 전달한다.

```text
contract_version
orchestration_invocation_id # 예약·멱등성·경로를 정하는 content ID
invocation_kind             MODEL | OWNED_TESTS
deployment_id / manifest_sha256 / model_version
plan_id / cohort_manifest_id / build_id
attempt_no / fence_token / claim_owner
model_unique_id
expected_unique_ids
expected_selectors
artifact_root
```

- contract는 canonical JSON SHA-256으로 self-seal한다.
- deployment/model/test expected 집합은 B1-1의 승인 `ModelDagRegistry`와 exact 대사한다.
- model invocation은 expected unique ID와 selector가 정확히 하나다.
- test invocation은 해당 model의 owned test ID/selector 집합과 정확히 같다.
- `+`, tag, path, wildcard, indirect graph selector, `--exclude`, named selector를 거부한다.
- wrapper argv와 contract selector를 다시 대사하며 caller가 target/log/state/defer를 지정하지
  못하게 한다.

`dbt_native_invocation_id`는 사전에 지정하지 않는다. dbt CLI가 매 호출 생성한 UUID를 JSON log
event와 fresh `run_results.metadata.invocation_id`에서 관측해 서로 대사하고 receipt에 별도
필드로 기록한다. run-results v6 result에는 resource type이 없으므로 unique ID의 resource type은
고정 deployment manifest에서 조회한다.

기존 전체 DAG의 `project_test`와 model run은 명시적인 `/usr/local/bin/pop-talk-dbt-legacy`
entrypoint만 사용한다. 신규 factory는 별도 `/usr/local/bin/pop-talk-dbt-strict`만 사용하며 contract
누락·빈 문자열·파싱 실패 시 dbt 실행 전에 실패한다. 환경변수 존재 여부로 strict에서 legacy로
fallback하지 않는다. cutover 후 legacy entrypoint 제거는 별도 단계다.

strict subprocess 환경은 현재 process 전체를 그대로 상속하지 않는다. `PATH`, locale, 인증에
필요한 `SNOWFLAKE_*` 중 명시 목록과 wrapper가 생성한 계약 변수만 보존한다. `DBT_*`는 아래
고정값 외에는 전부 제거하고 다음 실행 의미 변수를 계약값으로 고정한다.

```text
DBT_DEFER=false
DBT_STATE / DBT_DEFER_STATE 제거
DBT_INDIRECT_SELECTION=empty
DBT_WRITE_JSON=true
DBT_PARTIAL_PARSE=false
DBT_TARGET_PATH=<invocation target>
DBT_LOG_PATH=<invocation logs>
DBT_LOG_FORMAT_FILE=json
DBT_LOG_LEVEL_FILE=debug
```

`--state`, `--defer-state`, `--defer`, `--partial-parse`, target/log/write-json/indirect-selection의
caller override, `--log-format-file`/`--log-level-file` 및 동등한 환경 override와 `--vars` 임의
입력을 거부한다. stdout/stderr 형식은 Airflow 사람이 읽는 task log 용도이고 native ID 증거로
파싱하지 않는다. B2의 generation relation vars는 권위
cohort/claim에서 생성한 exact JSON 하나만 허용하고 env digest를 receipt에 별도로 봉인한다.

## 4. attempt별 artifact 봉인

모든 Airflow 서비스는 compose 공통 anchor를 통해 호스트의
`.local/dbt-attempt-artifacts`를 `/opt/airflow/dbt-attempt-artifacts`에 bind mount한다. scheduler,
API server, worker가 서로 다른 임시 filesystem을 보지 않도록 하며, 개발 환경 재시작 뒤에도
증거 파일을 보존한다.

실행 순서는 반드시 다음과 같다.

1. PostgreSQL에 `(build_id, attempt_no, fence_token, invocation_kind)`와
   `orchestration_invocation_id`를 **실행 예약**한다.
2. 예약을 소유한 호출만 invocation 디렉터리를 exclusive create하고 subprocess를 시작한다.
3. subprocess 종료 후 artifact를 검증하고 receipt 파일을 atomic rename으로 봉인한다.
4. 봉인 파일과 동일한 내용을 PostgreSQL immutable artifact로 등록한다.

wrapper는 다음 root 아래 invocation별 새 디렉터리를 만든다.

```text
/opt/airflow/dbt-attempt-artifacts/<build-id>/<attempt>/<fence>/<kind>/<orchestration-invocation-id>/
  target/run_results.json
  logs/dbt.log
  invocation_receipt.json
```

- root와 모든 부모/파일은 신뢰한 소유자·권한인지 검사한다. `resolve()` 결과가 root 내부인지
  확인하고, 각 경로 구성요소와 파일은 no-follow 방식으로 열어 symlink·junction·path traversal을
  거부한다.
- 이미 존재하는 invocation 디렉터리는 새 subprocess로 덮어쓰지 않는다. PostgreSQL completed
  row가 있으면 그것이 권위이며 파일이 없어도 SQL을 재실행하지 않고 `ARTIFACT_MISSING` 복구
  대상으로 표시한다.
- receipt 파일 봉인 후 DB 등록 전에 죽은 경우, 같은 logical ID 재호출은 sealed file의 digest와
  contract를 검증해 DB에 등록하며 subprocess를 다시 실행하지 않는다. 파일이 모호하거나
  불완전하면 같은 attempt에서 새 ID로 재실행하지 않고 attempt를 폐기한다.
- DB commit 응답을 잃은 경우 같은 ID를 재등록하기 전에 reservation/artifact 상태를 조회한다.
  동일 body 완료면 성공 replay이고 다른 body면 conflict다.
- wrapper는 caller의 target/log option을 거부하고 `DBT_TARGET_PATH`, `DBT_LOG_PATH`를 해당
  invocation 경로로 강제한다.
- 실행 전 target/log가 비었고 실행 뒤 이번 invocation의 새 `run_results.json`이 생겼는지
  확인한다.
- receipt JSON은 같은 디렉터리에 exclusive 임시 파일을 쓴 뒤 fsync하고 같은 filesystem rename으로
  봉인하며 이후 수정하지 않는다.

## 5. dbt native 실행과 run_results 검증

supervisor가 관측한 `Popen` 종료 코드가 0이어야 하고 `run_results.json`은 다음을 모두 만족해야
한다.

- exclusive invocation 경로의 `logs/dbt.log` JSON event에서 관측한
  `dbt_native_invocation_id`와 fresh
  `run_results.metadata.invocation_id`가 서로 같고 유효한 UUID다. 이 값은 사전 계산한
  `orchestration_invocation_id`와 같을 필요가 없으며 두 필드를 receipt에 별도로 기록한다.
- JSON log에서 native ID가 누락되거나 둘 이상 관측되거나 run_results와 불일치하면 실패다.
- 실제 result unique ID 집합이 expected 집합과 정확히 같다. 중복도 거부한다.
- run-results v6 result 자체의 resource type을 신뢰하지 않는다. 각 unique ID를 봉인된 deployment
  manifest에 조회해 MODEL은 model 하나/status `success`, OWNED_TESTS는 test만/status `pass`인지
  확인한다.
- fail, error, warn, skipped, 누락, extra, 0-result 오인 성공을 거부한다.
- artifact SHA-256과 생성 시각/크기, argv SHA-256, sanitized env SHA-256,
  orchestration/native invocation ID, deployment/cohort/fence를 receipt에 기록한다.

`EMPTY_TEST_SET`은 dbt subprocess, native invocation ID, run_results 없이 registry의 owned test 집합이
실제로 비었다는 사실만 별도 증거로 봉인한다.

dbt가 nonzero로 끝났지만 artifact를 남긴 경우 실패 증거는 보존하되 성공 receipt로 승격하지
않는다.

## 6. PostgreSQL invocation ledger

기존 적용 migration은 수정하지 않고 새 version migration으로 mutable 예약 표와 immutable 증거
표를 분리해 추가한다.

```text
dbt_invocation_reservations
  orchestration_invocation_id PK
  build_id / attempt_no / fence_token
  invocation_kind             MODEL | OWNED_TESTS | EMPTY_TEST_SET
  contract_sha256 / claim_owner
  state                       RESERVED | RUNNING | FILE_SEALED | COMPLETED | FAILED | ABANDONED
  dbt_native_invocation_id nullable
  heartbeat_at / started_at / completed_at
```

- `(build_id, attempt_no, fence_token, invocation_kind)`는 unique다.
- 같은 logical ID와 같은 contract의 재요청만 상태 조회·복구를 허용한다. 다른 ID 또는 다른
  contract는 subprocess 시작 전에 거부한다.
- MODEL의 종료 상태가 성공 증거로 등록되기 전에는 OWNED_TESTS/EMPTY_TEST_SET을 예약할 수 없다.
- MODEL 실행 결과가 불확실하거나 MODEL과 TEST 사이 queue delay로 lease가 만료되면 같은
  attempt/fence를 재사용하지 않고 B1-1에서 새 attempt/fence를 claim한다.
- 예약 표의 state/heartbeat UPDATE는 claim owner와 current fence를 조건으로 한 CAS만 허용한다.

immutable 증거 표는 다음 필드를 가진다.

```text
dbt_invocation_artifacts
  orchestration_invocation_id PK/FK -> dbt_invocation_reservations
  dbt_native_invocation_id nullable
  build_id / attempt_no / fence_token FK -> model_build_attempts
  cohort_manifest_id / deployment_id / model_unique_id
  invocation_kind
  expected_unique_ids JSONB
  executed_unique_ids JSONB
  run_results_sha256 / argv_sha256
  artifact_path
  status
  body JSONB
```

artifact 표에 대해 runtime은 SELECT/INSERT만 가능하고 UPDATE/DELETE할 수 없다. 예약/등록
transaction은 current
미만료 build claim, exact cohort/deployment/model/fence와 registry expected 집합을 다시 검사한다.
같은 orchestration invocation ID/body replay만 no-op이다. model/test invocation은 각각 고유 ID를
가지며 같은 attempt/kind에는 성공 증거가 하나만 존재한다. DB column identity와 JSON body identity를
각각 읽어 서로 대사한다.

B1-1의 최종 `model_execution_receipts`는 B1-3 relation query 검증 시 MODEL과 OWNED_TESTS 또는
EMPTY_TEST_SET artifact를 조합해 만든다. B1-2 artifact만으로 result READY를 commit하지 않는다.

## 7. heartbeat

strict wrapper는 `subprocess.Popen` supervisor로 dbt를 실행한다.

- child는 wrapper와 같은 process group에 두어 Airflow가 보낸 SIGINT/SIGTERM을 전달한다.
- stdout/stderr는 pipe에 가두지 않고 Airflow task log로 상속해 대용량 출력 deadlock을 막는다.
- poll loop와 별도 PostgreSQL connection으로 B1-1 heartbeat를 보낸다. 이 connection에는 짧은
  connect timeout, statement timeout, lock timeout을 고정한다.
- 마지막 성공 heartbeat부터 lease 안전 마감시각을 계산한다. 다음 heartbeat 실패나 deadline
  도달 시 child에 종료 signal을 전달하고 grace period 후 kill/wait한다.
- Airflow 취소 signal도 child에 전달하고 grace period 후 kill/wait한다.
- heartbeat 또는 취소 처리가 실패·불확실하면 subprocess가 exit 0이어도 성공 artifact를 등록하지
  않는다. Snowflake query 취소가 불확실해도 attempt별 relation이 다음 attempt와 격리된다.
- MODEL 완료 뒤 OWNED_TESTS가 queue에서 지연되어 lease가 만료된 경우 TEST를 시작하지 않고
  attempt를 abandon한다.

## 8. 검증

fake dbt executable이 격리 artifact를 쓰도록 하여 다음을 자동 검사한다.

- exact model 성공, exact owned tests 성공, 실제 zero-owned EMPTY
- 과거 run_results 재사용, invocation ID 불일치, missing/extra/duplicate ID
- JSON file log native ID 누락/복수/불일치, 기본 debug 형식 및 caller log-format override 차단
- fail/error/warn/skipped와 nonzero subprocess
- graph selector/target-path/log-path/state/defer 우회
- model/test artifact 상호 덮어쓰기 방지
- 같은 invocation response-loss replay와 같은 ID/다른 body conflict
- 실행 전 예약 경쟁: 실제 PostgreSQL connection 두 개와 wrapper 두 개가 동시에 요청해 fake
  executable 실행 횟수가 정확히 1인지 확인
- sealed file 뒤 DB 등록 전 crash 복구, DB commit response-loss, DB 완료 뒤 artifact file 유실
- 만료/stale fence와 heartbeat 실패
- heartbeat DB timeout, signal 전달, grace 후 kill/wait, lease 만료 뒤 TEST 시작 금지
- symlink/junction/path traversal/no-follow 위반
- registry test selector 누락/추가/다른 model 소유 test
- legacy 기존 wrapper 테스트 전부 유지
- 무변경 g1→g2→g3 REUSE provenance, 변경 branch와 과거 REUSE branch 결합, 잘못된 과거
  result/source/deployment 증거 거부 계약

current 불변 deployment의 manifest에는 전체 test 43개가 있다. 그중 27개 `MODEL_GATE`만 각 model의
strict OWNED_TESTS 계약에 소유시키고, 15개 `SOURCE_GATE`와 1개 `DEPLOYMENT_GATE`는 후속 별도 gate로
명시한다. 7 model/27 owned test가 registry와 exact하게 생성되고, 43개 전체가 세 분류 중 정확히
하나에 속하는 snapshot test를 추가한다. publication/cutover가 source/deployment gate를 생략할 수
없다는 계약 테스트도 후속 gate가 구현되기 전부터 유지한다.

B1-3에서 model DAG와 함께 `SOURCE_GATE`/`DEPLOYMENT_GATE` 전용 DAG·receipt를 구현한다.
publication assembler의 기준은 모든 test를 현재 generation에서 새로 실행했는지가 아니라,
**소비 generation plan이 선택한 exact binding의 provenance가 완전한지**다.

- REBUILD slot은 현재 attempt/fence의 result와 MODEL/OWNED_TESTS 증거를 요구한다.
- REUSE slot은 `plan.reuse_result_manifest_id`가 가리키는 원본 result의 검증된 model/test receipt를
  provenance로 참조한다. 소비 generation과 원본 실행 generation을 모두 보존하며 과거 receipt를
  현재 generation 실행으로 재표기하지 않는다.
- SOURCE_GATE는 소비 plan의 exact source snapshot/cutoff에 묶는다. 동일 binding의 기존 증거를
  재사용할 수 있지만 현재 generation 확인 row와 원본 실행 증거를 구분한다.
- DEPLOYMENT_GATE는 exact deployment ID/manifest SHA에 묶고 동일 binding 증거만 재사용한다.
- assembler는 27+15+1이라는 고정 수가 아니라 해당 deployment registry의 MODEL/SOURCE/DEPLOYMENT
  exact required test ID 집합을 검사한다.

fake executable 테스트 외에 설치된 dbt-core 1.10.11로 최소 로컬 fixture를 실제 실행하여
run-results v6 형식, JSON log의 native invocation ID, manifest 기반 resource type 대사가 구현과
호환되는지 확인한다. 실제 Snowflake 업무 model 실행은 B2다.

Cosmos 1.15.1 통합은 다음을 명시한다.

- 신규 model DAG는 `ExecutionMode.LOCAL`/subprocess 경로에서 strict executable만 호출한다.
- dependency install, partial parsing, Cosmos 자동 test 생성, 자동 Asset emitting/post-processing은
  끄거나 고정해 registry 외 작업이 생기지 않게 한다.
- Cosmos가 만드는 임시 project/target의 `run_results.json`을 권위 증거로 사용하지 않는다.
  task success callback 전에 strict wrapper가 영구 mount에 봉인하고 DB ledger에 등록한 receipt만
  사용한다.
- Cosmos task kill은 supervisor의 signal forwarding/cancellation 검증을 통과해야 한다.
- 설치본 Cosmos가 실제로 조립한 command와 sanitized env가 strict wrapper 계약을 통과하는
  synthetic smoke test를 둔다.

## 9. 완료 기준

- 신규 strict 경로가 model 하나와 owned test만 실행하고 fresh artifact로 증명한다.
- 동일 invocation 경쟁에서도 dbt executable은 한 번만 실행된다.
- invocation evidence가 B1-1 current fenced attempt에 immutable하게 저장된다.
- 기존 전체 DAG의 wrapper 계약과 133개 회귀 테스트가 유지된다.
- fake subprocess 실패 경계, 실제 dbt v6 fixture, current 7-model/27 model-owned test 및
  43-test 전체 gate 분류 대사가 통과한다.
- 실제 dbt fixture의 JSON file log native ID와 fresh run-results v6 native ID가 일치한다.
- REBUILD/REUSE/source/deployment exact binding provenance 계약 테스트가 통과한다.
- source/deployment gate가 아직 미구현임을 명시하고 B1-3 publication/cutover 선행조건에서
  제거하지 않는다.
- 검토 세션 승인을 받은 뒤 B1-3 partitioned Asset/model DAG factory로 이동한다.

## 10. 검토 질문

1. wrapper가 subprocess supervisor와 heartbeat를 함께 맡는 경계가 적절한가?
2. dbt CLI path는 argv가 아니라 allowlisted `DBT_TARGET_PATH`/`DBT_LOG_PATH`로 강제하는 것이
   dbt 1.10/Cosmos에서 안전한가?
3. invocation artifact 표와 최종 execution receipt를 분리하는 것이 task 경계를 명확히 하는가?
4. legacy와 strict 경로를 병행하는 cutover 방식에 selector 우회가 남는가?
5. 실행 예약과 immutable artifact를 분리한 crash/response-loss 상태 전이가 충분한가?
6. MODEL_GATE 27개와 나머지 SOURCE/DEPLOYMENT_GATE 16개를 분리하되 publication에서 모두
   요구하는 경계가 적절한가?
