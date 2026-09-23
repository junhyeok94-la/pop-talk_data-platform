# 모델 단위 DAG 단계 B1-2 구현 기록 — exact dbt invocation과 실행 증거

## 결과

승인된 B1-2 계획에 따라 model/owned-test 한정 dbt 실행을 SQL 시작 전에 예약하고, 실행 뒤
JSON file log와 fresh run-results를 교차 검증해 PostgreSQL에 불변 증거로 남기는 기반을
구현했다. 기존 전체 DAG는 legacy entrypoint를 계속 사용하며, model DAG factory와 실제
Snowflake generation relation 실행은 B1-3/B2 범위다.

## 구현 파일

- `pipelines/orchestration/sql/005_dbt_invocation_ledger.sql`
  - deployment/model별 exact invocation spec registry
  - mutable reservation과 immutable artifact 분리
  - `(build, attempt, fence, kind)` 실행 전 unique 예약
  - runtime role의 registry 쓰기·artifact 수정 금지와 reservation 상태 열만 UPDATE 허용
- `pipelines/orchestration/dbt_invocation_contract.py`
  - content-addressed orchestration invocation contract
  - dbt native invocation UUID를 JSON log와 run-results에서 독립 관측·대사
  - run-results v6 result의 resource type을 deployment manifest에서 판정
  - exact ID 집합, 중복, model success/test pass만 허용
- `pipelines/orchestration/postgres_control_store.py`
  - deployment writer의 invocation registry 전수 등록
  - current plan/cohort/claim/fence/spec exact 대사 후 실행 예약
  - MODEL 성공 전 OWNED_TESTS/EMPTY 예약 거부
  - reservation CAS 상태 전이, heartbeat, immutable artifact 등록/read-back
  - DB 완료 뒤 파일 유실 표시와 SQL identity/JSON body 대사
- `scripts/dbt_strict_runner.py`
  - contract 없는 호출은 dbt 시작 전에 거부하는 strict entrypoint
  - Cosmos argv와 exact fqn selector 대사, graph/wildcard/경로/defer/state/vars override 거부
  - 현재 deployment source를 attempt 전용 project로 복사
  - 최소 OS/인증 환경만 상속하고 dbt 실행 의미를 고정
  - `DBT_LOG_FORMAT_FILE=json`, `DBT_LOG_LEVEL_FILE=debug`, write-json=true,
    indirect-selection=empty, defer=false, partial-parse=false 강제
  - Popen poll supervisor, 별도 DB heartbeat, deadline, signal 전달, grace 후 kill/wait
  - invocation별 target/log/receipt 분리와 exclusive temp + fsync + atomic rename
  - sealed-file/DB-commit 응답 유실 replay, DB 완료/file 유실 시 SQL 재실행 금지
- `orchestration/airflow/Dockerfile`
  - `/usr/local/bin/pop-talk-dbt-strict`와 `pop-talk-dbt-legacy` 분리
  - 기존 `/usr/local/bin/pop-talk-dbt`는 legacy symlink로 유지
- `compose.yaml`, `scripts/prepare-airflow.ps1`
  - 모든 Airflow 서비스가 `.local/dbt-attempt-artifacts` 영속 경로를 공유
- `pipelines/orchestration/tests/test_dbt_strict_invocation.py`
  - contract 변조, env 오염, selector/option 우회, native ID/result exact 대사
  - heartbeat 실패 시 child 종료와 receipt overwrite 거부
  - 설치 dbt-core 1.10.11 serializer의 run-results v6 및 resource_type 부재 확인
  - 실제 dbt parse의 JSON file log native UUID 확인
  - 설치 Cosmos 1.15.1이 subprocess 직전 조립한 full command를 strict parser로 검증
- `pipelines/orchestration/tests/test_postgres_control_store.py`
  - 실제 PostgreSQL에서 MODEL 선행 조건, artifact 불변 replay, 두 connection 예약 경쟁
  - v5 runtime 최소권한 확인
- `pipelines/orchestration/tests/test_dbt_model_registry.py`
  - current deployment의 7 model, 43 test와 MODEL/SOURCE/DEPLOYMENT 27/15/1 분류 대사

## 장애·멱등성 경계

1. 같은 attempt/kind의 첫 reservation만 실행 소유권을 얻는다. 다른 logical ID는 SQL 전에
   unique conflict로 거부된다.
2. 동일 logical ID 재호출은 DB 상태를 먼저 읽는다. COMPLETED면 DB artifact와 파일을 exact
   대사하고 SQL을 다시 실행하지 않는다.
3. receipt 봉인 뒤 DB 등록 전에 죽으면 RESERVED/RUNNING/FILE_SEALED 상태에서 같은 ID만
   파일을 회수해 등록할 수 있다. current lease/fence가 만료됐으면 새 성공 등록을 거부한다.
4. DB 완료 row가 있지만 파일이 없으면 `ARTIFACT_MISSING`으로 표시하고 SQL을 재실행하지 않는다.
5. heartbeat 예외·지연 또는 마지막 성공 deadline 초과 시 child를 종료하고 성공 artifact를
   만들지 않는다. stdout/stderr는 Airflow log로 상속해 pipe deadlock을 만들지 않는다.
6. EMPTY_TEST_SET은 subprocess/native UUID/run-results 없이 registry의 실제 빈 owned-test 집합으로만
   예약할 수 있다.
7. 인증 환경 값은 child에 필요한 최소 목록만 전달하고 receipt digest 입력에서는 값 대신
   존재 여부만 봉인한다.

## 검증 결과

적용 migration digest:

```text
v1=30a7d3f57148bd7e9c649958491f64510d14463e87c20203af10190260c9a956
v2=1cfd5503fbfcee71f9c8d3b0672bad4735853a11639a234ef1e73c455acc2a0c
v3=694af2a30764e719b493437ad4f084b2c9131f483b04c563df8c1d76542330bb
v4=13663dbf74e56efbcdf0c0006c7409daf460231df280e7ea70368098ac143e2d
v5=9f83113b7d7fc366ff2aa1ee3f19a4ee37ff3ce0513ae6330e4a1db4c4c691fb
```

최종 Airflow 3.3.1 이미지의 pipelines 전체 suite:

```text
Ran 144 tests in 14.262s
OK
```

이 실행에는 실제 PostgreSQL 통합 테스트, 설치 dbt 1.10.11 serializer/JSON log fixture,
설치 Cosmos 1.15.1 full-command smoke가 포함된다. 실제 Snowflake 업무 SQL은 실행하지 않았다.

## 다음 범위

B1-3에서 partitioned Asset publisher/consumer, model별 DAG factory, SOURCE_GATE와
DEPLOYMENT_GATE receipt, REBUILD/REUSE provenance 기반 publication barrier를 구현한다. 이후 B2에서
attempt 전용 Snowflake relation vars와 실제 7-model 업무 실행을 연결한다.

## 1차 검토 보완

최초 구현 검토의 `CHANGES_REQUESTED` 다섯 항목을 다음처럼 보완했다.

- caller argv를 전달하지 않고 allowlist parser가 모든 토큰을 소비한 뒤 검증된 값만으로
  `run/test + exact fqn + 고정 경로/profile/target` 명령을 새로 만든다. 추가 bare selector,
  alias, 중복·미지 option과 B2 전의 non-empty relation vars는 SQL 전에 거부한다.
- SIGINT/SIGTERM latch를 child 생성 전에 설치하고 loop 종료와 wait 이후에도 확인한다.
  heartbeat I/O는 daemon thread로 격리하여 호출이 멈춰도 supervisor가 DB에서 읽은 실제
  claim 잔여 시간과 safety margin에 맞춰 child를 중단한다. 이미 시작된 heartbeat 결과가
  확정되기 전에는 성공으로 진행하지 않는다.
- DB 미등록 `FILE_SEALED` 복구는 receipt만 신뢰하지 않는다. no-follow regular-file descriptor로
  `run_results.json`과 JSON log를 다시 읽고 native UUID, exact ID/status, digest를 전수 대사한다.
- invocation registry는 caller가 만든 registry를 받지 않고 `validate_deployment`로 descriptor,
  source, manifest digest를 다시 검증한 배포본과 승인 ownership override에서 직접 재생성한다.
  v6가 registry/reservation에 `manifest_sha256 + model_version`을 저장하고 FK로 연결한다.
- 추가 회귀 테스트는 extra selector, non-authority vars, 취소/exit-0 경합, 정지 heartbeat deadline,
  원본 dbt 증거 변조 및 current immutable deployment 기반 registry 재생성을 포함한다.

v6는 적용 후 다음 digest로 고정했다. 이후 수정은 새 migration으로만 추가한다.

```text
v6=41476deeeacaa74540ac8971fa4601f34eca5eaf9f0285c3187b5eacba34b916
```

보완 후 Airflow 3.3.1 일회성 컨테이너에서 실제 PostgreSQL 통합 테스트를 활성화한 전체 suite:

```text
Ran 148 tests in 19.177s
OK
```

## 2차 검토 보완 및 사이드 이펙트 점검

1차 보완 재검토에서 발견된 두 P2를 수정했다.

- claim 만료 조회가 암묵적 외부 transaction을 남기지 않도록 store가 독립 transaction을
  열고 즉시 닫는다. 이에 따라 실제 `main → child exit 1 → FAILED CAS`가 예외 전파 뒤에도
  rollback되지 않는다. 별도 PostgreSQL connection에서 `FAILED / RuntimeError`를 확인하는
  통합 테스트를 추가했다.
- 파일 자체에만 `O_NOFOLLOW`를 적용하지 않고 filesystem root dirfd부터 모든 부모를
  `O_DIRECTORY | O_NOFOLLOW`로 순차 개방한다. `receipt`, `target/run_results.json`,
  `logs/dbt.log` 모두 같은 reader를 사용하며, 부모 `logs`를 과거 증거 디렉터리에 연결한
  반례를 거부한다.

사이드 이펙트도 함께 확인했다.

- migration/DDL 변경 없음: 적용·고정된 v1–v6는 그대로다.
- transaction 소유권: 조회 helper는 자신의 top-level transaction만 닫고 caller connection을
  임의 commit하지 않는다. 기존 reservation/state/artifact 쓰기 API의 CAS 경계는 유지된다.
- 정상 실행: Cosmos full command, dbt 1.10 serializer/log, invocation artifact와 기존 파이프라인
  테스트가 그대로 통과한다.
- 실패 실행: child가 0이 아닌 코드로 끝나면 성공 receipt를 만들지 않고 실패 상태만 영속한다.
- 복구 보안: 최종 파일과 부모 디렉터리 어느 쪽의 symlink도 과거 실행 증거로 우회할 수 없다.
- 플랫폼 계약: strict runner가 배포되는 Linux/Airflow 런타임은 dirfd와 `O_NOFOLLOW` 지원을
  필수로 하며, 지원하지 않는 환경에서는 안전하지 않은 fallback 대신 fail-closed한다.

최종 전체 suite:

```text
Ran 150 tests in 18.472s
OK
```
