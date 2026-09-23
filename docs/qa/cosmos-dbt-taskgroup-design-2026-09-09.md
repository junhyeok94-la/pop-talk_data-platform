# Cosmos 기반 dbt 모델별 Airflow 태스크 설계

## 결론 제안

기존 `build_snowflake_gold` 단일 `BashOperator`를 Astronomer Cosmos의
`DbtTaskGroup`으로 교체한다. Cosmos는 Airflow 환경에 설치하되 dbt Core와
`dbt-snowflake`는 현재처럼 `/opt/dbt-venv`에 격리한다.

- Cosmos: `astronomer-cosmos==1.15.1` 고정
- 실행: `ExecutionMode.LOCAL`
- dbt executable: `/usr/local/bin/pop-talk-dbt` wrapper → `/opt/dbt-venv/bin/dbt`
- 인증: 기존 `pipelines/dbt/profiles.yml`과 Snowflake RSA key 경로 유지
- 표시: dbt model/test를 Airflow task로 렌더링
- 위치: 기존 전체 DAG 안의 `build_snowflake_gold` TaskGroup

Cosmos 공식 문서는 Local mode에서 별도 virtualenv의 dbt executable 경로를 지정하는 방식을
의존성 충돌 회피 방법으로 안내한다. 현재 이미지가 이미 같은 구조이므로 Cosmos 때문에 dbt를
Airflow Python 환경에 다시 설치하지 않는다.

## 전체 의존 관계

```text
load_snowflake
  -> identify_gold_build
  -> validate_dbt_graph
  -> build_snowflake_gold.<model별 run task>
  -> build_snowflake_gold.<전체 test gate>
  -> publish_postgres
```

`publish_postgres`는 TaskGroup의 모든 leaf가 성공하고, 기존처럼
`expected_model_version` XCom을 받은 경우에만 실행한다.

## Parsing 방식

`LoadMode.DBT_MANIFEST`와 content-addressed 불변 dbt 배포본의 manifest를 사용한다. Local mode에 일반적으로
권장되는 `DBT_LS`는 DAG parse마다 subprocess를 실행하므로 이 프로젝트의 parse-time 무외부
작업 원칙과 import 성능 baseline에 맞지 않는다.

전용 배포 명령은 dbt 소스, profile, 생성한 manifest와 아래 계약을 하나의 임시 디렉터리에
완성한 뒤 digest 이름의 최종 경로로 원자 rename한다.

```text
pipelines/dbt_deployments/<deployment_id>/
  project/                 # 실행할 SQL/YAML/macro/profile의 불변 복사본
  manifest.json            # 위 project에서 생성한 manifest
  deployment.json          # 아래 생성 계약
```

`deployment.json`에는 source model version, manifest SHA-256, dbt-core/dbt-snowflake version,
profile/target, materialization 설정과 test unique_id 집합을 기록한다. 이 값들의 canonical
digest를 `deployment_id`로 사용한다. manifest와 sidecar는 source model version 계산에서
제외해 자기참조 digest를 만들지 않는다. 같은 deployment ID 경로는 절대 덮어쓰지 않는다.

검증된 배포가 완성된 뒤에만 `current.json` pointer를 atomic replace한다. DAG parse 시 이
pointer를 한 번 읽고, 그때 선택한 deployment의 절대 project/manifest 경로와
`deployment_id`, `manifest_sha256`, `model_version`을 `identify_gold_build`의 단일 XCom으로
고정한다. 세 값은 Cosmos operator의 직렬화되는 `env` template field로 전달한다.

Airflow 3.3.1의 `LocalDagBundle`은 bundle versioning을 지원하지 않아 worker가 최신 DAG 파일을
다시 읽을 수 있다. 따라서 Cosmos가 parse한 `project_dir`에만 의존하지 않는다.
`pop-talk-dbt` wrapper는 매 model/test 실행 직전에 XCom의 세 값과 DAG에 직렬화된 graph
identity를 대사한다. 두 identity가 다르면 dbt 실행 전에 실패한다. 같으면 Cosmos가 만든
태스크별 임시 `--project-dir`을 비우고 해당 digest의 불변 project를 복사하며, profile도 그
복사본을 사용한다. dbt의 기본 `target/`과 `logs/`는 이 쓰기 가능한 임시 project 아래에
생성되므로 불변 원본에는 쓰지 않는다. current가 A에서 B로 바뀌어도 A graph를 가진 run은 A
source만 실행하며, 최신 DAG 파일이 B로 다시 해석되면 직렬화된 A XCom과 B graph identity의
불일치로 닫힌다.

`validate_dbt_graph`가 실행 직전에 다음을 다시 확인한다.

- 고정 인자의 deployment ID와 실제 불변 directory 이름
- 고정 manifest digest와 실제 manifest bytes
- source model version과 불변 project의 실제 SQL/YAML digest
- manifest의 project/profile/target/dbt version과 deployment 계약
- manifest의 model/test unique_id 집합과 계약에 기록된 집합

Cosmos와 wrapper도 같은 digest별 manifest와 project 경로를 읽는다. live `pipelines/dbt` 코드만 바뀌어도
진행 중 run에는 영향을 주지 않으며, 새 코드는 새 불변 배포를 생성하고 새 DAG version/run으로
전환해야 실행된다. 정상적인 current pointer의 A→B 교체는 기존 run이 불변 A를 계속 실행하므로
허용한다. 실패시켜야 하는 경우는 기존 A 경로의 manifest/descriptor/SQL 변조 또는 A 그래프가
B 실행 project를 사용하는 혼합이다.

## 테스트 표현

`TestBehavior.AFTER_ALL`을 명시한다. 각 model은 Airflow의 개별 run task로 표시하되, schema와
singular test는 모든 model 성공 뒤 하나의 전체 `dbt test` gate에서 실행한다. 이 방식은
여러 부모를 참조하는 test가 조기 실행되는 것을 막고, `ref`/`source`가 없는 독립 singular
test도 선택 누락 없이 포함한다. UI에서 model 실패와 전체 품질 gate 실패는 구분되지만
개별 test 43개가 각각 Airflow task가 되는 구조는 아니다. 개별 실패 test는 gate의 dbt 로그로
확인한다.

배포 시 manifest의 기존 test unique_id 집합과 `dbt ls --resource-type test` 결과를 대사한다.
현재 baseline은 43개이며 추가·삭제는 명시적으로 snapshot을 갱신한다. 통합 검증에서는 전체
test gate의 실제 실행 선택 집합/run result도 manifest 집합과 대사한다. 독립 test 실패,
다중 부모 중 하나 실패, 중간 model 실패, skipped/upstream_failed일 때 `publish_postgres`가
실행되지 않는 부정 시나리오를 검사한다.

Airflow 3의 Asset/OpenLineage 연동은 이번 범위에서 사용하지 않는다. 기존 PostgreSQL 게시
gate의 의미만 보존한다.

## 동시성, 작업 디렉터리와 timeout

- Cosmos task에는 `pop_talk_dbt_pool`을 지정하고 pool slot을 1개로 만든다.
- 한 번에 dbt subprocess 하나만 실행하며 기존 profile의 `threads: 2`를 유지한다. 따라서
  Snowflake query 동시성 상한은 기존 단일 `dbt build`보다 커지지 않는다.
- `InvocationMode.SUBPROCESS`를 명시해 Airflow Python 환경에 dbt library를 import하지 않는다.
- Cosmos Local operator가 각 태스크에서 만드는 `/tmp` project 복사본과 독립 target/log
  경로를 실제 command/log로 검증한다. 불변 원본 project는 read-only로 사용한다.
- 각 Cosmos model/test 태스크 timeout은 30분, 전체 DAG의 `dagrun_timeout`은 4시간으로 둔다.
  기존 일반 태스크의 2시간 timeout은 유지한다. TaskGroup 자체 timeout이 아니라 개별 작업과
  전체 run 제한이 각각 적용된다는 의미다.
- 모델 일부 성공 뒤 실패하면 성공 모델을 덮어쓸 수는 있지만 PostgreSQL active snapshot은
  전체 test gate 전까지 바뀌지 않는다. 실패 태스크 재시도와 downstream 재개 시 이 상태를
  검증한다.

## 동작 변경과 보존

의도적으로 변경되는 것:

- `build_snowflake_gold` 한 태스크가 TaskGroup 내부의 model/test 태스크들로 확장
- 모델별 상태, 로그, 재시도와 실행 시간 확인 가능

보존되는 것:

- DAG ID와 dbt 이전/이후 태스크 ID
- Snowflake profile, role, warehouse, database/schema
- dbt SQL/YAML과 materialization
- 전체 Gold 성공 후에만 PostgreSQL 게시
- Gold model/deployment version 고정 및 게시 직전 재검증
- `max_active_runs=1`, 기존 일반 태스크 retry/timeout

## 검증 계획

1. 이미지 dependency resolution 및 Cosmos/dbt 버전 확인
2. 불변 deployment/manifest 계약과 atomic pointer 생성 검사
3. Airflow 3.3.1 import 오류 0
4. 렌더링된 model/test task ID와 dependency graph snapshot 검사
5. parse A→deploy B, manifest/descriptor/SQL 단독 변경, 직렬화된 env와 wrapper 경로 치환 테스트
6. 기존 단위 테스트와 DAG 구조 테스트
7. model/test unique_id 집합 대사와 실패 전파 구조 검사
8. 검토 승인 후 새 DAG run/새 publication revision으로 최소 표본 통합 실행

## 로컬 참고 소스 상태

작업공간 밖의 `D:\01.DEV\dbt-airflow`를 직접 검토했다. 특히
`airflow/hbu/include/dbt_common_config.py`와
`airflow/hbu/dw/bi/daily/bi_daily_cmm_pb_03.py`에서 다음 방식을 참고했다.

- `DbtTaskGroup`과 manifest parsing으로 모델별 Airflow 태스크를 만드는 구조
- Airflow와 분리된 dbt virtualenv, `InvocationMode.SUBPROCESS`
- project/profile/execution/render/operator 설정을 함수로 분리하는 방식
- `emit_datasets=False`, 명시적 test behavior와 select/exclude 계약

현재 프로젝트에는 그대로 복사하지 않은 부분도 있다. 참고 소스는 구형 Airflow import와
공유 target 경로, tag 중심 선택, `AFTER_EACH`를 사용한다. 여기서는 Airflow 3.3.1 API,
content-addressed manifest, 태스크별 임시 쓰기 경로, 모델 전체 후 43개 테스트를 검사하는
`AFTER_ALL`을 적용한다.
