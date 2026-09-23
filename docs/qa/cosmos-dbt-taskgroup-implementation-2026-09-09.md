# Cosmos dbt 모델별 태스크 구현 검토 요청

## 구현 결과

기존 `build_snowflake_gold` 단일 `BashOperator`를 Cosmos 1.15.1
`DbtTaskGroup`으로 교체했다. 메인 DAG는 총 16개 태스크이며 Gold 구간은 다음처럼 보인다.

```text
identify_gold_build
  -> build_snowflake_gold.stg_successful_exchange_publications_run
  -> build_snowflake_gold.stg_movie_observations_run
  -> build_snowflake_gold.dim_movie_run
  -> build_snowflake_gold.stg_boxoffice_observations_run
  -> build_snowflake_gold.fct_boxoffice_daily_run
  -> build_snowflake_gold.mart_boxoffice_daily_run
  -> build_snowflake_gold.mart_data_quality_run
  -> build_snowflake_gold.project_test
  -> confirm_gold_build
  -> publish_postgres
```

실제 edge는 manifest의 `ref`/`source` 의존성을 따르므로 독립 가능한 모델은 직렬 edge가 아니라
병렬 graph로 표시된다. `pop_talk_dbt_pool`은 1 slot이라 실제 dbt subprocess는 한 번에 하나만
실행된다.

## 주요 계약

- Cosmos `1.15.1`, Airflow `3.3.1`
- `LoadMode.DBT_MANIFEST`, `ExecutionMode.LOCAL`, `InvocationMode.SUBPROCESS`
- dbt Core `1.10.11`, dbt-snowflake `1.10.3`은 `/opt/dbt-venv`에 격리
- model 7개를 개별 run 태스크로 렌더링
- schema/singular test 43개는 `TestBehavior.AFTER_ALL`의 단일 전체 gate로 실행
- Cosmos 태스크 timeout 30분, DAG run timeout 4시간
- 모든 Cosmos 태스크 `ALL_SUCCESS`, `pop_talk_dbt_pool` 1 slot
- PostgreSQL 게시 전 배포 source/manifest/digest 재검증

## 불변 배포와 LocalDagBundle 보강

`scripts/deploy_dbt_cosmos_artifact.py`가 SQL/YAML/profile, 정규화된 manifest와 descriptor를
`pipelines/dbt_deployments/<deployment_id>`에 원자 게시한다. 현재 배포 ID는
`bf08df4b8ce3a5412d7931e5ed391e32d9772b0bb2cc624d70cbf2d5005ba2a8`이며 모델 7개와 테스트
43개를 기록한다. 동일 입력으로 생성기를 두 번 실행했을 때 새 디렉터리가 생기지 않고 같은 ID가
재사용됐다.

Airflow 3의 LocalDagBundle은 versioning을 지원하지 않는다. 실제 `SerializedDAG` 검사에서도
Cosmos의 `project_dir` 자체는 UI용 operator에 보존되지 않았지만, `env` template field는
보존됐다. 이에 다음 안전장치를 추가했다.

1. `identify_gold_build`가 deployment ID, manifest SHA-256, model version을 단일 XCom에 고정한다.
2. 모든 Cosmos operator가 이 XCom을 직렬화되는 env template으로 받는다.
3. 이미지의 `/usr/local/bin/pop-talk-dbt` wrapper가 매 실행 직전에 XCom identity와 DAG
   graph identity를 대사한다. A/B가 섞이면 dbt를 시작하지 않는다.
4. wrapper가 Cosmos의 태스크별 임시 project를 비우고 고정된 불변 project A를 복사한다.
   `--project-dir`과 `--profiles-dir`은 이 쓰기 가능한 복사본을 사용하므로 dbt `target/`과
   `logs/`가 불변 원본에 기록되지 않는다.
5. `confirm_gold_build`와 `publish_postgres`도 current/live source가 아닌 같은 XCom 배포본을
   다시 검증한다.

경로 이탈 입력은 64자리 lowercase SHA-256 형식 검사로 차단한다. A를 identify한 뒤 current가
B로 이동해도 A가 남아 있으면 A를 사용한다. A source/manifest 변조나 A digest와 B 실행의 혼합은
실패한다.

## 변경 파일

- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`
- `orchestration/airflow/Dockerfile`
- `orchestration/airflow/requirements.txt`
- `compose.yaml`
- `pipelines/orchestration/dbt_deployment.py`
- `pipelines/orchestration/movie_daily_runtime.py`
- `pipelines/orchestration/tests/test_dbt_deployment.py`
- `scripts/deploy_dbt_cosmos_artifact.py`
- `scripts/dbt_cosmos_runner.py`
- `scripts/check_airflow_dag_contracts.py`
- `pipelines/dbt_deployments/current.json`과 현재 불변 배포본

## 검증 결과

- 전체 Python 회귀 테스트: 65개 통과
- Airflow DAG import 오류: 0
- DAG 구조 계약: 6개 DAG 통과
- 메인 DAG: 모델 7개 + 전체 test gate 1개 포함 총 16개 태스크
- manifest/dbt ls test unique ID: 43개 일치
- Airflow/dbt Python 환경 `pip check`: 모두 통과
- wrapper: Cosmos 임시 project를 불변 배포 source로 교체하고, 허용되지 않은 경로 옵션·선택자·
  A/B identity 혼합을 실행 전에 거부
- 설치된 Cosmos 1.15.1 모델 operator가 만든 실제 argv(`--no-partial-parse`, `run`, 단일
  `--select`, project/profile/target flags)를 그대로 wrapper에 통과시키는 구조 회귀 검사 성공
- SerializedDAG 왕복 후 `identify_gold_build.op_kwargs`와 각 dbt task의 graph/XCom env 보존 확인
- `pop_talk_dbt_pool`: init에서 1 slot으로 생성 성공
- 동일 source 배포 재실행: 동일 deployment ID 재사용

## 아직 수행하지 않은 검증

구현 검토 승인 전이라 실제 Snowflake model 7개와 test gate, PostgreSQL publication을 포함한 새
전체 DAG run은 아직 실행하지 않았다. 승인 후 새 publication revision으로 한 바퀴 실행하고,
각 Cosmos 로그의 임시 target/log 경로와 최종 test 결과를 추가 증빙한다.

## 중점 검토 요청

1. `AFTER_ALL`이 독립 singular/multi-parent test를 포함한 기존 43개 전체 gate 의미를 보존하는가
2. XCom → serialized env → wrapper 방식이 LocalDagBundle에서 A/B 혼합을 충분히 fail-closed 하는가
3. `model_version` publication identity와 `deployment_id` 배포 identity가 분리돼 유지되는가
4. pool, timeout, `ALL_SUCCESS`와 PostgreSQL 게시 gate가 부분 성공을 안전하게 처리하는가

## 참고 프로젝트 반영

사용자가 지정한 `D:\01.DEV\dbt-airflow`의 공통 Cosmos 설정과 대표 일간 DAG를 검토했다.
manifest parsing, dbt 격리 virtualenv, subprocess invocation, 모델별 `DbtTaskGroup`, 설정 함수
분리 방식은 채택했다. 반면 해당 프로젝트의 구형 Airflow import, 공유 target 경로와
`AFTER_EACH`는 현재 Airflow 3.3.1 및 전체 품질 gate 요구와 달라 적용하지 않았다.
