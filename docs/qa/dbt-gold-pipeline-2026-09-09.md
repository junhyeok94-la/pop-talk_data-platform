# Snowflake STAGING → dbt Gold 구현 기록

구현일: 2026-09-09

## 범위

Snowflake STAGING의 immutable 관측을 dbt로 타입 변환하고 현재 상태 DW와 분석
마트로 만든다. 이번 단계는 Snowflake Gold까지이며 PostgreSQL reverse ETL은 다음
단계다.

## 게시 선택 계약

- `SILVER_EXCHANGE_LOADS.STATUS = 'SUCCESS'`인 게시만 소비
- `SOURCE_RUN_ID + ARTIFACT_VERSION`별 가장 높은 publication revision 선택
- observation 자체의 최초 `PUBLICATION_REVISION`은 최신 게시 판단에 사용하지 않음
- 선택한 SUCCESS ledger를 run/artifact로 observation과 결합
- 현재값은 `SOURCE_OBSERVED_AT` 우선, 이후 revision/게시 완료 시각/identity로 결정
- 0건 SUCCESS 실행도 품질 마트에 남고 ledger/observation 차이를 0으로 검증

## 모델

- STAGING views
  - `stg_successful_exchange_publications`
  - `stg_movie_observations`
  - `stg_boxoffice_observations`
- DW tables
  - `dim_movie`: canonical movie별 최신 관측 1건
  - `fct_boxoffice_daily`: target date + KOFIC movie별 최신 정정 1건
- MART views
  - `mart_boxoffice_daily`: 영화 속성을 부가하되 fact grain을 누락 없이 보존
  - `mart_data_quality`: SUCCESS ledger와 관측 건수, 정책/매핑/결측 대사

박스오피스 관측의 영화가 dimension에 없거나 서비스 제외 대상이어도 fact/mart에서
조용히 삭제하지 않는다. `movie_dimension_matched`와 `policy_eligible`로 상태를
명시해 소비자가 정책을 선택할 수 있게 했다.

## Airflow 연결

`pop_talk_movie_databricks_daily`의 `load_snowflake` 뒤에
`build_snowflake_gold`를 추가했다. Airflow 이미지에 격리 설치된 dbt executable과
`pop_talk_snowflake`와 같은 keypair profile을 사용한다. 프로젝트 mount가 read-only라
dbt log/target은 `/tmp`에 기록한다.

## 실제 검증

- 실제 Snowflake `dbt build` 반복 성공
- 최초 실행: models 7, data tests 42; PASS 49 / WARN 0 / ERROR 0
- 두 번 이상 재빌드 후 건수 불변
- `DW.DIM_MOVIE`: 107
- `DW.FCT_BOXOFFICE_DAILY`: 70
- `MART.MART_BOXOFFICE_DAILY`: 70
- ledger 대사 실패: 0
- 현재 dimension 미매칭 boxoffice: 0
- Airflow DAG import error: 0

## 최초 검토 보완

- movie dimension과 boxoffice fact의 recency 정렬에서 publication revision을
  completion time보다 먼저 비교하도록 수정
- 두 모델이 같은 `observation_recency_order` 매크로를 사용하도록 통합
- 동일 source time에서 rev 1이 03:00 완료, rev 2가 02:00 완료된 역전 fixture를
  추가하고 rev 2가 선택되지 않으면 실패하도록 회귀 테스트 추가
- 보완 후 실제 Snowflake 최종 실행: models 7, data tests 43;
  PASS 50 / WARN 0 / ERROR 0, DAG import error 0

## 검토 대상

- `pipelines/dbt/models/`
- `pipelines/dbt/tests/`
- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`
