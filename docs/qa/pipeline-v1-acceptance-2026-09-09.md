# Movie DW 파이프라인 v1 통합 인수 기록

실행일: 2026-09-09

## 대상 흐름

`S3 DAILY_READY → Databricks Bronze/Silver → S3 Exchange revision 6 → Snowflake
STAGING → dbt DW/MART → PostgreSQL dw_serving active snapshot`

원천 API 수집 DAG는 별도 선행 DAG이며, 이 인수 실행은 그 결과인 immutable
`DAILY_READY`를 시작점으로 한다.

## 실행

- Airflow DAG: `pop_talk_movie_databricks_daily`
- run: `manual__pipeline_v1_acceptance_20260909`
- source run: `4834bb8200e5a76f2cf8b408`
- publication revision: 6

## 판정

성공. 8개 task가 모두 한 DAG run 안에서 순차 성공했다.

- 전체 run 상태: `success`
- 실행 시간: 2026-09-09 10:22:09Z ~ 10:27:32Z, 약 323초
- Databricks run: `72724773223776`
- dbt build: 성공
- PostgreSQL publication:
  `22c6c9eb4785809280632a574959b1680d4096c40cf4ddec2ffb5c65dfcdd94b`
- PostgreSQL current movie: 107
- PostgreSQL current boxoffice: 70

성공 task:

1. `deploy_notebook`
2. `stage_bundle`
3. `transform_bronze_silver`
4. `publish_exchange`
5. `load_snowflake`
6. `identify_gold_build`
7. `build_snowflake_gold`
8. `publish_postgres`

이 결과로 sample 범위의 DW 파이프라인 v1은 S3의 수집 완료 manifest부터
PostgreSQL `dw_serving` 활성 snapshot까지 한 바퀴 실행된다. 원천 API 수집은 별도
선행 DAG이며, 기존 `dev.popcorn_movies` I/U, embedding, 사용자·관리자 API 연결은
후속 애플리케이션 통합 범위다.
