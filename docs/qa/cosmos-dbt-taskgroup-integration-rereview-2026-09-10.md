# Cosmos 통합 검증 재검토

판정: APPROVED — 앞선 통합 증빙 P2 두 건 종결. 정상 샘플 실행 및 신규 쓰기 실패 시 rollback/기존 게시본 보존 범위를 승인한다.

## PostgreSQL 신규 snapshot rollback

Airflow metadata에서 manual__cosmos_pg_new_write_full_retry_20260910의 선행 15개 success, publish_postgres failed/try_number=2를 확인했다. project_test 로그 120행은 PASS=43, ERROR/WARN/SKIP=0이다. stage 로그는 별도 source 9205b8da4dd4eb4b488b5a01과 processing_attempt=2를 나타낸다. publish_postgres의 두 attempt 로그 19행 모두 활성화 직전 주입 예외를 기록한다.

PostgreSQL read-only SELECT로 신규 publication 596d7b0e68562fce1e5221d5bc31974381e70e0c32a66b04f127dc451bb40401에 대해 다음을 확인했다.

- dataset_publications_v3 / movie_snapshot_v3 / boxoffice_snapshot_v3: 각각 0건.
- gold_build_registry_v3: 1건.
- 해당 run attempt: FAILED / Injected failure before activation.
- movie_gold active: 기존 d676420d8e2b44490545ef0a5955eda751d6cff7d859ec4810f830a00c53a2ef 유지.
- 현재 serving view: 영화 107건 / 박스오피스 70건.

기존 publication과 다른 identity이며 예외 지점까지 신규 INSERT/검증 블록을 통과하는 코드 경로와 일치한다. 이전의 같은 publication 재사용 실험과 구분되는 신규 snapshot rollback 증빙으로 수용한다.

## Snowflake 신규 영화 INSERT rollback

manual__cosmos_sf_new_insert_rollback_20260910은 별도 source ecc4cf4cc8fa25ea6acd0459를 사용했다. publish_exchange 로그 18행에서 영화 104 / 박스오피스 10건을 확인했다. load_snowflake의 attempt 1/2 로그 21행 모두 영화 MERGE 직후 주입 예외이며, metadata는 failed/try_number=2다.

동일 source/artifact를 Snowflake에서 직접 SELECT해 SILVER_EXCHANGE_LOADS / MOVIE_OBSERVATIONS_RAW / BOXOFFICE_OBSERVATIONS_RAW가 각각 0건임을 확인했다. 새로운 source identity, 비어 있지 않은 입력, MERGE 완료 후 예외 지점, 사후 0건 결과를 함께 대조해 신규 영화 관측 및 ledger rollback 증빙으로 수용한다. 박스오피스는 실행 전 실패이므로 그 INSERT rollback을 검증한 것으로 확대하지 않는다.

모델 7개, project_test, confirm_gold_build, publish_postgres 총 10개는 모두 upstream_failed/try_number=0이었다. PostgreSQL active와 107/70건도 그대로 유지됐다.

## 문서 및 승인 범위

기존 revision 8과 동일 PostgreSQL publication 실험은 재사용 경로 검증으로 범위가 정정됐다. 중단된 실행은 데이터 실패와 구분해 기록했으며, 이번 승인에서 일반적인 Docker/API 장애 복구나 worker A/B 전환까지 인증하지 않는다.

앞선 정상 16개 태스크/7개 모델/43개 테스트 및 참고 프로젝트 검토 결과를 유지한다. 이번에는 실제 Airflow 상태·로그와 두 DB의 SELECT만 수행했고 새 DAG 실행, 데이터 삭제·변경, 코드 수정은 하지 않았다. 추가 차단 사항 없음.
