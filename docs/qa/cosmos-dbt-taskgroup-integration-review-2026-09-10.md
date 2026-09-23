# Cosmos 샘플 통합 검토

판정: CHANGES REQUESTED — 정상 경로와 실패 전파는 확인했으나, 신규 데이터 쓰기 후 rollback에 대한 두 검증 주장이 실제 실행보다 넓다. 구현 결함을 새로 발견했다는 뜻은 아니며 통합 증빙 보완 요청이다. 코드와 외부 데이터는 변경하지 않았다.

## P2 — revision 8 검증은 신규 영화 행 rollback을 입증하지 않음

근거: 통합 문서 82행, pipelines/transforms/snowflake_exchange_loader.py:203-210.

revision 8은 revision 7과 같은 source_run_id/artifact_version 및 동일 bundle을 사용했다. loader MERGE의 ON은 source_run_id + artifact_version + 영화 key이며 publication_revision을 포함하지 않는다. 따라서 이미 revision 7로 존재하는 107개 영화가 모두 MATCHED되고 신규 INSERT 없이 예외 지점에 도달한다. 이는 기존 관측 재사용 계약에 맞지만 신규 영화 데이터 rollback 검증은 아니다.

독립 Snowflake SELECT에서 같은 source/artifact에 ledger revision 7=1, movie revision 7=107, boxoffice revision 7=70이며 revision 8은 모두 0임을 확인했다. 이 결과와 예외 경로는 신규 revision 8 ledger rollback 및 실패 전파를 뒷받침한다. 영화 revision 8의 0건은 MERGE가 처음부터 삽입하지 않은 결과와 구분되지 않는다. 박스오피스 MERGE는 영화 직후 실패 때문에 실행되지 않는다.

권고: 기존 관측 key와 겹치지 않는 승인된 표본 source/artifact로 영화 MERGE의 실제 INSERT가 발생했음을 확인하고, 예외 후 영화와 ledger가 모두 0이며 기존 정상 데이터가 유지되는지 검증한다. 기존 데이터를 삭제해 신규 상황을 만들지 않는다. 현재 문서에는 확인 범위를 ‘ledger rollback + 기존 관측 보존 + downstream 차단’으로 정확히 구분한다.

## P2 — PostgreSQL 실패 검증도 기존 publication 재사용 경로임

근거: 통합 문서 59행, pipelines/dbt/scripts/publish_to_postgres_v3.py:85-96.

독립 PostgreSQL SELECT에서 정상 실행과 manual__cosmos_postgres_fail_20260909의 publication_id는 모두 d676420d8e2b44490545ef0a5955eda751d6cff7d859ec4810f830a00c53a2ef였다. 이미 존재하는 publication이면 신규 dataset/movie/boxoffice snapshot INSERT 블록을 건너뛴다. 따라서 확인된 것은 같은 게시본 재검증 중 활성화 직전 실패, FAILED attempt 기록 및 active 보존이다. ‘새 snapshot을 준비하다 실패’한 경로는 실행하지 않았다.

권고: 실제 다른 publication identity를 만드는 표본으로 신규 snapshot INSERT 후 활성화 직전 실패를 검증한다. 이전 active ID와 실제 payload/count가 유지되고 신규 dataset/snapshot 행이 rollback됐는지 대사한다. registry reservation과 FAILED attempt는 의도적으로 남는 상태이므로 별도로 기록한다. 현재 결과를 신규 snapshot rollback 증빙으로 표현하지 않는다.

## 독립 확인한 성공 결과

- Airflow metadata SELECT: manual__cosmos_acceptance_20260909의 16개 태스크 모두 success. 모델 7개와 project_test 포함.
- project_test/attempt=1.log:119에서 PASS=43, WARN/ERROR/SKIP=0 확인. START test 이름 집합을 현재 고정 manifest test 이름 집합과 대사해 43개, 누락/추가 0. 현재 범위에서 이름 충돌 없이 집합 대사 가능하다.
- publish_postgres/attempt=1.log:18의 publication ID 및 107/70건과 실제 PostgreSQL active 상태 일치. movie_gold generation 16, quality_count 2, model_version 2276279b... 확인.
- PostgreSQL 실패 run은 publish_postgres failed/try_number=2이며 선행 15개 success. attempt는 FAILED / Injected failure before activation. 현재 serving view 107/70건 유지.
- revision 7 snowflake_fail run은 전체 success이며 동일 publication을 재사용했다. 신규 실패 주입 검증으로 계산하지 않은 문서 구분은 맞다.
- revision 8 rollback run은 load_snowflake failed/try_number=2. 모델 7개, project_test, confirm_gold_build, publish_postgres 총 10개가 upstream_failed/try_number=0임을 확인했다.
- Snowflake는 기존 profiles.yml의 RSA 연결로 SELECT만 실행했다. 앞선 hook 호출은 일반 CLI 문맥에서 SDK Connection 조회가 불가능해 실패했으며, 이후 기존 profile을 직접 사용했다. 자격 증명 내용은 출력하지 않았다.

## 외부 참고 프로젝트 대조

D:/01.DEV/dbt-airflow/airflow/hbu/include/dbt_common_config.py와 airflow/hbu/dw/bi/daily/bi_daily_cmm_pb_03.py를 직접 읽었다. manifest 지정, 별도 dbt 실행 파일, SUBPROCESS, emit_datasets=False, DbtTaskGroup 설정 분리 방향을 참고한 것은 적절하다. 대표 DAG는 AFTER_EACH와 공유 target_for_daily를 사용하며 현재 프로젝트가 전체 AFTER_ALL과 태스크별 임시 작업 공간으로 달리 적용한 이유도 타당하다. 공통 helper 자체의 test 기본값은 NONE이고 대표 DAG가 AFTER_EACH를 명시한다는 점을 구분한다.

## 승인 경계

정상 샘플 경로 및 위에서 직접 확인한 실패 전파/기존 상태 보존은 수용한다. 두 신규 쓰기 rollback 시나리오의 추가 증빙 또는 검증 범위 재정의가 필요하므로 전체 통합 검증 판정은 보완 요청이다. Docker 장애 복구의 원격 작업 무중복 주장과 실제 worker A/B 재구성은 이번 조회만으로 추가 인증하지 않는다. 기존 코드 검토 승인을 철회하는 것은 아니다.
