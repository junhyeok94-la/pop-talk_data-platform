# Snowflake Gold → PostgreSQL Serving 구현 기록

구현일: 2026-09-09

## 범위와 소유권

검증된 Snowflake `DIM_MOVIE`, `FCT_BOXOFFICE_DAILY`, `MART_DATA_QUALITY`를
PostgreSQL `dw_serving`에 versioned snapshot으로 게시한다. `dev.popcorn_movies`의
관리자 승인·노출·편집값과 회원·리뷰 데이터는 변경하지 않는다.

## 게시 구조

- `dataset_publications`: content hash와 기대 건수를 가진 불변 게시본
- `movie_catalog_snapshot`, `boxoffice_daily_snapshot`: publication별 전체 snapshot
- `active_publications`: 소비자가 읽을 단일 활성 포인터
- `movie_catalog_current`, `boxoffice_daily_current`: 활성본만 노출하는 views
- `publish_attempts`: 게시 transaction과 분리된 RUNNING/SUCCESS/FAILED 시도 이력

Gold 품질 대사가 깨끗하지 않으면 PostgreSQL에 접근하기 전에 실패한다. 게시본 identity는
SUCCESS ledger의 run/artifact/revision/READY 집합으로 만들고, movie/fact key+record hash로
별도 content hash를 만든다. 같은 identity의 내용이 달라지면 거부한다.

## 최초 검토 보완

- v2 테이블에 `source_as_of + max_revision + generation_completed_at` Gold generation을
  명시하고 dataset별 advisory lock/CAS 검사를 추가
- 구 generation 재실행이 현재 활성본보다 같거나 오래되면 활성 포인터 교체 거부
- upstream record hash 목록이 아니라 게시할 전체 행의 정규화 JSON으로 row/snapshot
  hash를 계산
- 저장 snapshot의 모든 row hash를 입력과 재대사하여 외부 변조·부분 저장을 거부
- active pointer 갱신과 attempt SUCCESS 전환을 같은 transaction으로 이동
- commit 결과가 불확실한 예외에서는 active pointer와 SUCCESS를 재조회해 둘 다 확인될
  때만 성공으로 복구하며, 재조회 자체가 불가능하면 FAILED로 오기록하지 않음
- 현재 앱 원장과 I/U trigger 연결은 이 단계 범위가 아니며, 이번 완료는
  `dw_serving` 활성 snapshot까지임

DDL과 시도 시작 기록은 게시 transaction 전에 commit한다. publication metadata,
전체 snapshot, 건수 대사, 활성 포인터 교체는 한 transaction이다. 실패하면 새 게시본은
전부 rollback되고 기존 활성 포인터가 유지되며, FAILED 시도만 별도 commit된다.

## Airflow

기존 DAG의 `build_snowflake_gold` 다음에 `publish_postgres` task를 연결했다.
Snowflake connection과 Airflow 서비스 환경의 전용 PostgreSQL 접속값을 사용한다.

## 실제 검증

- 최초 게시 성공: publication
  `bad2e8efaa9273e031a4c95cec1141e017be1ba9fc8d866d40c908f159e69f89`
- PostgreSQL current movie: 107
- PostgreSQL current boxoffice: 70
- 같은 Gold 재실행: 107 / 70, 새 snapshot 중복 없음
- 격리 publication `rollback-probe-postgres-20260909`를 적재 후 활성화 직전에 실패 주입
- probe 결과: dataset publication 0, movie snapshot 0, attempt `FAILED`
- 실패 후 활성 publication 불변, current movie 107 / boxoffice 70 유지
- 격리 dataset에서 A(old) → B(new) → A(old replay) 실행 결과 구 A 차단 및 B 유지

## 검토 대상

- `pipelines/dbt/scripts/publish_to_postgres.py`
- `pipelines/dbt/scripts/tests/test_publish_to_postgres.py`
- `scripts/probe_postgres_publication.py`
- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`

## 2차 재검토 보완 — v3

- dbt SQL/YAML 전체의 deterministic SHA-256을 Gold `model_version`으로 고정
- Airflow가 dbt 실행 전에 identity를 계산하고 게시 직전 같은 해시인지 재검증
- publication identity를 `model_version + SUCCESS source identities`로 생성하여 같은
  원천이라도 dbt 로직이 바뀌면 새 게시본 생성
- PostgreSQL sequence로 dataset 전역 generation을 발급하고, advisory lock 아래에서
  active generation보다 작은 기존 publication의 재활성화를 거부
- snapshot 저장을 canonical JSONB 단일 값으로 변경하고, replay마다 실제 저장 JSONB를
  다시 정규화·해시하여 입력 전체 행과 대사
- 실제 v3 결과: publish 107/70, replay 107/70, rollback publication 0 및 FAILED,
  current 107/70 유지, 격리 A→B→A에서 B 유지
- `dw_serving` current view는 v3 active snapshot을 가리킨다. 기존 서비스 원장의
  I/U trigger 연동은 후속 앱 통합 범위다.

## 3차 재검토 보완 — 실패 전 generation 예약

- snapshot transaction보다 앞서 commit되는 `gold_build_registry_v3` 추가
- Gold build identity 최초 확인 시 전역 sequence generation을 예약하며, 이후 게시가
  rollback되어도 예약 순서는 보존
- dataset publication은 새 generation을 발급하지 않고 registry 예약값을 사용
- 실제 격리 검증: A generation 예약 후 활성화 전 실패 → B 최초 게시 성공 → 아직
  dataset publication이 없던 A 최초 재시도. A를 stale로 거부하고 B active 유지
- 동일 내용이라도 model version이 다르면 유효한 별도 Gold build이므로 snapshot content
  hash의 전역 UNIQUE 제약은 제거하고 publication identity 내부 일치로 검증
