# S3 Exchange → Snowflake STAGING 구현 기록

구현일: 2026-09-09

## 범위

승인된 S3 `EXCHANGE_READY.json`을 읽어 Snowflake 실행별 관측 테이블에 멱등
적재한다. 이번 단계는 STAGING까지만이며 dbt DW/MART는 다음 검토 단계다.

초기 구현은 Databricks Free Edition 제약을 고려해 Airflow가 S3 객체를 검증해
Snowflake Python connector로 중계한다. S3 Exchange가 기준 저장소이며 임의 로컬
파일이나 READY 없는 prefix는 소비하지 않는다.

## 입력 검증

- READY/exchange contract version 고정
- READY path의 run/artifact/revision과 본문 identity 일치
- movies/boxoffice 정확한 필수 파일 집합
- 파일 SHA-256과 행 수
- 각 행의 source run, transform version, 원천 READY, source_observed_at
- 영화 canonical key 및 박스오피스 date+KOFIC key의 실행 내 유일성

## Snowflake 적재

- `STAGING.SILVER_EXCHANGE_LOADS`: READY별 상태·identity·대사 건수
- `STAGING.MOVIE_OBSERVATIONS_RAW`: 실행/artifact/영화별 immutable 관측
- `STAGING.BOXOFFICE_OBSERVATIONS_RAW`: 실행/artifact/일자/영화별 immutable 관측

JSONL은 트랜잭션 시작 전에 문자열 임시 테이블에 적재한 뒤 Snowflake
`PARSE_JSON`으로 VARIANT화한다. Snowflake DDL의 암시적 commit이 원자성을
깨뜨리지 않도록 영구 테이블과 임시 테이블 생성, 입력 준비를 모두 `BEGIN`보다
앞에서 끝낸다.

기존 업무 키의 record SHA가 다르면 실패한다. ledger DML, 관측 MERGE, 대사,
SUCCESS 전환만 하나의 명시적 트랜잭션에 포함하며 실패 시 rollback한다. 성공
READY 재실행도 ledger 수치만 신뢰하지 않고 영구 관측 테이블의 실제 key/count/hash를
입력과 다시 대사한 뒤 종료한다.

## 검증 결과

- 전체 단위 테스트 33개 통과
- DAG import error 0
- 첫 두 실제 시도에서 JSON alias SQL 컴파일 오류를 발견했으며 두 실행 모두
  transaction rollback 후 수정
- 성공 Airflow run: `manual__2026-09-09T09:12:20.057180+00:00`
- 동일 READY 재실행: `manual__2026-09-09T09:13:32.218709+00:00`, 성공
- Snowflake movie observations: 107
- Snowflake boxoffice observations: 70
- 재실행 반환 건수: 107 / 70, 중복 없음

### 최초 검토 지적 보완

- 임시 테이블 DDL과 입력 준비를 `BEGIN` 앞으로 이동해 transaction 내부 DDL 제거
- movie MERGE 직후 고의 예외를 발생시키는 failure injection 추가
- SUCCESS replay 시 영구 movie/boxoffice 관측의 key/count/hash 재대사 추가
- 격리된 identity를 사용하는 실제 Snowflake transaction probe DAG 추가

실제 검증 실행:

- revision 5 고의 실패: Databricks Jobs run `47388480226224`; movie MERGE 직후
  예외 발생 및 Airflow task 실패 확인
- revision 5 정상 재실행: Airflow
  `manual__2026-09-09T09:26:35.075941+00:00`; 107 / 70 성공
- 격리 rollback probe: Airflow
  `manual__2026-09-09T09:29:02.908775+00:00`
- probe 결과: `ROLLBACK_CONFIRMED`, movie observation 0, ledger 0

probe는 기존 artifact와 겹치지 않는
`rollback-probe-20260909-v1` identity를 사용한다. 따라서 movie MERGE가 실제로
실행된 뒤에도 대상 행과 ledger 행이 모두 0이라는 결과로 트랜잭션 rollback을
검증했다.

## 다음 단계 계약

동일 artifact가 더 높은 publication revision으로 다시 게시되어도 immutable
observation은 처음 적재된 revision을 유지할 수 있다. 다음 dbt 단계는 observation의
`PUBLICATION_REVISION`을 최신 게시 판단에 사용하지 않고, 반드시
`SOURCE_RUN_ID + ARTIFACT_VERSION`으로 SUCCESS load ledger와 결합해 승인된 게시
revision을 선택해야 한다.

## 검토 대상

- `pipelines/transforms/snowflake_exchange_loader.py`
- `pipelines/transforms/tests/test_snowflake_exchange_loader.py`
- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`
- `orchestration/airflow/dags/pop_talk_snowflake_transaction_probe.py`
