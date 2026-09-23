# 영화 기준선 대사 v1 구현 기록

## 구현

- 순수 엔진: `pipelines/modeling/movie_baseline_reconciliation.py`
- 단위 테스트: `pipelines/modeling/tests/test_movie_baseline_reconciliation.py`
- 읽기 전용 실행 어댑터: `scripts/reconcile_movie_baseline.py`
- 실데이터 결과: `docs/qa/movie-baseline-reconciliation-result-2026-09-10.md`

순수 엔진은 외부 SDK를 사용하지 않고 ID 정규화, 상호 배타적 분류, 행수 보존, 필드 상태 비교, 동일 정책 적용, 관계 영향 집계를 수행한다. 실행 어댑터만 Airflow 연결과 외부 저장소를 사용한다.

## 안전 계약

- S3는 정확한 manifest key를 필수 입력으로 받는다.
- manifest의 schema/status/bucket/key/bytes/SHA-256/record count를 검증한다.
- PostgreSQL은 단일 `REPEATABLE READ READ ONLY` 트랜잭션에서 고정 컬럼만 읽는다.
- Snowflake는 current 영화 ID와 각 영화가 선택한 source run×artifact 계보를 동일 SELECT 행에서 읽는다.
- stdout에는 상태·행 수·결과 경로만 출력한다.
- 결과 JSON에는 집계, 허용된 원천 식별자, hash와 계보만 포함하고 원문·DSN·토큰을 넣지 않는다.
- 업무 데이터는 변경하지 않는다.

## 검증

Airflow 컨테이너에서 단위 테스트 11개와 쓰기 없는 Python compile 검사가 통과했다.

```text
...........
Ran 11 tests in 0.005s
OK
```

테스트 범위:

- 선행 0·대소문자를 보존하는 KOFIC ID 정규화
- missing/invalid 구분
- 양쪽 중복과 상대편 단일 행의 ambiguous 격리
- 세 원천 행수 보존
- 날짜·boolean 파싱 오류
- casefold 충돌
- Gold 중복 격리
- legacy manifest checksum·record count 부정 테스트
- 허용되지 않은 manifest의 원본 객체 GET 차단 테스트
- 오류 출력 코드 allowlist와 비밀 문자열 비포함 테스트
- 정책·boolean 고정 fixture
- 서비스 관계의 고아 행 집계

실데이터 실행은 성공했고 Legacy 5,985, Service 5,319, Gold 118행을 대사했다. 기계 결과는 Git에서 제외된 `.local/airflow-workbench/data-modeling/movie-baseline-reconciliation.json`에 생성됐다.

## 실행 중 발견하고 수정한 사항

1. Airflow 3.3 일반 CLI 프로세스에서는 Provider Hook이 Task SDK 컨텍스트 밖의 Connection을 찾지 못했다.
2. Airflow 메타DB의 암호화된 Connection을 서버 내부에서만 읽고 boto3/Snowflake connector에 전달하도록 어댑터를 수정했다.
3. 국가 비교용 `production_countries`가 최초 PostgreSQL SELECT에서 빠진 것을 결과 검토로 발견해 추가했다.
4. 국가 필드 수정 후 테스트와 실데이터 대사를 다시 실행해 최종 결과를 생성했다.
5. 날짜를 `YYYYMMDD`, `YYYY-MM-DD`, date/datetime만 허용하도록 강화하고 비문자열 KOFIC ID를 invalid로 격리했다.
6. Snowflake 영화 ID와 선택 계보를 두 SELECT로 읽던 방식을 동일 SELECT 행 추출로 수정했다.

## 비목표

이번 구현은 대사와 기준선 결정까지만 수행한다. Legacy를 Databricks/Snowflake에 적재하거나 bridge DDL을 만들거나 서비스 데이터를 수정하지 않았다.
