# Silver → S3 Exchange 구현 기록

구현일: 2026-09-09

## 범위와 승인 경계

이번 단계는 Databricks Bronze/Silver 처리가 성공한 데이터 파일을 S3 exchange에
불변 게시하는 데까지만 다룬다. Snowflake external stage/COPY, dbt Gold, PostgreSQL
게시 계약은 다음 단계이며 이 문서의 완료 범위가 아니다.

실행 경로:

`S3 Raw → Airflow 검증 bundle → Databricks Bronze/Silver 성공 → S3 data files → EXCHANGE_READY`

## 동일 데이터 보장

Airflow bridge는 순수 변환 결과를 다음 두 JSONL 파일로 결정적 ZIP에 포함한다.

- `silver_movies.jsonl`
- `silver_boxoffice.jsonl`

각 JSONL 행에는 Delta 관측과 동일한 `source_observed_at`, `ready_manifest_key`,
`transform_version`이 포함된다. Exchange는 current snapshot이 아니라 실행별
관측 데이터이며, Snowflake는 원본 S3 raw를 다시 추적하지 않고도 관측 순서를
복원할 수 있다.

Databricks notebook은 raw에서 Silver를 다시 계산한 뒤 두 JSONL의 전체 바이트와
비교한다. 이 검증과 Delta 적재·대사·SUCCESS ledger가 모두 완료되어야 후속
`publish_exchange` task가 실행된다. 따라서 단순 건수 일치가 아니라 Databricks가
검증한 정확한 행만 S3 게시 후보가 된다.

bundle 생성 계약도 실행 artifact에 포함되도록 artifact SHA-256 입력은
`databricks_bridge.py + exchange_publisher.py + movie_bronze_silver.py + notebook`
네 파일이다. publish task도 staged artifact와 현재 전체 artifact를 다시 비교한다.

## S3 게시 계약

경로는 다음 식별자로 고정된다.

`exchange/movie_silver/v1/source_run_id=<run>/artifact_version=<sha256>/publication_revision=<n>/`

data file을 먼저 쓰고, 체크섬·행 수·원천 READY key를 담은
`EXCHANGE_READY.json`을 마지막에 쓴다. 모든 객체는 기존 바이트가 같으면
재사용하고 다르면 실패한다. 동시 최초 쓰기는 S3 `If-None-Match: *`로 보호하며,
경쟁에서 진 실행은 기존 바이트를 다시 비교한다. 중간 실패로 data file만 남아도
READY가 없으므로 소비자는 해당 prefix를 읽지 않는다.

publisher는 S3 쓰기 전에 staged XCom의 run/artifact/ZIP SHA-256과 실제 bundle을
대사한다. bundle version, 필수 두 JSONL checksum 존재, ZIP 전체 파일 집합,
중복 entry, 모든 문서 checksum을 검증한다. READY에는 exchange contract version과
승인된 source bundle SHA-256도 기록한다.

## 검증 결과

- 변환/bridge/exchange 단위 테스트: 31개 통과
- Airflow DAG import error: 0
- source 변경, publication revision, immutable S3 재실행·충돌 회귀 테스트 포함
- artifact: `13c426d71a7801d85c1703ac5da0ebdf0c07d01608ec99f38c6ab10695dae195`
- publication revision: 3
- Airflow run: `manual__2026-09-09T08:14:50.106822+00:00`, 성공
- Databricks Jobs run: `603749798402763`, 성공
- S3 publish task: 성공
- 게시 행 수: 영화 107개, 박스오피스 70행
- 검토 수정 artifact: `30da23b77fb0ff53d412a5f6ff2c914a6e9339629cce91d3d2c4bbe8c68441e6`
- source bundle SHA-256: `1c8e50b681aa93a49f41c03008c0d3a6d5b3e8e83aa4504dae1f5d80d7bd452d`
- publication revision 4 / Airflow run `manual__2026-09-09T08:50:13.144375+00:00` 성공
- Databricks Jobs run `90167865389731` 및 S3 publish 성공
- checksum 누락·identity 변조·부분 실패 복구·412 동일/상이 경쟁 회귀 테스트 통과

## 검토 대상

- `pipelines/transforms/databricks_bridge.py`
- `pipelines/transforms/exchange_publisher.py`
- `pipelines/databricks/03_movie_bronze_silver_daily.py`
- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`
- `pipelines/transforms/tests/test_exchange_publisher.py`
- 기존 변환/bridge 회귀 테스트
