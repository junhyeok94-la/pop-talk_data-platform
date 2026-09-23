# Legacy candidate 순수 적재 계약 구현 기록

- 상태: 구현 후 검토 요청
- 승인 계획: `movie-legacy-candidate-load-plan-review-2026-09-10.md`
- 범위: 외부 시스템 쓰기 전의 순수 변환·bundle·S3 Exchange·Snowflake loader 계약

## 구현 내용

- `movie_bronze_silver.legacy_exchange_observations`
  - Legacy Silver를 canonical movie key 순으로 정렬한다.
  - `source_observed_at=null`, known=false, basis=`LEGACY_UNKNOWN`을 부여한다.
  - 처리 시각을 결정적 payload에서 제외하고 boxoffice를 명시적 0행으로 만든다.
- `legacy_candidate_bridge.py`
  - manifest allowlist와 path/body/source SHA·bytes·count를 검증한다.
  - manifest, 원본 bytes, 변환 소스, 결정적 Silver/빈 boxoffice를 고정 metadata ZIP으로 묶는다.
  - candidate Unity Catalog Volume 경로에 동일 archive만 허용한다.
- `legacy_candidate_exchange.py`
  - bundle의 정확한 파일 집합·bytes·SHA와 v2 lineage/time 계약을 재검증한다.
  - source SHA × artifact × revision candidate prefix에 data-first, READY-last로 게시한다.
  - boxoffice는 실제 빈 bytes와 빈 SHA를 강제한다.
- `legacy_candidate_snowflake.py`
  - READY path를 먼저 분해하고 모든 child key가 같은 candidate prefix인지 GET 전에 검증한다.
  - canonical JSONL, lineage, null/false/LEGACY_UNKNOWN, key 유일성을 적재 전에 검증한다.
  - CANDIDATE DDL/temp 입력은 BEGIN 전에 준비하고, observation/전체 key-hash-count 대사/
    SUCCESS ledger는 하나의 DML transaction에서 commit한다.
  - 성공 replay도 실제 observation을 다시 대사하며 실패 주입은 rollback한다.

기존 v1 publisher/reader/loader와 production schema 코드는 변경하지 않았다.

## 테스트

`pipelines/transforms/tests` 전체 47개 테스트가 통과했다.

```text
...............................................
Ran 47 tests in 0.036s
OK
```

신규 fixture는 결정적 bundle, manifest path/SHA 불일치, Volume immutable staging,
READY-last와 동일 replay, 실제 빈 boxoffice, child path 선검증, v2 시간 계약,
BEGIN 이후 DDL 부재, movie merge 직후 실패 rollback을 포함한다. 신규 모듈은 별도
`py_compile`도 통과했다.

1차 검토 반례를 반영해 manifest header와 정확한 SHA source path를 source GET 전에
검증한다. Snowflake loader도 reader를 신뢰하지 않고 DDL 전에 READY/source/artifact/time
계약을 다시 검사한다. 성공 ledger replay fixture는 실제 저장 count와 incoming 전체
key/hash 대사가 오염을 거부하는지 각각 검증한다.

재검토의 마지막 경계도 반영했다. 잘못된 manifest key는 함수의 최초 S3 GET보다 먼저
거부하며, fixture에서 S3 GET 0회와 Volume 쓰기 0회를 확인한다.

## 아직 하지 않은 일

Databricks notebook, Airflow runtime/DAG, 실제 S3/Databricks/Snowflake candidate 쓰기는
아직 수행하지 않았다. 이 계약을 독립 검토한 뒤 해당 어댑터를 구현한다.
