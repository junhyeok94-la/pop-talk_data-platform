# Legacy 영화 candidate 격리 적재 구현 계획

- 상태: 선검토 요청
- 상위 설계: `movie-baseline-ingestion-bridge-design-2026-09-10.md`
- 선행 승인: `movie-identity-contract-implementation-rereview-2026-09-10.md`
- 입력: 검증된 Legacy SUCCESS manifest 1개와 그 manifest가 가리키는 원본 bytes
- 출력: candidate 전용 Databricks Delta, S3 Exchange v2, Snowflake candidate observation

## 1. 이번 단계의 완료 범위

이번 단계는 Legacy 5,985건만 격리 경로에 적재한다. Service-preserved snapshot,
candidate Gold, production staging/dbt, PostgreSQL serving은 다음 단계다.

```text
S3 Legacy SUCCESS + 원본
  → Airflow에서 hash/bytes/count 검증 및 불변 bundle 생성
  → Databricks candidate Bronze 1개 원본 객체 + Silver 5,985개 관측
  → S3 candidate Exchange v2 (movie 5,985, boxoffice 0)
  → Snowflake POP_TALK_DW_DEV.CANDIDATE observation 5,985개
  → 세 저장소 key/hash/count 대사
```

candidate 처리 태스크는 기존 `pop_talk_movie_databricks_daily`,
`workspace.bronze.movie_raw_objects`, `workspace.silver.movie_observations`,
`POP_TALK_DW_DEV.STAGING`, dbt Gold와 PostgreSQL 게시 테이블을 읽거나 변경하지 않는다.
승인 검증자는 이 운영 객체들을 실행 전후에 **읽기 전용**으로 fingerprint해 격리를 확인한다.

## 2. 입력과 실행 계약

- 수동 DAG: `pop_talk_movie_legacy_baseline_candidate`
- 필수 Param: 정확한 `manifests/legacy_snapshot/v1/sha256=<64hex>/SUCCESS.json`
- 기본 Param은 이미 대사한 SHA manifest로 두되 정규식과 본문 hash를 모두 검증한다.
- 원본은 재수집하거나 변환 결과로 덮어쓰지 않는다.
- XCom은 manifest key, source SHA, artifact version, archive SHA, 건수만 전달한다.
- `source_run_id = legacy:<manifest.sha256>`이며 manifest key의 SHA 경로, manifest 본문
  SHA, source object SHA가 모두 같아야 한다.
- publication identity와 S3 namespace는 source run × artifact × revision이다. 같은
  identity는 동일 결과를 재사용하고 payload가 다르면 실패한다. 다른 artifact는 같은
  숫자 revision을 별도 namespace에서 사용할 수 있으며 서로의 행을 덮어쓰지 않는다.
- candidate 완료 Asset은 발행하지 않아 Main DAG가 자동 시작되지 않게 한다.

Databricks Free Edition이 외부 S3를 직접 읽지 못할 수 있으므로, 현재 검증된 방식처럼
Airflow가 S3 bytes를 읽어 Unity Catalog Volume의 불변 bundle로 전달한다.

DAG는 `max_active_runs=1`이고 Databricks/S3/Snowflake 쓰기 태스크는 candidate 전용
pool 또는 `max_active_tis_per_dag=1`로 직렬화한다. 원격 submit token은
manifest key × source SHA × artifact × revision × processing attempt의 SHA-256이다.
동일 입력·동일 attempt의 태스크 재시도와 서로 다른 수동 DagRun은 같은 token과 결과를
재사용한다. 원격 실행이 확정 실패해 새 처리를 만들 때만 attempt를 올린다.

## 3. bundle과 변환 계약

bundle에는 다음 파일만 들어간다.

- `SUCCESS.json`: 원본 manifest bytes
- `source/movies_final.json`: 원본 bytes 그대로
- `movie_bronze_silver.py`: 실제 실행할 순수 변환 소스
- `silver_movies.jsonl`: 로컬에서 사전 계산한 결정적 결과
- `silver_boxoffice.jsonl`: 명시적인 0행 파일
- `bundle_index.json`: 각 문서 SHA-256, 입력/출력 건수, source/artifact identity

Databricks는 bundle checksum을 검증하고 같은 변환 소스로 결과를 다시 계산하여
사전 계산 JSONL과 byte 단위로 비교한다. Bronze에는 원본 S3 object 한 개의 key,
manifest key, source SHA, 원본 payload를 보존한다. Silver movie grain은
source run × artifact version × canonical movie key다.

artifact digest에는 candidate notebook, `movie_bronze_silver.py`, v2 enrichment와
manifest/bundle 검증·canonical JSONL 직렬화를 가진 `legacy_candidate_bridge.py`의 정확한
bytes와 상대 경로를 정렬해 포함한다. JSON은 key 정렬·공백 없는 UTF-8, JSONL은
canonical JSON 한 행 뒤 LF 하나, row는 canonical movie key 순으로 고정한다. ZIP entry도
이름 순서, 1980-01-01 timestamp, 고정 mode/compression으로 만든다. `ingested_at`과
`processed_at`은 bundle/JSONL/artifact hash에서 제외하고 Delta/Snowflake 저장 시각으로만
기록한다.

Legacy Silver/Exchange v2 시간 상태는 다음으로 고정한다.

- `source_observed_at = null`
- `source_observed_at_known = false`
- `source_time_basis = LEGACY_UNKNOWN`
- `ingested_at`, `processed_at`은 저장소 처리 시각이며 현재값 우선순위에 사용하지 않는다.

## 4. 격리된 저장 위치

| 계층 | 위치 |
|---|---|
| Databricks landing | `/Volumes/workspace/bronze/landing/movie_legacy_candidate/<source_sha>/<artifact>` |
| Databricks Bronze | `workspace.bronze.movie_legacy_candidate_raw` |
| Databricks Silver | `workspace.silver.movie_legacy_candidate_observations` |
| Databricks ledger | `workspace.silver.movie_legacy_candidate_runs` |
| S3 Exchange | `exchange/candidate/movie_legacy/v2/...` |
| Snowflake | `POP_TALK_DW_DEV.CANDIDATE` schema |

Snowflake에는 `LEGACY_EXCHANGE_LOADS`와 `MOVIE_OBSERVATIONS_RAW`만 만든다.
movie의 `SOURCE_OBSERVED_AT`은 nullable이다. loader가 모든 행에 대해
null/false/`LEGACY_UNKNOWN`을 **적재 전에** 검사하고 다른 조합을 거부한다.

Snowflake 실행 순서는 다음으로 고정한다.

1. candidate schema/table DDL과 임시 입력 준비를 `BEGIN` 전에 완료한다.
2. `BEGIN` 후 ledger identity/replay와 기존 observation key/hash conflict를 검사한다.
3. observation을 insert/merge하고 저장된 key/hash/count를 incoming 전체와 대사한다.
4. 같은 트랜잭션에서 SUCCESS ledger를 기록한 뒤 COMMIT한다.
5. 실패 시 BEGIN 이후의 observation과 ledger DML 전체를 rollback한다. DDL까지
   rollback되었다고 간주하지 않는다.

성공 replay도 ledger count만 신뢰하지 않고 실제 observation key/hash/count를 다시
검증한다. 테스트는 BEGIN 이후 DDL이 없음을 확인하고 movie merge 직후 실패 주입 시
부분 observation과 SUCCESS ledger가 남지 않음을 확인한다.

boxoffice 0행은 “Legacy 원천에는 박스오피스 관측이 없다”는 뜻이며 관객/매출이 0이거나
기존 fact를 삭제한다는 뜻이 아니다. candidate fact 테이블은 만들지 않는다.

## 5. 코드 단위

- `movie_bronze_silver.py`: Legacy Exchange v2 enrichment와 시간 계약
- 신규 `legacy_candidate_bridge.py`: manifest/source 검증, 결정적 bundle staging
- 신규 `legacy_candidate_exchange.py`: candidate prefix에 data-first, READY-last 게시
- 신규 `legacy_candidate_snowflake.py`: CANDIDATE schema 트랜잭션 적재와 replay 검증
- 신규 Databricks notebook: candidate Delta 쓰기와 ledger
- 신규 runtime module: Airflow 비의존 실행 어댑터
- 신규 DAG: 얇은 task/operator 경계와 한글 계약 설명

publisher/reader는 READY가 선언한 파일 key를 GET하기 전에 allowlist로 정확한 candidate
prefix와 source/artifact/revision 대응을 검증한다. adapter/notebook/loader도 각 쓰기
대상의 정적 allowlist를 검사하며 candidate namespace 밖 쓰기를 거부한다.

기존 production publisher/loader의 v1 동작은 수정하지 않는다. 공통화 때문에 production
경계를 넓히지 않고, v2가 검증된 뒤 후속 전환 단계에서 호환 reader를 통합한다.

## 6. 테스트와 승인 조건

순수 테스트에서 다음을 고정한다.

- manifest path/header/hash/bytes/count 및 원본 배열 shape
- 5,985/4,320/1,665 fixture 또는 실제 manifest 대사
- KOFIC key 유일성, missing KOFIC의 결정적 legacy-row key
- v2 null/false/LEGACY_UNKNOWN 조합과 반대 조합 거부
- bundle 파일 집합/checksum/사전계산 Silver byte 일치
- candidate S3 path identity, data-first/READY-last, immutable replay/conflict
- READY의 정확한 `movies.jsonl`, `boxoffice.jsonl` 파일 집합과 동일 publication prefix,
  bytes/hash/row_count 검증. boxoffice는 실제 빈 bytes, bytes=0, row_count=0 및 빈 bytes의
  SHA-256을 요구하며 공백만 있는 파일도 거부
- Snowflake candidate transaction rollback, 성공 replay, key/hash/count 검증
- DAG import error 0, parse-time 외부 I/O 없음, candidate 쓰기 allowlist 검증

실제 candidate 실행 승인 조건은 다음과 같다.

- Databricks Bronze 1, Silver 5,985, eligible 4,320, excluded 1,665
- S3 movies 5,985, boxoffice 0과 모든 checksum 일치
- Snowflake candidate movie 5,985 및 source SHA/artifact lineage 일치
- 동일 DAG run 재실행 시 건수가 증가하지 않음
- candidate 실행 전 진행 중인 production/Airflow/Databricks 작업이 없음을 기록하고 짧은
  비교 구간을 고정한다.
- 읽기 전용 승인 검증에서 Databricks 운영 Delta version/history와 key/hash,
  기존 S3 Raw/Exchange key/ETag·metadata hash, Snowflake API observation key/record hash와
  Gold 의미 컬럼 digest, PostgreSQL active publication ID와 내용 digest가 전후 동일하다.
  직접 조회가 제한된 항목은 조회 실패를 성공으로 간주하지 않고 가능한 version/history,
  API 응답 또는 실행/쿼리 기록과 그 검증 한계를 결과 문서에 남긴다.

## 7. 다음 단계

이 단계 독립 승인 뒤 Service-preserved snapshot을 같은 candidate schema에 별도 lineage로
적재한다. 그 후 registry/bridge/attribute-current/dim_movie candidate Gold를 구축한다.
