# Legacy candidate notebook/runtime/DAG 구현 기록

- 상태: 구현 후 검토 요청
- 선행 승인: `movie-legacy-candidate-contract-review-2026-09-10.md`
- 외부 쓰기 상태: 아직 candidate DAG를 실행하지 않음

## 구현 흐름

신규 수동 DAG `pop_talk_movie_legacy_baseline_candidate`는 다음 6개 태스크로 보인다.

```text
deploy_notebook
  → stage_bundle
  → transform_candidate_bronze_silver
  → publish_exchange
  → load_snowflake
  → confirm_candidate
```

- notebook은 digest 경로에 overwrite 없이 생성하고, 이미 있으면 export bytes가 로컬
  source와 정확히 같을 때만 재사용한다.
- artifact digest는 notebook, Legacy validation/transform/bundle, Databricks transfer,
  S3 v2 publisher, Snowflake v2 loader, runtime source의 상대 경로와 bytes를 포함한다.
- stage 결과의 submit token은 manifest/source/artifact/revision/attempt로 만들며 Airflow
  DagRun ID를 포함하지 않는다.
- Databricks notebook은 사전 계산 Silver JSONL과 원격 재계산 결과를 byte 비교한 뒤
  candidate Bronze/Silver/ledger만 쓴다.
- S3와 Snowflake 단계는 앞서 승인된 candidate allowlist 및 v2 계약을 그대로 호출한다.
- 마지막 barrier는 동일 lineage와 5,985/4,320/1,665/movie, boxoffice 0을 대사한다.

DAG는 `schedule=None`, paused, `max_active_runs=1`이며 Asset outlet, dbt, PostgreSQL 태스크가
없다. 외부 쓰기 태스크는 `max_active_tis_per_dag=1`이다.

## 저장 대상

- `/Volumes/workspace/bronze/landing/movie_legacy_candidate/...`
- `workspace.bronze.movie_legacy_candidate_raw`
- `workspace.silver.movie_legacy_candidate_observations`
- `workspace.silver.movie_legacy_candidate_runs`
- `exchange/candidate/movie_legacy/v2/...`
- `POP_TALK_DW_DEV.CANDIDATE.LEGACY_EXCHANGE_LOADS`
- `POP_TALK_DW_DEV.CANDIDATE.MOVIE_OBSERVATIONS_RAW`

## 검증

- transforms 전체: 47 tests OK
- orchestration 전체: 24 tests OK
- notebook/runtime/DAG `py_compile`: OK
- `airflow dags list-import-errors --output json`: `[]`
- 실제 DAG 태스크 목록: 위 6개와 일치

신규 runtime fixture는 선언된 모든 artifact dependency 변경 시 digest 변경, 기존 digest
notebook의 동일 bytes 재사용/다른 bytes 거부, 서로 다른 호출의 동일 논리 입력 submit token
일치를 검증한다.

1차 검토 지적을 반영했다.

- 실제 DagBag에서 `transform_candidate_bronze_silver`의 upstream이 `stage_bundle`임을
  확인했다.
- notebook은 source/archive SHA widget과 정확한 landing/manifest identity를 먼저
  검증하고, archive 전체 SHA를 확인한 뒤에만 ZIP을 열고 동적 변환 소스를 실행한다.
- Snowflake runtime은 S3 GET/DB cursor 전에 현재 전체 artifact digest가 publish 단계의
  artifact와 같은지 다시 확인한다.
- 관련 정적 순서와 load pre-I/O 실패 fixture를 추가했다.

## 다음 승인 후 실행

candidate 실행 전 읽기 전용 production fingerprint를 저장하고 진행 중 운영 run이 없는지
확인한다. 그 후 DAG를 한 번 실행해 Databricks/S3/Snowflake 실건수와 lineage를 검증하고,
같은 입력을 다시 실행해 멱등성을 확인한 뒤 production fingerprint를 다시 비교한다.
