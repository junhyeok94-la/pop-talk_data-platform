# DW → dev 직접 적재

현재 흐름은 S3 원천 → Databricks → Snowflake STAGING → dbt DW 모델·검사 → PostgreSQL dev → WAS → 화면이다.
`pop_talk_dw_serving`은 이 마지막 계산·적재 DAG의 이름이며 PostgreSQL 스키마 이름이 아니다.

## 데이터 소유권

| 대상 | 역할 |
| --- | --- |
| `dev.popcorn_movies` | 서비스 영화 원장. 정책 적격 DW 영화를 KOFIC 코드로 UPSERT |
| `dev.movie_boxoffice_daily` | KOFIC 코드·기준일별 흥행. 영화 미등록 시 movie_id는 NULL |
| `dev.batch_runs` | 적재 성공·실패, 입력 기준 시각과 변경 건수 |
| 기존 리뷰·미디어·편집·승인 테이블 및 컬럼 | 서비스가 관리하며 배치는 보존 |

기존 영화 ID와 승인 상태는 유지한다. 신규 영화는 DRAFT/PENDING으로 등록하며 공개 API는 PUBLISHED/APPROVED 영화만 제공한다. 원천의 비어 있는 값으로 기존 정보를 지우지 않는다.
영화·흥행·성공 이력을 한 트랜잭션에 반영한다. 실패하면 데이터 변경을 롤백한다. 같은 입력의 재시도는 중복 변경하지 않고 이전 시점 실행은 최신 데이터를 덮어쓰지 못한다.

## 실제 반영 결과

- 영화 5,319 → 5,352건: 신규 33건, 기존 정보 변경 76건. 신규는 승인 대기.
- 기존 영화 5,319건의 ID·운영 상태 보존. 리뷰 124,944건, 미디어 52,082건 보존.
- 정책 적격 4,356건을 대상으로 동기화하며 제외 1,719건은 서비스 원장을 변경하지 않음.
- 일별 흥행 80건 이관. 영화 6의 흥행 API 응답 확인, 신규 승인 대기 영화 16254의 공개 상세 404 확인.
- `dev_direct_service_20260911`: 6개 태스크, 모델 7개, dbt 검사 43개 성공. 재실행의 영화·흥행 변경 0건.
- PostgreSQL 통합 테스트 3개, WAS 테스트 16개, 챗봇 저장소 테스트 7개 통과. 챗봇 서버 기동은 별도.

## 제거 및 백업

`dw_serving`의 `active_publications_v3`, `boxoffice_snapshot_v3`, `dataset_publications_v3`, `gold_build_registry_v3`, `movie_snapshot_v3`, `publish_attempts_v3` 테이블, `movie_catalog_current`, `boxoffice_daily_current` 뷰, `publication_generation_v3` 시퀀스와 빈 스키마를 제거했다.
백업 `.local/qa/pop-talk-before-direct-dev.dump`는 dev와 이전 dw_serving을 포함하며 pg_restore 목록 확인을 완료했다. 삭제는 트랜잭션과 RESTRICT로 실행했다. 현재 dev는 테이블 16개·뷰 2개다.

구현: `orchestration/airflow/modules/pipelines/orchestration/service_sync.py`.
서비스 마이그레이션: `apps/was/migrations/016_dw_direct_service.up.sql`.
이전 날짜의 서비스 DB 감사 문서는 당시 구조를 기록한 이력이다. 현재 구조는 이 문서를 기준으로 읽는다.
