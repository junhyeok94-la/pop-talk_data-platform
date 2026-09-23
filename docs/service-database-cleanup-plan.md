# 서비스 DB 정리 — 실행 승인 대상

대상 DB: `pop_talk_local`. 사용자 승인 후 아래 테이블 30개와 합성 V3 테스트 게시본 6개를 삭제했다.
현재 테이블은 dev 15개 + dw_serving 6개이며, 뷰는 각 2개씩 유지한다.
서비스 테이블 건수, 활성 게시본, current 영화·흥행 내용 해시 보존 및 실제 흥행 API 응답을 확인했다.
추가 사용자 승인 후 `fence_token_seq`, `publisher_claim_token_seq`, `work_claim_token_seq`와
`dw_control` 스키마도 RESTRICT로 삭제했다. 해당 스키마가 없어졌음을 확인했다.

## 보존

- `dev` 전체: 테이블 15개와 뷰 2개, 회원·영화·리뷰·편집·미디어·임베딩 데이터.
- `dw_serving` V3 테이블 6개와 current 뷰 2개.
- 활성 `movie_gold` 게시본 및 실제 과거 게시 이력.

## 삭제할 테이블 30개

`dw_control` 17개:

```text
asset_deliveries
asset_inbox
asset_outbox
dbt_invocation_artifacts
dbt_invocation_registries
dbt_invocation_reservations
dbt_model_invocation_specs
generation_gate_receipts
generation_plans
model_build_attempts
model_build_slots
model_cohorts
model_execution_receipts
model_results
release_delivery_registries
release_delivery_specs
schema_migrations
```

`dw_serving` 구버전 13개:

```text
movie_catalog
movie_catalog_staging
publish_runs
movie_catalog_snapshot
boxoffice_daily_snapshot
dataset_publications
active_publications
publish_attempts
movie_catalog_snapshot_v2
boxoffice_daily_snapshot_v2
dataset_publications_v2
active_publications_v2
publish_attempts_v2
```

첫 시도는 독립 시퀀스 의존성 때문에 전부 롤백됐다. 테이블 삭제를 먼저 완료한 뒤,
별도 사용자 승인을 받아 시퀀스 3개와 스키마 삭제까지 완료했다.
V3에는 이전 smoke 테스트에서 만든 `v3-` 접두어의 합성 publication과
`v3-stale`, `v3-stale-first-failure` 포인터 및 해당 합성 publication의 스냅샷·시도만 정리한다.
실제 서비스의 해시 기반 publication과 활성 `movie_gold`는 보존한다.

## 백업 및 실행 방식

- 2026-09-11 12:57 KST, 두 대상 스키마의 구조·데이터를 pg_dump custom archive로 백업했다.
- 백업: `.local/qa/pop-talk-before-essential-cleanup.dump` (5,667,535 bytes).
- `pg_restore --list`로 아카이브 목차 226개 항목을 읽는 데 성공했다. 실제 복원 실행 검증은 아직 하지 않았다.
- 복구 시 먼저 별도 DB에 아카이브를 복원해 검증한 뒤 삭제 객체만 선택 복원한다. 운영 중인 V3 전체를 과거 백업으로 덮어쓰지 않는다.
- 삭제는 단일 트랜잭션에서 `DROP ... RESTRICT`로 수행한다. 예상 외 의존성이 있으면 롤백한다.
- 활성 게시와 충돌하지 않도록 게시와 동일한 advisory lock을 사용한다.
- 삭제 전후 dev 테이블별 건수와 current 영화·흥행의 내용 해시/활성 포인터가 같아야 commit한다.
- `compose.yaml`의 dw_control 자동 마이그레이션 호출은 제거했다.

정리 완료 시 테이블은 51개에서 21개, 뷰는 4개 유지된다.
