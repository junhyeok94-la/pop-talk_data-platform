# Phase 1 테이블 분류와 개발 대상 — 2026-09-23

## 결론

최초 조사 시 분석용 STG/DW/Mart가 없었고 구형 `dw_control` 테이블 17개가 남아 있었습니다.
이후 분류 기준에 따라 **구형 17개를 백업 후 삭제하고 분석 객체를 생성했습니다.**
중복 실행로그 `ops.pipeline_runs`는 후속 검토에서 제거해 현재 분석용 테이블 10개와 마트 뷰 3개입니다.
Airflow 시스템 71개와 **Workbench 13개 전체를 보존**합니다.
Workbench 모델 실험 4개는 대시보드와 공유 참조가 있어 이번 삭제 범위에서 제외했습니다.

아래 실측 행 수는 삭제 전 조사 기록입니다. 구현·검증·운영 연결 상태는 [실행 안내](phase1-pipeline.md)를 참고합니다.

## 확인 범위와 근거

| 범위 | 확인 방법 | 결과 |
|---|---|---|
| 플랫폼 `pop_talk_platform` | 실제 카탈로그·정확한 COUNT·FK·뷰·함수 조회 | `dw_control` 테이블 17개, 시퀀스 3개 |
| 운영 메타데이터 `airflow` | 실제 카탈로그·정확한 COUNT | `public` 71개, `workbench` 13개 |
| 서비스 `pop_talk_local` | 현재 DB 중지. 애플리케이션 소스·마이그레이션과 과거 카탈로그 비교 | 현재 테이블 수·행 수·마이그레이션 적용 여부는 미확인 |

플랫폼에서 직접 확인한 테이블은 총 101개입니다. `postgres` 기본 관리 DB와 서비스 DB는 이 합계에 포함하지 않습니다.
행 수는 READ ONLY / REPEATABLE READ 트랜잭션에서 조회했습니다. 사용자 행 내용·비밀번호·토큰은 수집하지 않았습니다.
테이블별 전체 목록은 [실측 목록](phase1-table-inventory-2026-09-23.md)에 있습니다.

## 1. 삭제 완료: 플랫폼의 과거 실행 제어 계층

`pop_talk_platform.dw_control` 전체 17개:

| 테이블 | 실측 행 수 | 과거 역할 |
|---|---:|---|
| asset_deliveries | 0 | 모델 실행 이벤트 전달 |
| asset_inbox | 0 | 모델 실행 이벤트 수신 |
| asset_outbox | 0 | 모델 실행 이벤트 발행 |
| dbt_invocation_artifacts | 0 | 전용 dbt 실행 산출물 |
| dbt_invocation_registries | 0 | 전용 dbt 실행 등록 |
| dbt_invocation_reservations | 0 | 전용 dbt 실행 예약 |
| dbt_model_invocation_specs | 0 | 모델별 실행 명세 |
| generation_gate_receipts | 0 | 모델 generation 품질 게이트 |
| generation_plans | 0 | 모델 generation 계획 |
| model_build_attempts | 0 | 모델 빌드 시도 |
| model_build_slots | 0 | 모델 빌드 슬롯 |
| model_cohorts | 0 | 모델 실행 입력 묶음 |
| model_execution_receipts | 0 | 모델 실행 증거 |
| model_results | 0 | 모델 실행 결과 |
| release_delivery_registries | 0 | 배포별 전달 계약 |
| release_delivery_specs | 0 | 배포별 전달 명세 |
| schema_migrations | 7 | 삭제한 제어 계층의 DDL 적용 이력 |

함께 정리할 시퀀스는 `fence_token_seq`, `publisher_claim_token_seq`, `work_claim_token_seq`입니다.
모든 해당 객체 소유자는 `dw_control_owner`입니다. FK 23개는 이 스키마 내부 참조이며,
다른 스키마에서 참조하는 FK·뷰는 조회 결과 0개였습니다. 스키마 내 함수도 0개입니다.
조회 시 플랫폼 DB 접속은 조사용 관리자 연결 1개뿐이었습니다.
외부 사용자 SQL이나 저장소 밖 소비자는 이 조사로 완전히 배제할 수 없으므로 실제 삭제 직전에 다시 확인합니다.

생성·사용 코드는 이전 정리에서 제거됐고 현재 초기화는 이 스키마를 재생성하지 않습니다.
Airflow의 `public.asset*` 테이블과는 다른 객체입니다. 이름이 비슷하다고 함께 삭제하지 않습니다.

실제 정리는 `retire_dw_control.sql`로 수행했습니다. 스키마 정의와 적용 기록을 백업하고,
참조하는 테이블과 참조되는 테이블을 같은 DROP TABLE 문에 명시해 내부 순환 FK를 처리하되
RESTRICT로 예상 밖 외부 의존성은 차단합니다. 이후 시퀀스와 빈 스키마를 정리합니다.
역할 삭제는 다른 DB의 소유권·권한까지 별도 확인한 뒤 판단합니다.

## 2. 보존: Airflow 시스템 테이블 71개

`airflow.public` 전체는 Airflow/FAB가 관리합니다.
DAG·Task·Asset·Connection·로그·인증·권한·마이그레이션 테이블이며 분석 업무 테이블이 아닙니다.
행 수가 0이거나 현재 쓰지 않는 기능의 테이블이어도 개별 DROP 대상에 넣지 않습니다.
필요한 로그 정리는 Airflow가 제공하는 보존 정책·관리 기능으로 수행합니다.

대표 객체: `dag`, `dag_run`, `task_instance`, `serialized_dag`, `asset`, `asset_event`,
`connection`, `variable`, `slot_pool`, `ab_user`, `ab_role`, `alembic_version`.
분석용 테이블을 이 DB에 추가하지 않습니다.

## 3. Workbench: 공용 보존과 모델 실험 삭제 후보를 분리

| 테이블 | 실측 행 수 | 분류 | 이유/선행 조건 |
|---|---:|---|---|
| dashboard_actions | 0 | 보존 | 대시보드 작업 상태 |
| dashboard_cache | 1 | 보존 | 대시보드 조회 캐시 |
| documents | 0 | 보존 | 대시보드·작업 화면과 모델 기능이 공유하는 문서 저장소 |
| leases | 1 | 보존 | 대시보드 갱신 예산·동시 실행 제어에도 사용 |
| studio_boards | 0 | 보존 | Studio 보드 |
| studio_preferences | 0 | 보존 | Studio 사용자 설정 |
| studio_revisions | 0 | 보존 | 보드 버전 이력 |
| studio_templates | 0 | 보존 | 보드 템플릿 |
| schema_migrations | 5 | 보존 | Workbench 초기화 이력 |
| lab_records | 0 | 조건부 삭제 | Model Lab UI/API·store 참조 제거 필요 |
| experiments | 0 | 조건부 삭제 | Model Lab뿐 아니라 Studio 실험 데이터셋도 참조 |
| executor_jobs | 0 | 조건부 삭제 | 모델 executor API·상태 조회 연결 제거 필요 |
| executor_reservations | 0 | 조건부 삭제 | 모델 executor 예약 로직 제거 필요 |

모델 DAG 삭제와 Model Lab 플러그인 제거는 별개입니다.
현재 `shared/metadata.py`의 `TABLES`와 `migrate()`가 13개 테이블을 모두 생성하고,
초기화가 이를 호출합니다. 모델 실험 테이블만 먼저 DROP하면 화면 오류 또는 재생성이 발생합니다.

따라서 Model Lab 화면·API·대시보드 실험 조회를 제거하거나 비활성화하고,
공유 저장소와 migration을 분리한 뒤 위 4개를 삭제합니다.
`documents`, `leases`를 모델 전용으로 간주해 함께 삭제하지 않습니다.
Phase 1에서 Workbench 기능을 확장하는 개발은 하지 않습니다.

## 4. 서비스 DB: 보존하며 연결하는 대상

애플리케이션 저장소의 WAS 소스와 `apps/was/migrations`를 확인했습니다.
서비스 DB는 중지돼 있고 분리된 애플리케이션 저장소의 `.env`도 아직 없어,
아래는 **소스·과거 카탈로그 기반 분류**입니다. 현재 DB의 실측 결과가 아닙니다.

| 객체 | Phase 1 역할 | 분류 |
|---|---|---|
| dev.popcorn_movies | 기존 서비스 영화 ID와 KOFIC 코드 매핑, 검증된 원천 속성 게시 | 보존·게시 대상 |
| dev.reviews | 내부/외부 리뷰와 수정·삭제 상태 추출 | 보존·읽기 원천 |
| dev.movie_boxoffice_daily | 영화·날짜별 검증된 흥행 게시. migration 016 적용 확인 필요 | 보존·게시 대상 |
| dev.batch_runs | 서비스 게시 트랜잭션의 성공·실패·입력 이력 | 보존·게시 이력 |
| dev.movie_editorial, dev.popcorn_movie_media | 관리자 보정·미디어 | 보존, 분석 배치가 덮어쓰지 않음 |
| dev.movie_categories, dev.movie_category_links, dev.display_categories | 서비스 카테고리·화면 구성 | 보존 |
| dev.users, dev.user_refresh_tokens, dev.admin_audit_logs | 회원·인증·감사 | 보존, Phase 1 분석 복제 제외 |
| dev.chat_sessions, dev.chat_messages | 서비스 대화 | 보존, Phase 1 제외 |
| dev.popcorn_movie_embeddings, dev.movie_embedding_jobs | 서비스 검색용 임베딩·작업 | 보존, 모델 학습 DAG 제거와 무관 |
| dev.popcorn_movies_service, dev.display_categories_service | 서비스 공개 뷰 | 보존 |
| dev.movie_display_category_links | 일부 migration에 존재하지만 과거 카탈로그 목록과 불일치 | 현재 존재·소비자 확인 전 보존 |

내부 리뷰와 외부 리뷰는 별도 `external_reviews` 테이블이 아니라 현재 소스상
`reviews.source_system/source_user_key/source_review_key`로 구분됩니다.
기존 리뷰 UUID와 영화 BIGINT ID를 유지합니다. 외부 작성자를 내부 회원으로 결합하지 않습니다.

과거 `dw_serving`의 V3 테이블 6개와 current 뷰 2개는 **추가 확인 후 삭제 후보**입니다.
확인할 테이블: `active_publications_v3`, `dataset_publications_v3`, `gold_build_registry_v3`,
`movie_snapshot_v3`, `boxoffice_snapshot_v3`, `publish_attempts_v3`.
뷰: `movie_catalog_current`, `boxoffice_daily_current`.
현재 WAS는 `movie_boxoffice_daily`를 직접 조회하며 검색한 WAS/프런트/챗봇 소스에서
`dw_serving` 참조는 찾지 못했습니다. 하지만 과거 보고서에는 보존 대상으로 기록돼 있으므로
서비스 DB를 실제 조회해 migration 적용·뷰 의존성·외부 소비자·복구 필요성을 확인하기 전에는 삭제하지 않습니다.
이 플랫폼 저장소에서 서비스 DDL을 복제하거나 임의로 변경하지 않습니다.

## 5. 구현한 분석 객체

대상 DB는 `pop_talk_platform`입니다. 스키마는 `ops`, `stg`, `dw`, `mart`로 분리합니다.
물리 테이블 10개와 dbt 마트 뷰 3개입니다. 마트는 우선 뷰로 시작하며 성능 근거가 생기면 물리화합니다.

| 순서 | 객체 | 행의 기준 / 핵심 키 | 목적 |
|---|---|---|---|
| A | ops.source_loads | 원본 manifest와 loader 버전별 논리 적재 / load_id + 입력 해시·버전 unique | 동일 원본 재시도 중복 방지, 원본 S3 위치·검증 결과 |
| A | stg.movie_observations | 적재·원천·영화·원본 객체별 관측 | KOFIC/KMDb 원본 참조·수집 시각·정제 값·hash |
| A | stg.boxoffice_observations | 적재·KOFIC 코드·대상 날짜별 관측 | 일별 순위·관객·누적 관객·매출·관측 시각 |
| B | dw.dim_movie | 영화 1건 / movie_key, KOFIC 코드 unique | 공통 영화 식별. 첫 버전은 현재 속성 기준 |
| B | dw.dim_date | 날짜 1건 / calendar_date | 달력 차원 |
| B | dw.fct_boxoffice_daily | 영화·대상 날짜 1건 / movie_key + target_date | 품질 검증된 최신 일별 흥행 |
| B | mart.movie_boxoffice_daily | 영화·대상 날짜 1건 | 영화 속성과 흥행 지표 |
| C | dw.movie_service_map | 리뷰가 있는 서비스 시스템·영화 ID 1건 | 최신 리뷰 스냅샷의 영화 매핑, dbt 조인·not_null 검사 |
| C | stg.review_observations | 적재·리뷰 출처·서비스 리뷰 ID별 관측 | 원본 출처 키·작성/수정/삭제 시각·평점·상태 |
| C | dw.fct_review | 출처·리뷰 ID 1건의 현재 상태 | 영화 연결·평점·삭제/노출 상태. source_review_key도 보존 |
| C | mart.movie_review_summary | 영화·리뷰 출처 1건 | 유효 리뷰 수·평균 평점·집계 기준 시각 |
| C | mart.movie_performance | 영화·리뷰 출처 1건의 현재 요약 | 흥행 요약과 출처별 리뷰 지표. 서로 다른 기준 시각 표시 |
| D | ops.service_publications | 서비스 게시 요청 1건 / publication_id | 게시 입력 해시·cutoff·대상·상태·재확인 결과 |

`ops.source_loads` 등록과 STG 데이터 적재 완료는 같은 플랫폼 DB 트랜잭션으로 처리합니다.
실행·실패 이력은 Airflow에 기록하며 별도의 중복 실행로그 테이블을 운영하지 않습니다.
동일 입력·버전 재실행은 STG를 중복 생성하지 않고, 버전 변경 재처리도 DW 업무 키를 중복시키지 않습니다.

`ops.service_publications`와 서비스의 `dev.batch_runs`는 역할이 다릅니다.
전자는 플랫폼의 게시 요청, 후자는 서비스 DB 트랜잭션의 결과입니다.
서로 다른 DB를 하나의 트랜잭션이라고 간주하지 않습니다. 응답 유실 시 같은 게시 식별자·hash로
서비스 성공 이력을 재조회해 확정하고, 이미 성공한 게시를 중복 반영하지 않습니다.

## 6. 지표·모델 기준

- 흥행 팩트는 영화·날짜당 1건입니다. 최근 7일 관측이 매일 겹쳐도 중복 집계하지 않습니다.
- 상위 박스오피스 목록에 없는 날을 관객 0으로 채우지 않습니다. 누적 관객수는 날짜별 합산하지 않습니다.
- 영화 차원에 없는 흥행 원천은 STG에 보존하고 미매핑 상태를 드러냅니다. 조인으로 조용히 버리지 않습니다.
- 리뷰의 `created_at`이 외부 원래 작성일인지 수입 시각인지 먼저 확인합니다. 미상은 NULL/unknown으로 표시합니다.
- 내부·외부 리뷰를 출처별로 집계하고 평점 척도를 확인합니다. 회원·외부 작성자의 ID 공간을 합치지 않습니다.
- 삭제/비공개 리뷰는 상태를 보존하되 공개용 집계에서 제외합니다. 리뷰 수정·삭제 시 기존 집계가 갱신돼야 합니다.
- `mart.movie_performance`의 흥행 값은 리뷰 출처별로 반복될 수 있으므로 출처 행 전체를 합산하지 않습니다.
- 원본 API 관측 시각, 흥행 대상일, 리뷰 작성일, 수집 시각, 게시 시각을 구분합니다.
- 비밀번호·인증 토큰·회원 전체 행·대화 내용은 이번 분석 테이블에 복제하지 않습니다.

## 7. 개발 순서와 완료 기준

1. 위 실측 목록을 기준으로 `dw_control` 정리 마이그레이션을 준비하고 별도 실행 단계에서 적용합니다.
2. **먼저 A의 테이블 3개를 구현**합니다. 기존 원본 한 묶음을 적재하고 동일 입력 재실행·실패 롤백을 검증합니다.
3. B의 차원·흥행 팩트·첫 마트를 만들고 영화/날짜 키 중복·backfill·관측 시각 역전을 검사합니다.
4. 서비스 DB 실측 후 C의 영화 매핑·리뷰 추출·두 마트를 구현합니다. 업데이트·삭제 전파도 검증합니다.
5. D의 게시 요청과 서비스 게시 계약을 연결합니다. 품질 실패 시 게시 차단과 응답 유실 복구를 검증합니다.
6. Workbench 모델 실험 테이블 4개 정리는 기능 분리와 묶어 별도 수행합니다. 분석 첫 적재를 막는 선행 조건은 아닙니다.

DDL은 `modules/pipelines/platform/schema.sql`, DW/Mart는 `transformation/dbt`에서 관리합니다.
서비스 DB가 중지된 상태라 실제 서비스 카탈로그 검증은 남아 있습니다. 리뷰·게시 코드는 서비스 소스 계약으로
구현했고 격리 DB에서 검증했습니다. 운영 리뷰 추출·서비스 게시는 아직 실행하지 않았습니다.
