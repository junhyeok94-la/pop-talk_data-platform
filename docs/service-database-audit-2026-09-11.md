# PostgreSQL 서비스 데이터베이스 전수 점검

확인 시점: 2026-09-11 12:46 KST. 대상은 실제 로컬 `pop_talk_local`이다.
읽기 전용 트랜잭션으로 사용자 스키마 전체의 테이블·뷰·컬럼·키·인덱스·트리거·함수·뷰 의존성과 테이블 건수를 조사하고 현재 애플리케이션 및 DAG 코드를 대조했다.
개인정보 행은 추출하지 않았다. DB 데이터·스키마·운영 코드는 변경하지 않았다.
미사용 판단은 현재 저장소와 실행 진입점 기준이다. 외부 프로그램의 접근까지 입증한 삭제 승인은 아니다.

## 핵심 판단

**서비스의 원장은 여전히 `dev`다. `dw_serving`은 DW 결과를 전달하는 별도 영역이며 `dev`를 대체하지 않았다.**
실행 중인 WAS 설정도 `database=pop_talk_local`, `DATABASE_SCHEMA=dev`로 확인했다.

| 스키마 | 테이블 | 일반 뷰 | 역할 |
|---|---:|---:|---|
| `dev` | 15 | 2 | 회원·영화·리뷰·채팅·관리자·추천의 실제 서비스 데이터 |
| `dw_serving` | 19 | 2 | DW 게시 결과. 현재 V3와 과거 버전이 공존 |
| `dw_control` | 17 | 0 | 삭제한 모델별 분산 DAG의 제어·실행 기록 |
| `public`, `cdb_admin` | 0 | 0 | 공용/확장 객체. `cdb_admin`에는 vector 관련 함수·타입 존재 |
| 합계 | **51** | **4** | 실제 사용자 테이블·뷰 55개. 시스템 카탈로그 제외 |

현재 `prd` 스키마는 없다. 배치 실행 상태를 저장하는 Airflow 메타 DB는 별도 `airflow` 데이터베이스이며 이번 테이블 수에 포함하지 않는다.

## 화면에서 실제로 읽는 경로

| 기능 | 실제 읽기·쓰기 대상 | 코드 근거 |
|---|---|---|
| FE 영화 목록/상세 | WAS → `dev.popcorn_movies` + `movie_editorial`, 카테고리 | `apps/was/app/public_catalog.py` |
| FE 영화 흥행 카드 | `dev.popcorn_movies`에서 KOFIC 코드 → `dw_serving.boxoffice_daily_current` | 같은 파일의 `/catalog/movies/{movie_id}/boxoffice` |
| 관리자 영화 조회 | `dev.popcorn_movies_service` | `apps/admin/src/lib/movies-source.ts`, `movies-query.ts`, `apps/was/app/catalog.py` |
| 관리자 영화 수정/검수 | `dev.popcorn_movies`, 별도 편집/감사 테이블 | `apps/admin/src/app/admin-api/movies`, `apps/was/app/admin.py` |
| 챗봇 영화/추천 | `dev.popcorn_movies_service`, `popcorn_movie_embeddings`, `reviews` | `apps/chatbot/app/repositories/movie_repository.py` |
| 챗봇 대화 | `dev.chat_sessions`, `chat_messages`, `users` | `apps/chatbot/app/repositories/chat_repository.py` |
| 관리자 배치 이력 | `dev.batch_runs` | `apps/admin/src/lib/batch-runs.ts` |
| 현재 DW 게시 | `dw_serving`의 V3 테이블 6개 및 current 뷰 2개 | `orchestration/airflow/modules/pipelines/orchestration/dw_pipeline.py`, `pipelines/dbt/scripts/publish_to_postgres_v3.py` |

`dev.popcorn_movies_service`와 `dw_serving.movie_catalog_current`는 같은 데이터의 별칭이 아니다.
전자는 서비스 영화 ID와 미디어·임베딩 상태를 붙인 뷰이고, 후자는 DW 영화 키와 JSON payload를 담은 현재 게시 결과다.
현재 영화 카탈로그 API는 후자를 읽지 않는다. 두 영역 사이에 자동 동기화 트리거도 없다.

## dev 전체 목록 — 유지할 서비스 객체

0건은 미사용의 증거가 아니다. 관리자 편집·카테고리 수동 연결 등 아직 사용 이력이 없어도 코드에서 사용하는 기능이다.

| 테이블/뷰 | 현재 건수 | 목적과 사용처 |
|---|---:|---|
| `popcorn_movies` | 5,319 | 영화 원장. 서비스 PK `id`, 외부 연결 키 `kofic_movie_cd`, 메타데이터·승인·공개 상태 보관 |
| `popcorn_movies_service` (뷰) | 5,319 | 영화 원장 + `media` JSON + `is_embedded`. 관리자와 챗봇 조회. 자체 공개/승인 필터 없음 |
| `popcorn_movie_media` | 52,082 | 영화별 포스터·스틸컷. 영화 원장의 단일 `poster_url`과 역할이 다름 |
| `popcorn_movie_embeddings` | 5,310 | 실제 추천/검색 벡터와 문서·모델별 상태. READY 5,308 / STALE 2 |
| `movie_embedding_jobs` | 5,338 | 임베딩 작업 큐/이력. 결과 벡터 테이블과 다름. 현재 모두 SUCCEEDED |
| `movie_editorial` | 0 | 관리자 줄거리 보정·삭제 표시 등. 원천 재수집 때 보존해야 할 별도 운영 데이터 |
| `movie_categories` | 14 | 서비스 자체 분류 사전. 검색 aliases, 관리자 자동 분류 match_keywords를 구분 |
| `movie_category_links` | 0 | 영화와 분류의 수동 다대다 연결. 0건이어도 자동 분류는 별도로 동작 |
| `display_categories` | 16 | 홈의 자연어 추천 알약 문구, 연결된 category_codes, 노출 순서 |
| `display_categories_service` (뷰) | 원장 기준 16개 | 알약별 카테고리명·잘못된 코드·영화 합집합 건수를 계산. 집계 조건은 화면 공개 조건과 별도 |
| `users` | 319 | 회원·권한·온보딩 상태와 취향 카테고리 |
| `user_refresh_tokens` | 110 | 로그인 갱신 토큰과 교체 관계. 배치 대상 아님 |
| `reviews` | 124,944 | 사용자/외부 리뷰, 평점·게시 상태. 영화 ID 보존이 필수 |
| `chat_sessions` | 328 | 회원/비회원 대화 세션 |
| `chat_messages` | 666 | 세션별 메시지·의도·출처 |
| `admin_audit_logs` | 0 | 관리자 작업 감사 기록. API 경로별 감사 기록 적용 범위는 별도 점검 대상 |
| `batch_runs` | 9 | 기존 초기 적재 실행 이력. 현재 Airflow 배치 로그의 자동 복제본이 아님 |

현재 `onboarding_profiles`, `movie_display_category_links` 테이블은 없다. 예전 마이그레이션·주석에 남은 명칭과 실제 객체를 구분해야 한다.
`movie_categories`의 DB 주석에는 아직 영화를 연결하지 않는다는 설명이 있지만, 현재는 수동 연결 테이블과 키워드 기반 자동 분류가 있으므로 주석이 오래됐다.

## dw_serving 전체 목록

스냅샷 테이블 건수는 여러 게시본의 누적 건수다. 화면에 제공할 현재 건수는 반드시 `*_current` 뷰로 확인해야 한다.

| 객체 | 현재 건수 | 역할 / 판정 |
|---|---:|---|
| `movie_snapshot_v3` | 7,071 | 현재 사용. 게시본별 영화 JSON 스냅샷 |
| `boxoffice_snapshot_v3` | 740 | 현재 사용. 게시본별 일별 흥행 JSON 스냅샷 |
| `dataset_publications_v3` | 10 | 현재 사용. 게시 완료한 데이터셋의 건수·해시·버전 |
| `active_publications_v3` | 3 | 현재 사용. 데이터셋별 활성 게시본 포인터. 실제 서비스는 `movie_gold` 1개 |
| `gold_build_registry_v3` | 12 | 현재 사용. 게시 식별자·내용·순서 등록, 충돌 및 역전 방지 |
| `publish_attempts_v3` | 19 | 현재 사용. 게시 시도별 성공·실패 이력 |
| `movie_catalog_current` (뷰) | 6,075 | 현재 `movie_gold` 영화 결과. dev로 동기화할 입력 후보 |
| `boxoffice_daily_current` (뷰) | 80 | 현재 `movie_gold` 흥행 결과. WAS가 실제 조회 |
| `movie_catalog` | 100 | 초기 게시 구현 잔여. 현재 DAG/서비스 사용 근거 없음 |
| `movie_catalog_staging` | 100 | 초기 게시용 임시 적재 잔여 |
| `publish_runs` | 1 | 초기 게시 구현 이력 |
| `movie_catalog_snapshot` | 107 | V1 영화 스냅샷 잔여 |
| `boxoffice_daily_snapshot` | 70 | V1 흥행 스냅샷 잔여 |
| `dataset_publications` | 1 | V1 게시 목록 잔여 |
| `active_publications` | 1 | V1 활성 포인터 잔여 |
| `publish_attempts` | 3 | V1 게시 시도 잔여 |
| `movie_catalog_snapshot_v2` | 321 | V2 영화 스냅샷 잔여 |
| `boxoffice_daily_snapshot_v2` | 210 | V2 흥행 스냅샷 잔여 |
| `dataset_publications_v2` | 3 | V2 게시 목록 잔여 |
| `active_publications_v2` | 2 | V2 활성 포인터 잔여 |
| `publish_attempts_v2` | 5 | V2 게시 시도 잔여 |

과거 버전 13개 테이블은 정리 후보다. V3 테이블도 테스트 데이터와 운영 데이터가 섞여 있다.
활성 포인터의 `v3-stale`, `v3-stale-first-failure`는 과거 실패/역전 검사 데이터다. current 뷰는 `movie_gold`로 필터링하므로 지금 화면에는 섞이지 않지만, 삭제 시 연관 게시/시도/스냅샷을 함께 확인해야 한다.

## dw_control 전체 목록 — 분산 모델 DAG의 잔여

현재 단일 DW DAG는 이 테이블들을 실행 상태 저장에 사용하지 않는다. Airflow 태스크 상태와 XCom으로 제어한다.
다만 `compose.yaml`의 airflow-init에는 아직 `migrate_dw_control.py` 실행이 남아 있다. DB만 삭제하면 재초기화 과정에서 다시 생길 수 있으므로 코드·시작 명령·마이그레이션을 같이 정리해야 한다.

| 테이블 | 건수 | 과거 목적 |
|---|---:|---|
| `asset_outbox` | 14 | 발행할 모델 완료/준비 이벤트 |
| `asset_deliveries` | 21 | 이벤트별 수신자 전달 상태 |
| `asset_inbox` | 21 | 수신 이벤트 처리/중복 방지 |
| `generation_plans` | 1 | 모델 세대별 계산 계획 |
| `generation_gate_receipts` | 4 | 원천·배포 검사 증빙 |
| `model_build_slots` | 7 | 모델별 실행/재사용 배정 |
| `model_build_attempts` | 7 | 모델 계산 시도·점유 |
| `model_cohorts` | 7 | 모델이 사용할 부모 결과 묶음 |
| `model_execution_receipts` | 7 | dbt 실행 증빙 |
| `model_results` | 7 | 확정된 모델 결과 |
| `dbt_invocation_registries` | 1 | 배포별 실행 허용 목록 |
| `dbt_model_invocation_specs` | 7 | 모델별 명령·검사 명세 |
| `dbt_invocation_reservations` | 14 | 명령 실행 예약 |
| `dbt_invocation_artifacts` | 14 | dbt 결과 파일 등록 |
| `release_delivery_registries` | 1 | 배포별 전달 정책 |
| `release_delivery_specs` | 7 | 모델별 이벤트 수신자 |
| `schema_migrations` | 7 | 위 제어 스키마의 마이그레이션 이력 |

서비스 FK나 current 뷰는 이 제어 테이블들을 참조하지 않는다. 현재 실행 경로 기준 정리 후보 17개이며, 과거 실행 재현용 보존 여부는 별도다.

## 트리거가 실제로 하는 일

`dev`에서 확인한 사용자 함수는 5개이고 별도 사용자 프로시저는 없다. 데이터 흐름은 주로 트리거 함수로 구현되어 있다.

| 함수 | 연결 | 실제 동작 |
|---|---|---|
| `set_updated_at` | 영화·임베딩·카테고리·알약의 UPDATE | 수정 시각 갱신 |
| `queue_movie_embedding` | 영화 INSERT / 지정 메타데이터·승인·공개 컬럼 UPDATE | 기존 벡터를 STALE로 만들고 공개·승인된 영화는 UPSERT 큐 등록 |
| `queue_curated_movie_embedding` | editorial / category_links INSERT·UPDATE·DELETE | 운영 보정 변경 시 벡터 무효화/큐 등록 |
| `protect_removed_movie` | 영화 UPDATE | 관리자 삭제된 영화가 재동기화로 되살아나지 않도록 HIDDEN/REJECTED 유지 |
| `duplicated_category_aliases` | 직접 조회하는 점검 함수 | 카테고리 alias 중복 조회 |

임베딩 큐를 넣는 것과 실제 임베딩을 생성하는 것은 다르다. 실제 생성은 워커가 해야 한다.
이번 집계에서는 큐 5,338건이 모두 성공 상태이며 마지막 생성 이력은 2026-08-14 UTC다. 워커가 현재 기동/소비 중인지까지 이 수치로 단정할 수 없다.

**연결 시 주의:** `queue_movie_embedding`, `protect_removed_movie`는 함수 자체 search_path 설정이 없고 테이블명을 스키마 없이 사용한다.
현재 WAS는 `dev, public`, admin은 `dev`로 연결하여 동작하지만, 새 배치 연결의 기본 search_path로 `dev.popcorn_movies`만 명시해 쓰면 트리거 안에서는 다른 경로를 볼 수 있다.
동기화 구현 전에 함수의 스키마 고정 또는 배치 세션의 명시적 search_path 설정이 필요하다.

## 동기화 전 확인된 중요한 차이

### 1. 두 영화 목록은 일치하지 않는다

KOFIC 코드로 대조한 결과:

| 범위 | 건수 |
|---|---:|
| dev와 DW 공통 | 5,312 |
| DW에만 존재 | 763 |
| dev에만 존재 | 7 |
| DW 단독 중 정책 적격 | 33 |
| DW 단독 중 정책 제외 | 730 |
| DW 전체 정책 적격 / 제외 | 4,356 / 1,719 |

따라서 `dev`를 지우고 DW 6,075건을 그대로 넣으면 안 된다. 서비스 전용 7건과 기존 ID를 보존하고, 신규 후보와 정책 제외 데이터를 구분해야 한다.
정책 적격은 관리자 승인과 같은 개념이 아니다. 기존 서비스 영화의 정책 판단이 달라졌다고 일괄 승인 취소해서도 안 된다.

### 2. 공개 API와 추천의 노출 조건이 다르다

공개 `/catalog/movies`와 상세는 현재 `movie_editorial.is_removed`만 제외한다. `service_status='PUBLISHED'`, `approval_status='APPROVED'`를 조건에 넣지 않는다.
현재 원장에는 DRAFT/PENDING 10건, PUBLISHED/REJECTED 1건이 있다. editorial은 0건이므로 이 상태들도 공개 API의 조회 대상이다.
챗봇의 일부 조회는 PUBLISHED만 확인한다. `popcorn_movies_service` 뷰 자체도 승인/공개/운영 삭제를 필터링하지 않는다.
신규 영화를 PENDING으로 넣으면 안 보일 것이라고 가정할 수 없다. 공개·승인·삭제 정책을 먼저 코드에서 통일해야 한다.

### 3. 서비스 ID와 운영 데이터를 반드시 보존해야 한다

`popcorn_movie_media`, `popcorn_movie_embeddings`, `movie_embedding_jobs`, `movie_editorial`, `movie_category_links`, `reviews`가 `dev.popcorn_movies.id`를 FK로 참조한다.
대부분 ON DELETE CASCADE다. 영화 전체 교체는 124,944건의 리뷰 등 관련 데이터 삭제로 이어질 수 있다.
DW의 `movie_key`는 서비스 bigint ID가 아니다. KOFIC 코드로 기존 행을 매칭하고 원천 소유 필드만 갱신해야 한다.

### 4. 관리자 배치 화면에는 현재 Airflow 실행이 표시되지 않는다

`dev.batch_runs`의 9건은 모두 `load-initial-movies`의 과거 기록이며 마지막 시작 시각은 2026-08-16 UTC다.
현재 DAG는 해당 테이블에 기록하지 않고 Airflow와 `dw_serving.publish_attempts_v3`에 기록한다.
관리자 화면이 멈춘 것처럼 보일 수 있으므로 후속 연결에서 기록의 기준을 정해야 한다.

### 5. 컬럼 모양도 그대로 복사할 수 없다

DW 결과는 대문자 키의 JSON payload이고 배열 필드도 서비스의 PostgreSQL 배열과 변환이 필요하다.
`dev.popcorn_movies`는 제목·개봉일 등 필수 제약과 KOFIC unique 제약을 갖는다. DW 정책 제외·필수값 누락을 별도로 처리해야 한다.
원천 수정으로 제목/배우/장르 등을 갱신하면 임베딩 트리거가 실행된다. 실제 값이 달라진 행만 UPDATE하고 임베딩 워커의 소비 상태를 확인하는 것이 효율적이다.

## 권장하는 최종 역할 구분

1. **`dev` 유지:** 서비스 원장과 운영 데이터, 기존 두 뷰를 유지한다. 새로운 서비스 원장을 추가하지 않는다.
2. **`dw_serving`은 전달 역할:** 현재 V3/current만 사용하고 과거 13개 테이블·테스트 데이터를 정리한다.
3. **`dw_control` 정리:** 사용하지 않는 초기화 명령과 코드 의존성을 제거한 뒤 과거 제어 테이블 17개를 정리한다.
4. **그 다음 동기화:** 공개 조건·필드 소유권·신규 승인 기준을 정한 후 `movie_catalog_current → dev.popcorn_movies`의 변경분 반영을 구현한다.

이번 작업은 조사만 수행했다. 테이블 삭제나 동기화 구현은 하지 않았다.
전체 컬럼·제약·인덱스·뷰 정의·트리거 정의는 함께 작성한 `service-database-catalog-2026-09-11.md`에서 확인할 수 있다.
