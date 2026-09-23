# 데이터 모델링 전수검사 초안 독립 검토

판정: CHANGES REQUESTED — 주요 구현 진단과 핵심 수치는 타당하다. 다만 기준선·현재값 선택 계약, 평점 grain, 기존 서비스 모델 범위와 게시 대상 해석을 수정한 뒤 구현 기준으로 사용하는 것이 적절하다. 아래 우선순위는 결함 심각도이며, 초안의 P0~P5 구현 단계와 구분한다.

검토 방법: 현재 소스, 기획 문서, S3 LIST/GET, Snowflake SELECT, PostgreSQL 읽기 전용 트랜잭션을 대조했다. 업무 코드·데이터는 변경하지 않았다. 재처리 운영, 알림, 모니터링은 범위에서 제외했다.

## 1. [P1] 레거시 편입 전에 기준선의 범위와 속성별 현재값 결정 규칙을 확정해야 함

대상: 초안 §6 P0, §7. 근거: `pipelines/dbt/models/dw/dim_movie.sql`, `pipelines/dbt/macros/observation_recency_order.sql`, `pipelines/transforms/movie_bronze_silver.py`의 transform_legacy_snapshot / _kmdb_enrichment / exchange_observations.

5,985건이라는 규모와 canonical key 중복 0건만으로 레거시가 전체 서비스 카탈로그를 포괄한다고 확정할 수 없다. 서비스 5,319건과의 교집합·서비스에만 있는 영화·ID 충돌을 아직 측정하지 않았다. 초안은 이 대사를 P0에 포함하면서도 그 결과를 확인하기 전에 레거시를 전체 기준선으로 확정하고 있다. 우선 ‘기준선 후보’로 명명하고, 서비스 전용 행의 보존 여부와 대상 연도/정책을 대사 후 결정해야 한다.

현재 dim_movie는 source_observed_at → publication_revision → publication_completed_at 등으로 **행 전체 하나**를 선택한다. 최신 API 관측에서 KMDb가 REVIEW_REQUIRED/UNMATCHED이면 보강 필드는 null/빈 배열이 된다. 이 행을 선택하면 기존의 정상 포스터·줄거리 등을 잃을 수 있다. 반대로 오래된 레거시의 이관 시각을 최신 관측 시각으로 넣으면 더 최신 API 정보를 덮을 수 있다. 레거시 adapter 자체에는 source_observed_at도 없다.

P0 완료 조건에 다음을 추가해야 한다: 원천의 실제 기준시각과 적재시각 구분, 시각 미상의 처리, 필드별 소유 원천/우선순위, ‘미수집·매칭보류·명시적 삭제’의 null 의미, 이전 확정값 유지 여부. 단순 COALESCE도 명시적 삭제를 무시하므로 계약 없이 적용하면 안 된다. 매칭 수정·원천 ID 부재에도 통합 ID를 유지할 방법 역시 bridge의 키/유일성 규칙으로 명시해야 한다.

## 2. [P1] fct_rating 권장 grain이 개별 평점과 집계 평점을 혼합함

대상: 초안 §5, §6 P4. 근거: `docs/project-plan-and-architecture.md` §6, `apps/was/migrations/008_external_reviews.up.sql`.

초안의 fct_rating = ‘영화 × 출처 × 관측 기준일’은 여러 사용자가 같은 날 같은 영화에 남기는 평가를 담을 수 없다. 이는 원천 집계 평점 snapshot 또는 mart_movie_rating의 grain에 가깝다. 기획의 fct_rating은 출처별 평점 관측 한 건이며 집계 마트와 구분되어 있다.

개별 평점은 source_system + 원천 평가 ID 또는 원천 사용자/영화/시각에 대한 명시적 계약으로 식별하고, 집계 평점은 별도 snapshot grain으로 정의해야 한다. 평점 척도, 평가 수, 집계 기준시각, 원천 원점수와 정규화 점수도 필요하다. fct_review의 ‘현재 상태 또는 버전’ 역시 둘 중 하나를 선택해야 PK를 결정할 수 있다.

현재 외부 리뷰 source_review_key는 원천 native ID가 아니라 SHA-256 import signature이며, source_user_key는 내부 users.id와 무관하다. 따라서 ‘원천 리뷰 ID 한 건’이라고만 적으면 수정 시 동일 리뷰인지 판별하는 계약이 빠진다. 최초 수집 과정이 미확인이라는 지적은 유지하되, 이미 구현된 import signature/외부 사용자 분리 계약은 함께 기재해야 한다.

## 3. [P2] 기존 서비스의 보정·분류·미디어·임베딩 모델이 전수 범위에서 빠져 있음

대상: 초안 §3.3, §4.7, §5, §8. 근거: `apps/was/migrations/001_movie_catalog.up.sql`, `009_was_product_features.up.sql`, `010_was_comments_and_curated_embedding_trigger.up.sql`, `014_remove_unused_onboarding_profiles.up.sql`; 현재 PostgreSQL dev 객체 목록.

실제 dev에는 movie_editorial, movie_category_links, movie_categories, display_categories, popcorn_movie_media, popcorn_movie_embeddings가 있다. 특히 movie_editorial의 plot_override와 is_removed는 배치 원본과 분리된 운영자 소유 값이고, 현재 트리거는 논리 삭제 영화의 재공개를 막는다. 영화/운영 보정 변경에서 임베딩 파생 데이터로 이어지는 관계도 이미 구현되어 있다.

이들을 DW에 모두 복제하라는 뜻은 아니다. 전수검사에는 각 객체의 grain, 소유 주체, DW 편입 여부, 게시 시 보존할 FK/속성, 파생 데이터의 기준 hash/model/version을 목록화해야 한다. ‘인물·장르·미디어 관계 모델 없음’은 **DW에 없음**으로 한정해야 기존 PostgreSQL 미디어 모델과 혼동하지 않는다. source_service_status/source_approval_status와 service_movie_id는 현재 legacy adapter에서도 null placeholder이므로, 이를 dbt에서 빼먹어 실제 운영 상태값을 잃고 있는 것처럼 서술해서도 안 된다.

회원 선호·챗 세션/메시지 등도 현재 서비스 모델에는 존재한다. 사용자/관리자 대시보드의 전수 범위를 표방하려면 분석 대상인지 OLTP 전용인지 적어야 한다. 폐기된 onboarding_profiles를 현행으로 복원하지 말고 현재 users 및 category 모델을 기준으로 확인한다.

§8의 `services/db/migrations`는 현재 작업 경로에 없다. 실제 근거 경로인 `apps/was/migrations`로 수정해야 한다.

## 4. [P2] 분석 게시본과 사용자 공개 마트를 구분하고 연관 데이터의 게시 범위를 함께 정해야 함

대상: 초안 §4.7, §6 P1.

policy_eligible=false 58건이 dw_serving에 포함된 것은 확인했다. 그러나 현재 게시본은 WAS/챗봇이 조회하지 않는 분석 snapshot이고, 관리자에게는 제외 사유가 필요하다. 따라서 포함 자체를 현행 사용자 노출 오류로 단정할 수 없다. 실제 공개 조회 연결 전에 사용자용 mart_movie_service를 도입한다는 요구는 타당하다.

Reverse ETL의 영화 입력만 필터링하면 그대로 게시하는 전체 fct_boxoffice_daily와의 참조 범위가 달라질 수 있다. 사용자 공개 영화/박스오피스를 함께 필터링할지, 관리자용 전체 카탈로그와 별도 dataset/view를 둘지 결정해야 한다. 정책 적격성, 매칭 품질, 관리자 승인, 공개 상태, 논리 삭제를 구분하고 마지막 세 항목의 최종 소유권은 서비스에 둔다. KMDb 매칭 실패만으로 공개를 차단할지도 제품 정책으로 명시해야 한다.

## 5. [P2] 핵심 계약과 검증을 P5까지 미루지 말고 각 구현 단계의 완료 조건으로 이동

대상: 초안 §6.

P0~P5를 작업 순서로 사용한다면 이를 단계 번호라고 밝혀야 한다. 현재 사고 대응 의미의 P0 장애가 입증된 것은 아니다. ID/기준선 우선 방향은 수용하지만 grain·PK·null·원천 소유권과 관계/매칭/정책 테스트가 P5이면 앞 단계 모델을 무엇으로 검증할지 불명확하다.

권장 순서는 ‘기준선 대사 + ID/속성 병합 계약 + 해당 테스트’를 첫 단위로, KMDb 필드 교정을 독립적인 다음 단위로 진행하는 것이다. 서비스 연결 전에 공개 계약과 참조 무결성을 검증한다. 이후 관계 정상화·박스오피스 확장·리뷰 편입 순서는 실제 대시보드 요구에 맞추되 각 단계에 계약과 테스트를 포함한다. 오래된 객체 정리만 마지막에 두어도 된다.

초안의 누적값 역행 테스트는 원천 정정에 따른 감소를 곧바로 불법 데이터로 판정하지 않도록 구분해야 한다. 순위의 유일성/범위는 조회일이 아니라 **대상일 × 조회 범위** 기준이며, 순위·점유율·상영횟수 등 모든 당일 지표가 모든 차원에서 additive인 것은 아니다.

## 독립 확인 결과와 세부 정정

| 항목 | 독립 결과 |
| --- | --- |
| 서비스 영화 / 리뷰 | 5,319 / 124,944건, 초안과 일치 |
| 리뷰 원천 | naver_movie 118,301건, source_system NULL 6,643건. 빈 문자열이라는 표현은 현재 데이터에 해당하지 않음 |
| S3 전체 객체 | 1,698개, 초안과 일치 |
| 레거시 변환 | 5,985건, 적격 4,320/제외 1,665, 기존 매칭 표시 5,312, canonical 중복 0 |
| Snowflake raw 관측 | 영화 526 / 박스오피스 230건 |
| dim_movie | 118건, 적격 60/제외 58, MATCHED 99/REVIEW_REQUIRED 16/UNMATCHED 3 |
| fct_boxoffice_daily | 80건, 2026-09-02~2026-09-09 |
| mart_data_quality | 5건, movie/boxoffice 건수 차이 합계 0, 현재 미연결 팩트 0 |
| PostgreSQL current 영화 | 118건 |

- S3 표의 열거 건수 합계는 1,697이다. 빠진 `manifests/pop_talk_admin_snapshot/...` 1건을 추가하면 실제 1,698과 일치한다.
- stg_successful_exchange_publications의 grain은 ‘artifact당’이 아니라 **source_run_id × artifact_version당 최신 성공 revision**이다. SQL PARTITION BY와 publication_key 모두 두 값을 사용한다. 서로 다른 source run을 artifact만으로 합치면 안 된다.
- movie_transform_runs의 물리 ledger 식별 조건은 READY key × transform version × publication revision × processing attempt다. 단순 ‘변환 실행 한 번’보다 이 조합을 적는 편이 정확하다.
- boxoffice는 대상일별 팩트의 최신 정정값이다. dim_movie를 Type 1이라 부르는 것은 적절하지만 날짜별 fact도 Type 1 차원처럼 표현하거나 이력이 전혀 없는 것으로 읽히지 않도록 구분한다. MVP에 SCD2 유효기간 모델이 없는 것 자체는 결함이 아니다.
- KMDb 기존 Raw 표본에서 최상위 vodUrl 없이 vods.vod[] 아래 vodClass/vodUrl을 확인했다. 변환은 최상위를 읽는다. Snowflake 영화 관측 526건 모두 vod_url이 null이었다. ‘항상 null’은 현재 데이터/해당 응답 구조의 범위로 한정한다.
- kmdb_rating 관측값은 Y 438건, Y||0 13건, Y||N 5건, 0||Y 5건, null 65건이었다. ratingMain을 우선 읽는 코드와 일치하며 평점으로 사용할 수 없다. ratingGrade는 등급 후보로 다루되 복수 ratings 및 대표 항목 선택도 정의해야 한다.
- posters/stills/keywords/vod_url/kmdb_rating/embedding_input_sha256의 dbt 미투영은 확인했다. 다만 MOVIE_OBSERVATIONS_RAW의 VARIANT payload에는 남아 있으므로 **Gold/serving 투영 누락**이며 원천 데이터의 영구 소실은 아니다.
- 독립 배열 중복 제거에 따른 배우↔배역, 감독 한글↔영문명 대응 손실, 제목·연도 중심 매칭, 일일 현재 연도 첫 100건 후보, 전역 미연결 수의 품질 행별 반복에 관한 진단은 코드와 일치한다.

## 검증 한계

Databricks Delta 전체를 직접 조회하거나 403 상태를 다시 재현하지 않았다. 그 부분은 초안 작성자의 관찰로 남긴다. 기존 초기 수집/레거시의 운영 경로 미편입과 서비스 조회의 dw_serving 미연결은 소스로 확인했다. 이번에는 모든 API 응답을 전수 재검증하지 않았으며, KMDb 구조는 기존 S3 표본으로 확인했다. 기획 모델 목록과 현재 서비스 소유 모델을 위와 같이 분리하면 이 초안을 다음 구현의 유효한 기준으로 사용할 수 있다.
