# Pop Talk DW 데이터 모델링 전수검사

- 검사일: 2026-09-10
- 검사 범위: 원천 API와 기존 데이터부터 S3 Raw, Databricks Bronze/Silver, S3 Exchange, Snowflake staging/DW/mart, PostgreSQL 게시까지
- 이번 검사에서 제외한 범위: 재처리·재시도 운영 절차, 알림, Airflow 모니터링 화면 개선
- 검사 방식: 구현 코드, 매니페스트, S3 객체, Snowflake 객체와 데이터, PostgreSQL 테이블과 서비스 조회 코드를 대조했다.

## 1. 결론

현재 파이프라인은 **일일 영화 API 표본을 한 바퀴 운반하고 검증하는 기술적 골격**은 갖췄다. 그러나 데이터 모델 관점에서는 아직 좁은 MVP다.

- 현재 Snowflake의 실질적인 Gold 범위는 영화 118건과 일별 박스오피스 80건이다.
- 서비스 PostgreSQL에는 영화 5,319건과 리뷰 124,944건이 있지만 현재 DW 모델과 연결되지 않는다.
- S3에는 기존 영화 5,985건짜리 레거시 스냅샷이 있고 현행 정제 규칙으로 4,320건이 정책 적격이지만, 자동 파이프라인 입력에는 포함되지 않는다. 이 스냅샷이 전체 서비스 카탈로그의 기준선인지는 PostgreSQL 영화 5,319건과 대사한 뒤 확정해야 한다.
- 기획된 모델 중 실제 구현된 핵심 모델은 `dim_movie`, `fct_boxoffice_daily`, `mart_boxoffice_daily`, `mart_data_quality`뿐이다.
- Silver가 가진 포스터 목록, 스틸컷, 키워드 등의 풍부한 속성 일부가 dbt staging에서 사라진다.
- KMDb VOD와 등급 필드의 매핑 오류가 있다.
- `policy_eligible=false`인 영화도 PostgreSQL 게시 데이터에 포함된다.
- PostgreSQL의 `dw_serving` 게시본은 실제 WAS·관리자·챗봇이 조회하는 `dev.popcorn_movies` 계열과 연결되지 않았다.

따라서 다음 개발의 핵심은 새 도구를 추가하는 것이 아니라 **기준선 후보 대사, 영화 식별자 통합, 필드별 현재값 계약, 공개용 마트 분리, 누락 속성 보존, 리뷰 모델 편입**이다.

## 2. 현재 데이터 흐름

```text
KOFIC/KOBIS + KMDb API
        │
        ▼
S3 raw + manifests
        │
        ▼
Databricks Bronze(원문 객체) → Silver(정제 관측)
        │
        ▼
S3 exchange(JSONL + EXCHANGE_READY)
        │
        ▼
Snowflake staging(VARlANT 관측) → dbt DW → dbt mart
        │
        ▼
PostgreSQL dw_serving(JSONB 스냅샷)

별도 상태:
S3 legacy_snapshot 5,985건 ── 아직 위 흐름에 미편입
PostgreSQL dev 영화 5,319건·리뷰 124,944건 ── 아직 DW와 미연결
```

## 3. 수집 원천과 호출 방식

### 3.1 KOFIC/KOBIS

- 기본 URL: `https://www.kobis.or.kr/kobisopenapi/webservice/rest`
- Airflow 연결: `pop_talk_kofic`
- 구현: `pipelines/collectors/movie_raw.py`, `pipelines/collectors/movie_daily.py`

| API | 역할 | 주요 호출 조건 | Raw에서 확인한 주요 필드 |
|---|---|---|---|
| `boxoffice/searchDailyBoxOfficeList` | 최근 일별 박스오피스 영화 후보와 실적 수집 | 기본 최근 7일의 `targetDt` | 순위, 매출액·누적, 관객수·누적, 점유율, 스크린수, 상영횟수 등 |
| `movie/searchMovieList` | 현재 연도 영화 후보 보충 | 현재 연도 개봉, 장편영화, 첫 페이지 100건 | 영화코드, 제목, 개봉일, 제작연도, 국가, 장르, 감독, 제작사 등 |
| `movie/searchMovieInfo` | 후보 영화 상세 보강 | 각 `movieCd` | 러닝타임, 원제, 배우·배역, 감독, 장르, 국가, 제작사, 관람등급 등 |

일일 수집의 후보 집합은 다음 두 집합의 합집합이다.

1. 최근 기본 7일 박스오피스에 등장한 영화
2. 현재 연도 장편영화 검색 결과의 첫 100건

이 방식은 CDC나 전체 영화 증분 수집이 아니다. 현재 연도 2페이지 이후 영화, 박스오피스에 들지 않은 영화, 과거 영화의 정보 변경은 놓칠 수 있다. 따라서 일일 API는 **전체 카탈로그의 기준선**이 아니라 **최근 관측 갱신원**으로 모델링해야 한다.

초기 수집 DAG는 연도별 영화목록을 페이지 순회하고 상세·KMDb 후보를 모을 수 있지만, 그 결과는 현재 Main Asset 체인으로 자동 연결되지 않는다.

### 3.2 KMDb

- 기본 URL: `https://api.koreafilm.or.kr/openapi-data2/wisenut/search_api/search_json2.jsp`
- Airflow 연결: `pop_talk_kmdb`
- 호출 조건: KOFIC 한글·영문 제목별 검색, 상세 응답, 페이지당 100건, 기본 최대 3페이지
- 역할: KOFIC 영화에 포스터, 스틸, 줄거리, 키워드, VOD, KMDb 식별자 등의 보강 후보를 제공한다.

Raw에서 확인한 주요 구조는 `DOCID`, 제목군, 제작연도, 감독, 배우, 국가, 장르, 줄거리, 포스터, 스틸, 키워드, `ratings`, `vods` 등이다.

Raw 단계에서는 후보를 제거하거나 하나로 확정하지 않는다. Silver에서 정규화 제목과 제작연도를 이용해 `MATCHED`, `REVIEW_REQUIRED`, `UNMATCHED`로 판정한다.

현재 매칭은 제목·연도 중심이라 동명 영화에 취약하다. 감독, 국가, 러닝타임을 증거와 점수에 포함하고, 매칭 규칙 버전과 선택 근거를 별도 모델에 보존해야 한다.

### 3.3 기존 내부 데이터

현재 자동 수집 API와 별개로 중요한 두 원천이 있다.

| 원천 | 현재 규모 | 상태 |
|---|---:|---|
| S3 `legacy_snapshot`의 `movies_final.json` | 5,985 영화 | 변환 함수와 검사는 있으나 Main 흐름에 미편입 |
| PostgreSQL `dev.popcorn_movies` | 5,319 영화 | 서비스의 현재 영화 기준, DW와 미연결 |
| PostgreSQL `dev.reviews` | 124,944 리뷰 | DW 수집·정제·팩트 모델 없음 |

레거시 영화 5,985건을 현행 변환 규칙으로 읽은 결과는 다음과 같다.

- 정책 적격: 4,320건
- 제외: 1,665건
- KMDb 기존 매칭 표시: 5,312건
- 미매칭: 673건
- canonical key 중복: 0건

PostgreSQL 리뷰의 `source_system`은 `naver_movie` 118,301건, NULL 6,643건이다. 다만 현재 저장소에는 Naver 리뷰를 원천부터 수집한 코드와 계약이 없어, 이 데이터의 출처·수집일·수정 및 삭제 처리 방식은 문서로 복원해야 한다.

## 4. 레이어별 모델 전수검사

### 4.1 S3 Raw와 manifests

검사 시점 S3 객체는 총 1,698개였다.

| 경로 | 객체 수 | 의미 |
|---|---:|---|
| `raw/movie_api_daily/v1` | 1,606 | 일일 KOFIC/KMDb 응답 원문 |
| `raw/movie_api/v1` | 5 | 초기·수동 수집 원문 |
| `raw/legacy_snapshot/v1` | 1 | 기존 영화 5,985건 스냅샷 |
| 기타 관리자 스냅샷 Raw | 1 | 현재 파이프라인 외 과거 표본 |
| `manifests/movie_api_daily/v1` | 48 | 일일 실행 단계·완료 계약 |
| `manifests/movie_api/v1` | 5 | 초기 실행 계약 |
| `manifests/legacy_snapshot/v1` | 1 | 레거시 스냅샷 계약 |
| 기타 관리자 스냅샷 manifest | 1 | 현재 파이프라인 외 과거 표본의 계약 |
| `exchange/movie_silver/v1` | 30 | Silver 반출 파일·완료 계약 |

Raw와 manifest의 역할 분리는 적절하다.

- Raw: API가 반환한 원문을 변경하지 않고 보관한다.
- manifest: 어떤 입력을 언제 어떤 실행이 수집했고, 파일 개수·체크섬·완료 상태가 무엇인지 기록한다.

주의할 점은 경로가 존재한다고 모델에 편입된 것은 아니라는 것이다. `legacy_snapshot`과 초기 수집 결과는 현재 자동 Gold 흐름의 입력이 아니다.

### 4.2 Databricks Bronze

모델: `workspace.bronze.movie_raw_objects`

- grain: S3 Raw 객체 한 개
- 키 역할: `object_key`
- 주요 컬럼: `run_id`, 원천, 리소스, 요청 JSON, 수집시각, payload 체크섬, payload JSON, 적재시각

Bronze는 원문 재현성과 감사에 적합하다. 비즈니스 엔터티를 표현하는 테이블이 아니라 수집 객체 원장이다.

### 4.3 Databricks Silver

| 모델 | grain | 역할 |
|---|---|---|
| `movie_observations` | source run × transform version × canonical movie | 영화 정제·매칭·정책 판정 관측 |
| `boxoffice_observations` | source run × transform version × 기준일 × KOFIC 영화코드 | 일별 박스오피스 관측 |
| `movie_transform_runs` | READY key × transform version × publication revision × processing attempt | 변환 실행 원장 |
| `movies_current` | canonical movie당 최신 1건 | 최신 영화 관측 뷰 |
| `boxoffice_current` | 기준일 × 영화당 최신 1건 | 최신 박스오피스 뷰 |

영화 canonical key는 현재 `kofic:<movieCd>`다. KOFIC 상세로 제목, 날짜, 러닝타임, 인물, 장르, 국가, 제작사, 관람등급을 구성하고 KMDb 후보에서 포스터·스틸·줄거리·키워드 등을 보강한다.

정책은 개봉일 필수, 2020-01-01 이후, 성인·에로·다큐 장르와 회사·감독 블랙리스트 등을 검사한다. 행을 삭제하지 않고 `policy_eligible`과 제외 이유를 기록하는 방식은 감사와 정책 변경에 유리하다.

현재 Silver의 주요 문제는 다음과 같다.

1. `vod_url`은 확인한 KMDb 응답의 실제 `vods.vod[].vodUrl` 구조가 아니라 최상위 `vodUrl`을 읽어 현재 관측 526건에서 모두 null이다.
2. `kmdb_rating`은 `ratingGrade`보다 `ratingMain`을 우선해서 실제 값이 `Y`, `Y||0`처럼 저장된다. 평점인지 관람등급인지 의미도 불명확하다.
3. 배우명과 배역을 서로 독립된 배열로 중복 제거해 배우↔배역 관계가 손실된다.
4. 감독 한글명과 영문명도 독립 배열이어서 같은 사람의 표기 대응을 보장하지 못한다.
5. KMDb 매칭 증거, 점수, 규칙 버전을 별도 영속 모델로 보존하지 않는다.
6. 박스오피스 Silver는 순위, 당일 매출액, 당일 관객수, 누적 관객수만 남긴다. Raw에 있는 누적 매출, 점유율·증감, 스크린수, 상영횟수, 순위 이동 정보가 소실된다.

Databricks Free Edition의 카탈로그 REST/SQL warehouse 조회 권한은 403으로 제한되어 이번 검사에서는 Delta 테이블을 API로 직접 전수 조회하지 못했다. 다만 성공한 Databricks 실행, Exchange 체크섬, Snowflake 관측 건수로 전체 전달 계약은 검증했다.

### 4.4 S3 Exchange

경로 grain은 source run × artifact version × publication revision이다. `movies.jsonl`, `boxoffice.jsonl`을 먼저 쓰고 체크섬·건수를 담은 `EXCHANGE_READY.json`을 마지막에 게시한다.

이 구간은 Databricks와 Snowflake 사이의 명시적 계약이며, Free Edition 환경에서 양쪽을 느슨하게 결합하는 현재 방식은 합리적이다.

### 4.5 Snowflake staging

| 모델 | 역할 |
|---|---|
| `SILVER_EXCHANGE_LOADS` | Exchange 적재 원장 |
| `MOVIE_OBSERVATIONS_RAW` | 영화 Silver 레코드를 VARIANT로 보관 |
| `BOXOFFICE_OBSERVATIONS_RAW` | 박스오피스 Silver 레코드를 VARIANT로 보관 |
| `stg_successful_exchange_publications` | source run × artifact version별 최신 성공 publication revision 선택 |
| `stg_movie_observations` | 성공 publication에 속한 영화 속성 투영 |
| `stg_boxoffice_observations` | 성공 publication에 속한 박스오피스 속성 투영 |

현재 staging 물리 데이터는 영화 관측 526건, 박스오피스 관측 230건이다. 최신 엔터티로 줄이면 영화 118건, 날짜×영화 80건이며 박스오피스 기간은 2026-09-02부터 2026-09-09까지다.

`stg_movie_observations`가 Silver payload 중 다음 필드를 투영하지 않아 Gold와 serving에서 활용할 수 없다. 원문은 `MOVIE_OBSERVATIONS_RAW`의 VARIANT에 남아 있으므로 영구 소실은 아니다.

- `service_movie_id`
- 전체 `posters`, `stills`, `keywords`
- `kmdb_rating`, `vod_url`
- `embedding_input_sha256`
- 현재 null placeholder인 서비스 상태·승인 상태 관련 필드

필드를 모두 `dim_movie`에 넣을 필요는 없지만, 분석 또는 서비스에 필요한 것은 staging에서 타입을 부여하고 자식 모델로 분리해야 한다.

### 4.6 Snowflake dbt DW와 mart

현재 dbt 모델은 7개, 테스트는 43개다.

| 계층 | 모델 | grain·역할 |
|---|---|---|
| staging | `stg_successful_exchange_publications` | source run × artifact version당 최신 성공 publication revision |
| staging | `stg_movie_observations` | 성공 publication의 영화 관측 |
| staging | `stg_boxoffice_observations` | 성공 publication의 박스오피스 관측 |
| DW | `dim_movie` | canonical movie당 최신 1건, Type 1 현재상태 |
| DW | `fct_boxoffice_daily` | 기준일 × KOFIC 영화 한 건 |
| mart | `mart_boxoffice_daily` | 박스오피스 팩트와 영화 차원 결합 |
| mart | `mart_data_quality` | publication별 건수 대사와 전역 미매칭 팩트 수 |

현재 결과는 다음과 같다.

- `dim_movie`: 118건
- 정책 적격 60건, 제외 58건
- KMDb `MATCHED` 99건, `REVIEW_REQUIRED` 16건, `UNMATCHED` 3건
- `fct_boxoffice_daily`: 80건
- 모든 박스오피스 팩트가 영화 차원과 연결됨
- publication 입력/관측 건수 차이 0

구조적 문제는 다음과 같다.

1. `dim_movie`는 최신 행 하나를 택하는 Type 1이며 분석용 유효기간 모델은 없다. 박스오피스 팩트는 대상일별 최신 정정값을 선택하고 이전 관측은 staging/Delta에 남는다.
2. 영화 현재값도 행 전체 하나를 선택한다. 최신 API 관측의 KMDb 매칭이 보류되면 과거의 정상 포스터·줄거리가 빈 값으로 바뀔 수 있으므로 필드별 소유 원천·null 의미·유지 규칙이 필요하다.
3. 원천 ID 매핑이 `dim_movie`의 속성에 묻혀 있고 `bridge_movie_source_id`가 없다.
4. DW에는 인물·배역·별칭, 장르, 제작사, 미디어의 관계 모델이 없다. PostgreSQL 서비스에는 별도의 미디어·분류 모델이 있으므로 서로 구분해야 한다.
5. 박스오피스 원천은 일일 순위 목록이므로 행 부재가 0을 의미하지 않는다. 수집 범위와 조회 조건 모델이 필요하다.
6. 누적 관객·누적 매출은 날짜 간 합산할 수 없는 semi-additive 지표인데 마트 계약에 의미가 명시되지 않았다.
7. `mart_data_quality`의 미매칭 팩트 수는 publication별 값이 아니라 전체 현재 팩트 값인데 각 행에 반복된다.
8. 현재 테스트에는 음수 측정값, 대상일×조회범위별 순위 범위·유일성, 허용 상태값, 매칭 불변식, 개봉일·연도 일관성, 정책 계약 등이 충분하지 않다.
9. 예전 MVP 객체(`DIM_MOVIE_MVP`, `MART_MOVIE_DATA_QUALITY_MVP`, `STG_MOVIES`, `MOVIES_SILVER_RAW`, `CONNECTION_TEST`)가 남아 있어 BI와 개발자가 현행 모델로 오인할 수 있다.

기획 문서에는 있지만 구현되지 않은 모델은 다음과 같다.

- `bridge_movie_source_id`
- `dim_person`, `person_alias`, `bridge_movie_person`
- `dim_date`, `dim_source`
- `dim_genre`, `bridge_movie_genre`
- `fct_rating`, `fct_review`
- `mart_movie_rating`
- MovieLens 적재와 영화 매핑
- 리뷰 감성·텍스트 분석 모델

### 4.7 PostgreSQL serving

현재 Reverse ETL은 Snowflake의 `DW.DIM_MOVIE`, `DW.FCT_BOXOFFICE_DAILY`, `MART.MART_DATA_QUALITY`를 읽어 `dw_serving`의 JSONB snapshot v3에 게시한다.

현재 active `movie_gold` 게시본은 영화 118건, 박스오피스 80건, 품질 5건이다. 게시 세대 교체와 마지막 정상본 유지 구조는 갖췄다.

하지만 데이터 모델 관점에서 두 경계가 아직 정의되지 않았다.

1. `policy_eligible=false` 58건도 그대로 게시된다. 현재 `dw_serving`은 사용자 공개본이 아닌 분석 snapshot이므로 포함 자체가 오류는 아니지만, 실제 서비스 연결 전에는 관리자용 전체 데이터와 사용자 공개용 마트를 분리해야 한다.
2. WAS·관리자·챗봇 코드는 `dev.popcorn_movies` 또는 `dev.popcorn_movies_service`를 조회하며 `dw_serving`을 조회하지 않는다. 따라서 현재 Reverse ETL 완료는 **분석 스냅샷의 PostgreSQL 적재 성공**이지 **실제 서비스 반영 성공**은 아니다.

기존 회원·관리자 상태·서비스 공개 상태는 DW가 덮어쓰면 안 된다. DW에서 계산한 속성과 기존 서비스가 소유하는 속성을 구분하고, 명시적 식별자 브리지를 거쳐 승인된 필드만 게시해야 한다.

서비스 PostgreSQL에는 다음 모델과 소유 경계가 이미 있다.

| 모델 | 역할·grain | 소유 경계 |
|---|---|---|
| `popcorn_movies` | 서비스 영화 한 편 | 서비스 식별자와 기본 공개 레코드 |
| `movie_editorial` | 영화별 운영자 보정 | `plot_override`, `is_removed` 등은 관리자 소유이며 DW가 덮어쓰지 않음 |
| `movie_categories`, `movie_category_links`, `display_categories` | 카테고리와 영화 분류 관계 | 서비스 탐색·표시 정책 소유 |
| `popcorn_movie_media` | 영화별 미디어 한 건 | 서비스에서 사용하는 포스터·스틸 등의 관계 |
| `popcorn_movie_embeddings` | movie ID × document type × chunk number × embedding model | 입력 hash, 모델·버전과 파생 규칙을 보존해야 함 |
| `reviews` | 내부 또는 외부 리뷰 한 건 | 회원 리뷰와 import 리뷰의 식별·수정 규칙 분리 |

논리 삭제 영화의 자동 재공개를 막고 보정 변경에서 임베딩을 갱신하는 트리거도 이미 존재한다. 이 모델들을 모두 DW에 복제할 필요는 없지만, Reverse ETL은 이 소유권과 FK를 보존해야 한다. 회원 선호·채팅 데이터는 현재 단계에서는 OLTP 전용으로 두며, 향후 대시보드 지표 요구가 확정될 때 별도 분석 계약을 만든다.

## 5. 가장 중요한 엔터티와 권장 grain

| 도메인 | 권장 모델 | 권장 grain |
|---|---|---|
| 영화 | `dim_movie` | 통합 영화 한 편 |
| 원천 식별자 | `bridge_movie_source_id` | 원천 시스템의 영화 ID 한 개 |
| 서비스 연결 | `bridge_movie_service_id` | canonical movie와 서비스 movie의 연결 한 건 |
| 매칭 판정 | `movie_source_match_observation` | 실행 × 기준 영화 × 후보 × 규칙 버전 |
| 인물 | `dim_person` | 사람 한 명 |
| 인물 표기 | `person_alias` | 원천이 있는 이름 표기 한 건 |
| 영화 인물 | `bridge_movie_person` | 영화 × 인물 × 역할 × 순서 |
| 장르 | `dim_genre` | 정규 장르 한 개 |
| 영화 장르 | `bridge_movie_genre` | 영화 × 장르 × 원천 |
| 미디어 | `bridge_movie_media` | 영화 × 미디어 URL × 유형 × 원천 |
| 박스오피스 | `fct_boxoffice_daily` | 대상일(`target_date`) × 영화 × 집계 범위 |
| 리뷰 현재상태 | `fct_review` | source system × 안정적인 원천 리뷰 키 한 건 |
| 리뷰 버전 이력(선택) | `fct_review_version` | source system × 원천 리뷰 키 × 관측 또는 변경 버전 |
| 개별 평점 | `fct_rating` | source system × 원천 평가 식별자 한 건. native ID가 없으면 사용자·영화·시각 또는 import signature 계약 필요 |
| 집계 평점 관측 | `fct_rating_snapshot` | 영화 × 출처 × 집계 기준시각 × 평점 척도 |
| 서비스 게시 | `mart_movie_service` | 서비스에 게시 가능한 영화 한 편 |

배열을 모두 차원 테이블로 분해할 필요는 없다. 다만 관계 자체가 분석 대상이거나 순서·역할·원천을 보존해야 하는 인물, 장르, 미디어는 자식 모델이 적합하다. 원문 배열도 감사용으로 유지할 수 있다.

## 6. 단계별 데이터 모델링 작업

여기서 단계 0~5는 장애 심각도가 아니라 구현 순서를 뜻한다. grain·키·null·소유권 계약과 dbt 검증은 마지막에 몰지 않고 각 단계의 완료 조건에 포함한다.

### 단계 0 — 기준선 후보 대사와 현재값 계약

1. S3 레거시 5,985건과 PostgreSQL 서비스 영화 5,319건을 먼저 대사한다.
2. 교집합, 서비스에만 있는 영화, 레거시에만 있는 영화, KOFIC ID 충돌, 대상 연도·정책 차이를 산출한다.
3. 레거시는 확정 전까지 **전체 기준선 후보**로 취급한다. 대사 결과로 보존 범위와 초기 기준선을 결정한다.
4. 원천의 실제 관측시각과 적재시각을 분리하고, 시각 미상의 레거시가 최신 API를 덮지 않도록 규칙을 정한다.
5. 제목·개봉일·줄거리·포스터 등 필드별 소유 원천과 우선순위를 정한다.
6. null을 `미수집`, `매칭 보류`, `명시적 삭제`로 구분하고 이전 확정값 유지 규칙을 정한다. 단순 `COALESCE`만으로 해결하지 않는다.
7. 위 계약을 schema YAML과 테스트로 먼저 고정한다.

### 단계 1 — 전체 영화 관측과 식별자 통합

1. 확정한 레거시 범위를 현재 observation 계약에 편입한다.
2. 기준선 관측과 일일 API 최신 관측을 필드별 현재값 규칙으로 통합한다.
3. `bridge_movie_source_id`를 만들고 KOFIC, KMDb, 향후 MovieLens ID를 행 단위로 관리한다.
4. `bridge_movie_service_id`를 만들어 PostgreSQL 서비스 영화와 연결한다.
5. 매칭 수정이나 특정 원천 ID 부재에도 통합 영화 ID가 안정적으로 유지되도록 surrogate key, 유일성, 유효상태 계약을 둔다.
6. 기준선·서비스·최신 API의 교집합, 미매칭, 충돌을 품질 마트와 테스트로 공개한다.

완료 기준은 “한 영화가 각 시스템에서 어떤 ID인지”와 “어느 원천 관측이 현재 값을 결정했는지”를 SQL로 설명할 수 있는 것이다.

### 단계 2 — KMDb 의미 오류와 영화 관계 모델 수정

1. KMDb `vods.vod[].vodUrl`을 정상 추출한다.
2. `kmdb_rating`을 폐기 또는 의미에 맞게 분리한다. `ratingGrade`는 복수 `ratings` 중 대표 항목 선택 규칙이 필요한 관람등급 후보이며 `ratingMain`은 평점이 아니다.
3. 매칭 후보·선택 근거·점수·규칙 버전을 별도 모델로 저장한다.
4. Silver의 포스터, 스틸, 키워드, VOD와 필요한 해시를 Snowflake까지 투영한다.
5. `dim_person`, `person_alias`, `bridge_movie_person`을 구현해 배우↔배역과 감독 별칭 관계를 보존한다.
6. 장르와 제작사를 정규화하고 포스터·스틸·VOD는 `bridge_movie_media`로 분리한다.
7. 각 관계의 grain·순서·원천·대표값 규칙과 테스트를 함께 구현한다.

### 단계 3 — 분석용과 사용자 공개용 게시 경계 분리

1. 관리자 분석용 전체 영화와 사용자 공개용 `mart_movie_service`를 분리한다.
2. 정책 적격성, KMDb 매칭 품질, 관리자 승인, 공개 상태, 논리 삭제를 별도 조건으로 유지한다.
3. 관리자 승인·공개 상태·논리 삭제의 최종 소유권은 PostgreSQL 서비스에 둔다.
4. KMDb 매칭 실패를 공개 차단 조건으로 사용할지는 제품 정책으로 확정한다.
5. 사용자 공개 영화만 필터링할 때 박스오피스도 같은 범위로 제한할지, 관리자용 전체 팩트를 별도 dataset으로 둘지 결정한다.
6. Reverse ETL은 목적에 맞는 mart를 읽고 서비스 FK·운영자 보정·카테고리·미디어·임베딩을 보존한다.
7. 공개 영화와 연관 팩트의 참조 무결성을 테스트한다.

### 단계 4 — 박스오피스 팩트 확장

1. Raw의 누적 매출, 매출 점유율·증감, 스크린수, 상영횟수, 순위 변화 필드를 Silver와 fact까지 전달한다.
2. 박스오피스 조회 범위·API 조건을 `dim_source` 또는 coverage 모델에 기록한다.
3. 매출액·관객수·점유율·순위·상영횟수 각각의 가산 가능 차원을 계약에 명시한다. 누적 지표는 날짜 간 합산하지 않는다.
4. 대상일×조회범위별 순위 유일성·범위와 0 이상 측정값을 테스트한다. 누적값 감소는 원천 정정일 수 있으므로 실패가 아니라 별도 이상 관측으로 분류한다.

### 단계 5 — 리뷰와 평점 모델 편입

1. PostgreSQL 리뷰를 최초 기준선으로 immutable extraction하고 원천 계약을 작성한다.
2. 내부 회원 리뷰와 외부 `naver_movie` 리뷰의 사용자 식별자·수정·삭제 규칙을 분리한다.
3. 서비스 영화 ID 브리지를 통해 canonical movie로 연결한다.
4. `fct_review`는 현재상태 모델로 두고, 수정 이력이 필요하면 source system × 원천 리뷰 키 × 변경 버전 grain의 `fct_review_version`을 별도로 둔다.
5. 개별 `fct_rating`과 출처 집계 `fct_rating_snapshot`을 구분한다. 원점수, 정규화 점수, 척도, 평가 수, 기준시각을 보존한다.
6. 현재 외부 `source_review_key`는 native ID가 아니라 SHA-256 import signature임을 계약에 명시하고 동일 리뷰의 수정 식별 한계를 관리한다. `source_user_key`는 내부 회원 ID와 분리한다.
7. `mart_movie_rating`에는 출처·기간·매칭 성공률을 항상 함께 보여 준다.
8. 위 grain과 수정·삭제 규칙을 검증하는 테스트를 함께 구현한다.

MovieLens 3,200만 건은 위 ID 브리지와 평점 grain이 검증된 다음에 추가해야 한다.

### 마지막 정리

1. `mart_data_quality`를 실행별 품질과 현재 전역 품질로 분리한다.
2. 사용 확인 후 Snowflake와 PostgreSQL의 오래된 MVP/test 객체를 별도 스키마로 격리하거나 제거한다.

## 7. 권장 다음 구현 단위

다음 개발은 한 번에 리뷰·검증 가능한 아래 단위가 적절하다.

**“영화 기준선 대사와 현재값 계약 v1”**

- 레거시 5,985건과 PostgreSQL 서비스 영화 5,319건의 ID·범위 대사
- 교집합·양쪽 전용 행·ID 충돌·정책 차이 산출
- 원천 관측시각/적재시각, 필드별 소유권, null 의미, 이전값 유지 계약 작성
- stable canonical ID와 source/service bridge의 grain·유일성 설계
- 위 계약을 검증할 dbt 모델·테스트 명세 작성

이 단위를 먼저 끝내야 레거시 편입이 기존 서비스 영화를 빠뜨리거나 최신 API 보강값을 덮는 일을 막을 수 있다. 다음 구현 단위에서 레거시 observation 적재와 source/service bridge를 실제로 만들고, 이후 KMDb 교정과 서비스 게시 마트를 붙인다.

## 8. 근거 파일

- 아키텍처: `docs/project-plan-and-architecture.md`
- 일일·초기 수집: `pipelines/collectors/movie_daily.py`, `pipelines/collectors/movie_raw.py`
- Airflow 수집 DAG: `orchestration/airflow/dags/pop_talk_movie_raw_daily.py`, `orchestration/airflow/dags/pop_talk_movie_raw_initial.py`
- Databricks 변환: `pipelines/transforms/movie_bronze_silver.py`, `pipelines/databricks/03_movie_bronze_silver_daily.py`
- Exchange: `pipelines/transforms/exchange_publisher.py`
- Snowflake loader: `pipelines/transforms/snowflake_exchange_loader.py`
- dbt 모델: `pipelines/dbt/models`
- PostgreSQL 게시: `pipelines/dbt/scripts/publish_to_postgres_v3.py`
- 서비스 DB 기준: `apps/was/migrations`, WAS·관리자·챗봇의 `popcorn_movies` 조회 코드
