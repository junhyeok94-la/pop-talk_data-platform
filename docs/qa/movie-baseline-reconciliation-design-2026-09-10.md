# 영화 기준선 대사와 현재값 계약 v1 설계

- 상태: 구현 전 검토 요청
- 상위 근거: `docs/qa/data-modeling-full-audit-2026-09-10.md`
- 범위: S3 레거시 영화 5,985건과 PostgreSQL 서비스 영화 5,319건의 읽기 전용 대사 및 후속 통합 계약
- 제외: 레거시의 Bronze/Silver 실제 적재, Gold 변경, 서비스 DB 갱신, 재시도·알림·모니터링

## 1. 목적

레거시 5,985건을 곧바로 전체 기준선으로 선언하지 않는다. 먼저 서비스 영화 5,319건과 동일한 규칙으로 비교해 다음 질문에 데이터로 답한다.

1. KOFIC ID 기준 교집합과 양쪽 전용 영화는 각각 몇 건인가?
2. 동일 KOFIC ID에서 제목·개봉일·KMDb ID 등 핵심 필드가 얼마나 충돌하는가?
3. 중복되거나 비어 있어 stable canonical ID로 쓸 수 없는 원천 ID가 있는가?
4. 특정 Gold publication의 현재 영화 ID 집합과 연결은 어느 정도인가?
5. 레거시를 기준선으로 편입할 때 서비스 전용 영화와 운영자 소유 값을 어떻게 보존해야 하는가?

## 2. 구현 단위

### 2.1 순수 대사 모듈

신규 모듈 `pipelines/modeling/movie_baseline_reconciliation.py`를 만든다.

- 외부 SDK나 Airflow에 의존하지 않는다.
- 레거시 JSON과 PostgreSQL 조회 행을 입력받는다.
- 문자열·날짜·ID 정규화와 분류를 결정론적으로 수행한다.
- 결과는 dataclass/일반 dict 형태로 반환한다.
- 원천 레코드 전체나 개인정보는 출력하지 않고 영화 식별·충돌 진단에 필요한 값만 다룬다.

### 2.2 실행 어댑터

신규 스크립트 `scripts/reconcile_movie_baseline.py`를 만든다.

- Airflow의 `pop_talk_aws` 연결을 통해 S3 legacy SUCCESS manifest와 원본을 읽는다.
- `POP_TALK_POSTGRES_*` 환경변수로 PostgreSQL에 읽기 전용 트랜잭션을 연다.
- `dev.popcorn_movies`와 필요한 운영 경계 집계만 조회한다.
- Airflow의 `pop_talk_snowflake` 연결로 현재 Gold의 KOFIC ID 집합과 이를 만든 publication 식별자를 읽는다. 이 입력은 v1 완료 판정에 필수다.
- 표준 출력에는 비밀값 없이 JSON 결과만 기록한다.
- 어떤 클라우드·DB 데이터도 변경하지 않는다.

### 2.3 테스트와 증거

- `pipelines/modeling/tests/test_movie_baseline_reconciliation.py`
- ID 중복, ID 없음, 날짜 형식, 제목 Unicode/공백, 서로 다른 KMDb ID, 서비스 전용·레거시 전용 분류를 검사한다.
- 실데이터 실행 결과는 `docs/qa/movie-baseline-reconciliation-result-2026-09-10.md`에 집계와 해석만 기록한다.
- 재현 가능한 기계 판독 결과는 `.local` 아래 JSON으로 남기되 Git에는 넣지 않는다.

## 3. 대사 grain과 분류

기본 대사 grain은 입력 행이며, ID 그룹 진단 grain은 정규화된 `kofic_movie_cd` 한 개다. 서비스 DB는 해당 컬럼에 UNIQUE 제약이 있지만 양쪽 입력 모두 제약을 신뢰하지 않고 검증한다.

KOFIC ID는 원래 문자열로 읽어 선행 0을 보존하고 양끝 공백만 제거한다. 비교는 대소문자를 보존한 exact 값으로 수행한다. 형식은 `[A-Za-z0-9]{1,32}`이며 이것은 외부 KOFIC의 의미적 유효성을 보증하는 것이 아니라 파이프라인 비교용 형식 검사다. casefold 결과가 같은 서로 다른 ID는 별도 충돌로 보고 자동 결합하지 않는다.

| 분류 | 정의 |
|---|---|
| `singleton_matched` | 양쪽에 동일한 유효 KOFIC ID가 정확히 한 행씩 존재 |
| `singleton_legacy_only` | 레거시에만 유효 KOFIC ID가 정확히 한 행 존재 |
| `singleton_service_only` | 서비스 DB에만 유효 KOFIC ID가 정확히 한 행 존재 |
| `missing_kofic` | 해당 원천 행의 KOFIC ID가 null 또는 trim 후 빈 문자열 |
| `invalid_kofic` | 해당 원천 행의 ID가 비교용 형식에 맞지 않음 |
| `duplicate_ambiguous` | 어느 한쪽이라도 같은 ID가 둘 이상이며, 그 ID 그룹에 속한 양쪽의 모든 행 |

분류는 원천별 행 단위로 상호 배타적이다.

```text
legacy_total_rows
= legacy_missing + legacy_invalid + legacy_duplicate_ambiguous
 + legacy_singleton_matched + legacy_singleton_only

service_total_rows
= service_missing + service_invalid + service_duplicate_ambiguous
 + service_singleton_matched + service_singleton_only
```

중복 ID 그룹 수와 그 그룹에 포함된 행 수를 별도로 출력한다. 예를 들어 레거시 2행과 서비스 1행이 같은 ID면 세 행 모두 각 원천의 `duplicate_ambiguous`에 속하며 matched 비교에는 들어가지 않는다.

KOFIC ID가 없거나 invalid인 레거시 행은 `legacy-row:<snapshot_sha256>:<source_row_number>`를 임시 관측 키로만 유지한다. 통합 영화 ID로 자동 승격하거나 제목만으로 서비스 영화와 자동 결합하지 않는다. 기존 `transform_legacy_snapshot`은 중복 canonical key에서 실패하므로 진단 입력기로 재사용하지 않고, manifest 무결성 검사와 행 분류를 먼저 독립 수행한다.

## 4. 동일 ID의 필드 비교

교집합 영화는 다음 필드를 비교한다.

| 필드 | 비교 규칙 | v1 해석 |
|---|---|---|
| `title_ko` | NFC, 연속 공백 정리 후 exact | 차이는 검토 대상이며 자동 우선순위를 정하지 않음 |
| `title_en`, `title_original` | NFC·공백 정리 | 값 상태까지 포함해 비교 |
| `release_date` | 유효 ISO date로 정규화 | 파싱 오류는 missing과 분리한 핵심 충돌 |
| `production_year` | 4자리 정수 | 파싱 오류는 missing과 분리한 보조 충돌 |
| `kmdb_id` | trim 후 case-preserving exact | source bridge 충돌 후보 |
| `kmdb_matched` | 엄격한 boolean 파싱 | 파싱 오류는 missing과 분리 |
| `runtime_minutes` | 양의 정수 | 파싱 오류는 missing과 분리한 보조 충돌 |
| `source_hash` | 서비스 값 존재 여부만 집계 | 레거시 해시와 알고리즘 계약이 달라 직접 동일성 판정에 사용하지 않음 |

각 필드 비교 결과는 `equal`, `conflict`, `legacy_only_value`, `service_only_value`, `both_missing`, `legacy_invalid`, `service_invalid`, `both_invalid`로 구분한다.

배열은 문자열 정규화 후 원래 순서를 보존하는 JSON의 SHA-256, 줄거리·포스터는 NFC·줄바꿈·공백 규칙을 적용한 값의 SHA-256으로 비교한다. 알고리즘 버전 `movie-baseline-reconciliation-v1`을 결과에 남긴다. 큰 텍스트와 URL 전체를 결과 문서에 노출하지 않는다.

기준선 범위 판단을 위해 각 배타적 분류별로 다음 집계를 추가한다.

- 개봉연도 분포, 개봉일 미상·파싱 오류 수
- 동일한 `movie-service-eligibility-v1` 정책의 적격 수와 제외 사유
- 서비스 공개·승인 상태와 논리 삭제 여부
- 특히 `service_only` 및 정책 제외 그룹 안의 현재 공개·승인 영화 수
- `movie_editorial`, `popcorn_movie_media`, `movie_category_links`, `popcorn_movie_embeddings`의 연결 영화 수와 고아 FK 수
- Gold current의 `singleton_matched`, legacy-only, service-only 연결 수와 Gold publication/source 식별자

## 5. 현재값과 소유권 계약 초안

대사 결과가 확정 전이므로 아래는 구현을 위한 기본 계약이며, 실데이터 결과와 검토를 통해 확정한다.

### 5.1 시각

- `source_observed_at`: 원천이 실제로 관측된 시각
- `ingested_at`: DW에 적재한 시각
- 레거시 파일은 신뢰 가능한 원천 관측시각이 없으므로 snapshot manifest 생성시각이나 DW 편입시각을 최신 원천시각으로 사용하지 않는다.
- 시각 미상의 기준선은 일일 API 관측보다 우선할 수 없다.

### 5.2 필드 상태

현재값 병합을 위해 속성은 값뿐 아니라 상태를 가진다.

- `PRESENT`: 원천이 명시적으로 제공한 값
- `NOT_COLLECTED`: 해당 원천/호출에서 수집하지 않은 값
- `MATCH_PENDING`: 보강 원천 매칭이 보류되어 확정할 수 없는 값
- `EXPLICITLY_CLEARED`: 원천 계약에 따라 삭제가 명시된 값
- `PARSE_ERROR`: 값은 존재하지만 계약 타입으로 해석할 수 없음

각 필드 관측은 source entity ID, source match decision과 규칙 버전, 값 상태, 관측시각, 시각 근거를 함께 가진다. 빈 문자열과 빈 배열을 일괄적으로 삭제로 해석하지 않는다. API별 계약이 없는 한 `NOT_COLLECTED`로 보고 이전의 **같은 확정 source 연결** 값을 유지한다.

- KMDb 매칭이 일시 보류되면 이전 유효한 확정 연결이 취소되지 않은 경우에만 그 연결의 보강값을 유지할 수 있다.
- 이전 연결이 오매칭으로 취소되거나 다른 KMDb entity로 교체되면 이전 entity의 보강값은 무효화한다.
- `EXPLICITLY_CLEARED`는 해당 필드를 지울 권한이 있는 원천과 삭제 증거가 있을 때만 허용한다.
- `PARSE_ERROR`를 PRESENT나 삭제로 바꾸지 않는다.
- 관측시각이 같고 값이 다르거나, 한쪽 시각이 미상인 경우 자동 우선순위를 적용하지 않고 충돌 상태를 남긴다.

### 5.3 소유권

- KOFIC ID, 기본 제목·개봉 정보: DW가 원천 관측과 병합 규칙을 관리한다.
- KMDb ID와 보강 필드: DW가 매칭 근거·규칙 버전과 함께 관리한다.
- 서비스 내부 ID, 공개·승인·논리삭제: PostgreSQL 서비스가 최종 소유한다.
- `movie_editorial`의 보정 줄거리와 삭제 상태: 서비스가 최종 소유하며 Reverse ETL이 덮어쓰지 않는다.
- 서비스 카테고리, 미디어 표시 순서, 임베딩 상태: 서비스 소유 관계를 보존한다.

## 6. 식별자 모델 계약 초안

### 6.1 원천 ID와 영화 연결

- `source_movie_identity` grain: `source_system × source_entity_type × normalized_source_id` 한 건. 원천 entity 자체를 나타내며 movie 연결을 포함하지 않는다.
- `bridge_movie_source_current` grain: `source identity × movie_key`의 현재 확정 연결 한 건.
- `bridge_movie_source_history` grain: `source identity × movie_key × valid_from` 한 건. `valid_to`, 연결 상태, 규칙 버전, 증거 hash를 보존한다.
- 한 KOFIC identity의 활성 확정 연결은 한 movie_key만 허용한다.
- 동일 KMDb identity가 여러 movie_key에 연결되는 현상은 우선 충돌·검수 대상으로 격리하고 자동 확정하지 않는다.
- 다수 후보는 확정 bridge가 아니라 별도의 `movie_source_match_observation`에 `source run × canonical movie × candidate source identity × rule version` grain으로 보존한다.
- 유효기간 중첩을 금지하고 병합·분리 시 기존 movie_key를 재사용하지 않고 명시적 대체 관계를 기록한다.

### 6.2 서비스 ID와 영화 연결

- `bridge_movie_service_current` grain: `service_system × service_movie_id` 한 건이며 현재 movie_key 하나를 가리킨다.
- `bridge_movie_service_history` grain: `service_system × service_movie_id × valid_from` 한 건이다.
- 현재는 `POP_TALK_POSTGRES × popcorn_movies.id`다.
- 하나의 활성 service movie는 하나의 stable `movie_key`를 가리키며 유효기간은 중첩될 수 없다.
- KOFIC ID 변경이나 매칭 수정이 발생해도 service movie ID와 stable movie key의 연결 이력을 유지한다.

## 7. 완료 조건

- 순수 대사 모듈의 단위 테스트 통과
- 실데이터를 읽기 전용으로 실행
- 총건수와 분류 합계가 양쪽 입력 건수와 대사됨
- KOFIC ID 중복·누락 수가 명시됨
- 교집합의 핵심 필드 충돌 수가 명시됨
- 분류별 연도·정책·공개·승인·논리삭제 분포가 명시됨
- 운영자 소유 관계의 연결 영화 수와 고아 FK 수가 기록됨
- 현재 Gold ID 집합과 publication/source 식별자를 포함한 3자 대사가 완료됨
- 결과를 토대로 레거시의 기준선 채택 범위와 다음 구현 단위를 확정
- 독립 검토 세션 승인

## 8. 비목표와 안전 경계

- 제목 유사도만으로 ID가 없는 영화를 자동 매칭하지 않는다.
- 레거시 또는 서비스 DB를 수정하지 않는다.
- 서비스의 공개 상태·관리자 보정값을 DW 값으로 덮어쓰지 않는다.
- 대사 결과가 나오기 전에 5,985건 전체를 Gold로 게시하지 않는다.
- 비밀번호·토큰·전체 줄거리·전체 URL을 보고서에 기록하지 않는다.

## 9. 실행 무결성과 출력 계약

- CLI는 임의 latest를 고르지 않고 정확한 legacy manifest key를 필수 인자로 받는다.
- manifest의 `schema_version`, `status`, `bucket`, `key`, `bytes`, `sha256`, `record_count`를 검증하고 그 manifest가 지정한 S3 객체만 읽는다.
- PostgreSQL은 `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY` 이후 하나의 트랜잭션에서 고정 SELECT·필요 컬럼만 조회한다.
- 결과에는 PostgreSQL DB/스키마, 추출시각, S3 digest, Gold publication/source 식별자, 비교 규칙 버전을 기록한다. 서로 다른 저장소를 동일 시점 snapshot이라고 표현하지 않는다.
- stdout JSON은 사전에 정의한 집계·hash·식별자 allowlist만 포함한다. 원천 행, DSN, Airflow connection extras, 토큰은 포함하지 않는다.
- stderr는 SDK/DB 예외 원문 대신 비밀값을 제거한 오류 코드와 단계만 기록한다.
- 로컬 기계 판독 결과는 Git에서 제외된 `.local/data-modeling/` 아래에 저장하고 파일에는 집계만 포함한다.
