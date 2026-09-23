# 영화 기준선 대사 v1 실데이터 결과

- 실행일: 2026-09-10
- 규칙 버전: `movie-baseline-reconciliation-v1`
- 정책 버전: `movie-service-eligibility-v1`
- 실행 성격: S3, PostgreSQL, Snowflake 읽기 전용
- 기계 판독 결과: `.local/airflow-workbench/data-modeling/movie-baseline-reconciliation.json`

## 1. 입력

| 입력 | 행 수 | 식별 근거 |
|---|---:|---|
| S3 legacy snapshot | 5,985 | SHA-256 `7e0e303f...52a651fc`, manifest의 bytes/hash/count 검증 |
| PostgreSQL `dev.popcorn_movies` | 5,319 | 단일 `REPEATABLE READ READ ONLY` 트랜잭션 |
| Snowflake `DW.DIM_MOVIE` current | 118 | 각 영화 ID와 선택된 source run×artifact 계보를 동일 SELECT 행에서 추출 |

서로 다른 저장소의 추출 시각은 결과 JSON에 별도로 기록했으며 동일 시점 snapshot으로 간주하지 않았다.

## 2. KOFIC ID 대사

| 분류 | Legacy 행 | Service 행 |
|---|---:|---:|
| 1:1 일치 | 5,309 | 5,309 |
| 해당 원천에만 존재 | 676 | 10 |
| missing | 0 | 0 |
| invalid | 0 | 0 |
| duplicate ambiguous | 0 | 0 |

각 원천의 분류 합은 입력 행 수와 정확히 일치한다. casefold 충돌도 없다.

이 결과는 두 데이터셋이 5,309개의 동일 KOFIC ID와 동일한 핵심 속성을 공유한다는 사실을 보여준다.

```text
Legacy와 Service가 공유하는 영화 5,309
+ Service에만 관측된 영화 10
= 현재 서비스 영화 5,319
```

서비스 전용 10건은 모두 `DRAFT/PENDING`이고 현재 `source_system=KOFIC_KMDB`로 기록돼 있다. 8건은 2026년 영화이며 2건은 각각 1995년·2018년 영화다. 이 집계만으로 레거시보다 나중에 수집됐다고 단정하지는 않는다.

## 3. 동일 영화의 필드 비교

5,309건의 1:1 교집합을 비교했다.

| 필드 | 주요 결과 |
|---|---|
| 한글 제목 | 5,309건 모두 동일 |
| 개봉일 | 5,309건 모두 동일 |
| 제작연도 | 값이 있는 5,292건 모두 동일, 양쪽 미상 17건 |
| 러닝타임 | 값이 있는 5,301건 모두 동일, 양쪽 미상 8건 |
| KMDb ID | 동일 4,869, 양쪽 미상 246, 서비스에만 값 194 |
| KMDb 매칭 여부 | 동일 5,115, 차이 194 |
| 제작사 | 동일 5,304, 차이 2, 양쪽 미상 3 |
| 국가 | 동일 5,292, 차이 1, 레거시에만 값 16 |
| 줄거리 hash | 동일 4,848, 차이 1, 서비스에만 값 194, 양쪽 미상 266 |
| 대표 포스터 hash | 동일 4,037, 차이 8, 서비스에만 값 180, 양쪽 미상 1,084 |

서비스에만 KMDb ID가 있는 영화는 194건, 줄거리도 194건, 대표 포스터는 180건이다. 필드별 집계이므로 세 집합이 모두 같다고 단정할 수 없고, 수집 시점도 입증하지 않는다. 다만 서비스 쪽에만 보존된 값이 실제로 있으므로 레거시 행 전체로 서비스 속성을 덮으면 안 된다는 결론은 유효하다.

## 4. 정책 대사

### 4.1 Legacy

- 적격 4,320건
- 제외 1,665건
- Legacy 전용 676건은 모두 제외 대상
- 제외 사유는 중복 가능: 회사 687, 개봉일 2020년 이전 570, 장르 417, 감독 62

### 4.2 Service

- 적격 4,329건
- 제외 990건
- 공개·승인된 영화 가운데 현행 정책 제외 대상이 987건
- 서비스 전용 10건 중 적격 7건, 제외 3건

교집합 정책 판정은 5,307건이 일치하고 2건이 다르다. KOFIC `20264148`, `20264879`는 레거시에 제외 장르가 있으나 서비스 값에는 해당 사유가 없다.

이 결과로 다음을 확정한다.

- `policy_eligible`은 분석 속성으로 유지한다.
- 기존 공개 영화 987건을 새 정책만으로 자동 숨기지 않는다.
- 사용자 공개 여부는 정책 판정과 서비스 승인·공개·논리삭제를 분리해 결정한다.

## 5. 현재 Gold와의 3자 대사

| Gold 분류 | 행 수 |
|---|---:|
| Legacy와 Service 양쪽에 연결 | 28 |
| Service에만 연결 | 3 |
| Legacy·Service 어디에도 없음 | 87 |
| missing/invalid/duplicate | 0 |

현재 Gold 118건은 일일 API의 최근 관측 범위이므로 레거시 기준선과 교집합이 작아도 오류가 아니다. Gold-only 87건은 현재 API 관측에 있지만 두 기존 집합에는 없다는 뜻이다. 스냅샷 이후 신작일 수도 있고 수집 범위 차이일 수도 있으므로 별도 시각 증거 없이 신규 영화로 단정하지 않는다.

## 6. 서비스 소유 관계의 보존 영향

| 관계 | 행 수 | 연결 영화 수 | 고아 행 |
|---|---:|---:|---:|
| `movie_editorial` | 0 | 0 | 0 |
| `movie_category_links` | 0 | 0 | 0 |
| `popcorn_movie_media` | 52,082 | 4,265 | 0 |
| `popcorn_movie_embeddings` | 5,310 | 5,310 | 0 |

현재 운영자 보정과 카테고리 연결은 없지만 미디어와 임베딩은 이미 대규모로 연결되어 있다. 후속 Reverse ETL은 기존 movie ID를 유지해야 이 관계를 보존할 수 있다.

## 7. 기준선 제안

### 구현 제안

- Legacy 5,985건 전체를 **초기 원천 관측 기준선**으로 채택할 것을 제안한다.
- 정책 제외 1,665건도 Silver/DW 관측에서는 삭제하지 않고 제외 속성과 사유를 보존한다.
- 일일 KOFIC/KMDb 관측은 별도 최신 관측 갱신원으로 사용한다.
- 서비스 영화는 원천 데이터의 최신 정답으로 사용하지 않고, service ID·공개 상태·승인·운영 관계의 소유 원천으로 사용한다.

### 병합 경계

- Legacy가 현재 Service에만 존재하는 KMDb 보강값 194건을 덮지 않는다.
- 최신 API 관측이 KMDb 매칭 보류 또는 미수집이면 이전의 같은 확정 연결 보강값을 유지한다.
- KMDb 연결이 취소·교체되면 이전 entity의 보강값은 무효화한다.
- 기존 서비스 공개 영화는 현행 정책만으로 자동 비공개하지 않는다.
- Service 전용 10건은 ID bridge에서 보존하고, 그중 현재 API Gold에 있는 행은 API 관측과 연결한다. 나머지는 provenance가 추가 확인될 때까지 서비스 소유 행으로 유지한다.

## 8. 다음 구현 단위

다음 단계는 **전체 영화 observation 편입과 식별자 bridge v1**이다.

1. Legacy manifest를 Databricks 변환 입력 Asset으로 연결한다.
2. Legacy 5,985건을 공통 Silver movie observation에 적재한다.
3. 기준선과 일일 API를 source observation으로 함께 유지한다.
4. 행 전체 최신 선택을 필드별 현재값 선택 모델로 교체한다.
5. `source_movie_identity`, source current/history bridge를 구현한다.
6. `bridge_movie_service_current/history`를 구현해 PostgreSQL 5,319건을 연결한다.
7. 총건수·ID·정책·필드 provenance 테스트를 함께 추가한다.
