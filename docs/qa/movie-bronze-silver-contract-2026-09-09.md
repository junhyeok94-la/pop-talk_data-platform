# Movie Bronze/Silver 변환 계약 1단계 구현 기록

구현일: 2026-09-09

## 범위

일일 Raw 수집의 `DAILY_READY → SUCCESS → stage manifests → raw envelopes`
계약을 검증하고, 네트워크·Airflow·Spark 의존성이 없는 결정적 변환 코어를
구현했다. 이 단계에서는 Databricks 테이블 생성, S3 쓰기, Snowflake 적재,
PostgreSQL 게시를 수행하지 않았다. 기존 수집 DAG와 collector도 수정하지 않았다.

## 구현

- 운영 소비는 `sample=false`만 허용한다. 명시적인 테스트 호출에서만
  `allow_sample=True`를 사용할 수 있다.
- READY, raw SUCCESS, 4개 stage manifest의 run/scope/status를 대사한다.
- 모든 raw reference와 실제 envelope 집합이 정확히 같아야 한다.
- canonical payload SHA-256을 envelope와 manifest 양쪽에서 검증한다.
- Bronze는 원문 payload JSON, request, source/resource, source key와 checksum을
  그대로 보존한다.
- Silver 영화는 KOFIC 문자열 ID를 유지하고 상세 필드를 정규화한다.
- KMDb 매칭은 정규화한 한글/영문 제목 완전 일치와 제작연도 허용 차이를
  사용한다. 응답 순서 대신 우선순위/연도차/외부 ID로 동률을 결정한다.
- 후보가 잘렸는데 매칭되지 않으면 `REVIEW_REQUIRED`, 완전 후보에서
  매칭되지 않으면 `UNMATCHED`다.
- 정책 제외 행도 삭제하지 않는다. `policy_eligible`, 제외 이유, 정책 버전을
  기록한다. v1은 개봉일 2020년 이후, 성인물/에로/다큐멘터리 및 기존
  회사/감독 제외 목록을 사용한다. 개봉일 미상은 추측하지 않는다.
- 박스오피스의 일일 관객과 누적 관객을 별도 필드로 보존한다.

## 검토 반영

`movie-pipeline-review-2026-09-09.md`의 P1/P2 세 항목을 반영했다.

- 실행 추적값을 content hash에서 제외하고 policy/matching/embedding hash를 분리했다.
- KMDb 후보 ID를 중복 제거하며, 동률 최상위 또는 truncated 결과는 확정하지 않는다.
- 운영 candidate scope 완료, 선언 건수, 영화 ID 집합, outcome 중복, boxoffice 날짜와
  원 요청까지 대사한다.
- 초기 snapshot byte/checksum/count 검증과 공통 Silver 어댑터를 추가했다.

## 검증

Airflow 3.3.1 이미지의 Python에서 실행:

```text
python -m unittest discover -s pipelines/transforms/tests -v
Ran 17 tests
OK
```

완료된 실제 일일 S3 입력을 읽기 전용으로 대사한 결과:

```json
{"source_run_id":"4834bb8200e5a76f2cf8b408","bronze_object_count":331,
 "silver_movie_count":107,"silver_boxoffice_count":70,"eligible_count":54,
 "excluded_count":53,"kmdb_matched_count":98,"kmdb_review_required_count":6,
 "kmdb_truncated_count":6,"kmdb_ambiguous_count":0,"kmdb_provisional_count":6}
```

재검토 문서 `movie-pipeline-rereview-2026-09-09.md`의 추가 P1/P2도 반영했다.
초기 어댑터는 실제 `movie_cd/movie_nm/open_dt/...` 스키마를 사용하고 서비스 PK를
생성하지 않는다. content hash는 명시적 업무 필드 allowlist만 사용하며 출처,
서비스 PK 매핑, 승인/노출 관측값을 제외한다. 초기/일일 어댑터의 동일 업무
콘텐츠 hash 일치 테스트를 포함한다.

실제 S3 초기 snapshot 전체 읽기 전용 변환 결과:

```json
{"movie_count":5985,"eligible_count":4320,"excluded_count":1665,
 "missing_kofic_id_count":0,"missing_title_count":0,
 "service_movie_id_non_null_count":0,"multi_genre_count":2270,
 "multi_director_count":246,"multi_actor_count":3329,"multi_company_count":2768}
```

## 검토 대상

- `pipelines/transforms/movie_bronze_silver.py`
- `pipelines/transforms/tests/test_movie_bronze_silver.py`
- `pipelines/transforms/README.md`
- `scripts/check_movie_transform.py`

## 다음 단계

검토 승인 후 이 코어를 Databricks notebook에 포함하고, Airflow가
`pop_talk_databricks` Jobs API로 실행하도록 연결한다. Free Edition에서 외부
S3 직접 접근이 제한되면 S3를 원본 기준 저장소로 유지하면서 Airflow가 검증된
manifest/object만 Databricks Volume으로 전달하는 bridge를 구현한다. 해당
전송 방식은 실제 기능 검증 전에는 확정했다고 기록하지 않는다.
