# 기존 데이터셋과 일일 원천 수집

초기 적재는 기존 `pop_talk-local_dev/batch/initial_dataset/data/movies_final.json`을 그대로 보관한다. 기존 서비스 PostgreSQL은 이미 복원되어 있으므로 재복원하지 않는다. 초기 JSON은 5,985건으로, 현재 서비스 DB의 승인·정제된 영화 수와 다를 수 있다.

2026-09-09 실제 업로드 및 전체 바이트 readback 검증 완료:

- Bucket: `amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an`
- SHA256: `7e0e303fa1224ea15d864b90c0f98677089c3fddd3dc7e709f6785cf52a651fc`
- 12,553,297 bytes, 5,985 records
- 원본: `raw/legacy_snapshot/v1/sha256=7e0e303fa1224ea15d864b90c0f98677089c3fddd3dc7e709f6785cf52a651fc/movies_final.json`
- 완료: `manifests/legacy_snapshot/v1/sha256=7e0e303fa1224ea15d864b90c0f98677089c3fddd3dc7e709f6785cf52a651fc/SUCCESS.json`

재현: 원본 파일을 ReadAllBytes한 후 Base64 문자열을 `docker --config .docker-local compose exec -T airflow-scheduler python /opt/airflow/scripts/upload_legacy_snapshot.py` stdin으로 전달한다. 내용 해시 주소와 조건부 PUT으로 동일 파일 재실행 시 덮어쓰지 않고 기존 바이트를 검증한다. API 키나 AWS 키를 인자로 전달하지 않는다.

## 일일 DAG

`pop_talk_movie_raw_daily`, KST 매일 03:00, catchup=False, 동시 실행 1개. Connection은 `pop_talk_aws`, `pop_talk_kofic`, `pop_talk_kmdb`를 사용한다.

`plan_collection → collect_candidates → collect_details → collect_kmdb → validate_daily`

- 기존 `scheduler/daily_sync.py`의 최근 7일 박스오피스 + 당해연도 장편영화(`220101`) 첫 페이지 100건을 합집합으로 수집한다. 박스오피스 우선, 영화 코드는 문자열로 유지한다.
- 원본은 제외 정책 적용 전에 보존한다. 장르 제외·매칭·정규화는 후속 Silver 단계의 책임이다. 기존 PostgreSQL 직접 UPSERT 코드와 KMDb 휴리스틱 매칭 코드를 이 raw DAG에서 실행하지 않는다.
- KMDb 원본 후보는 한글/영문 제목으로 각각 최대 3페이지 수집한다. 기존 감독명 추가 검색·제목/연도 매칭은 후속 변환에 이식할 대상이다. 원천 보존 단계에서 잘못된 매칭을 확정하지 않는다.
- 원본 경로: `raw/movie_api_daily/v1/collection_date=YYYY-MM-DD/run_id=.../`.
- 완료 경로: `manifests/movie_api_daily/v1/collection_date=YYYY-MM-DD/run_id=.../DAILY_READY.json`.
- `validate_daily` 성공은 위 READY를 게시한 뒤 같은 URI의 Airflow Asset event를 발행한다.
  event에는 bucket, READY key, 업무일, Raw run ID와 생산 Airflow run ID만 넣으며 비밀값이나
  원천 본문은 넣지 않는다. 성공 태스크 clear 등으로 중복 event가 생길 수 있으므로 후속
  DAG가 READY identity로 중복 제거한다.
- 같은 실행 재시도는 이미 저장한 응답을 재사용한다. 다른 실행은 새 경로에 쌓인다. XCom은 계획과 S3 경로만 전달한다.
- `max_movies=0`은 일일 후보 전체, 양수는 검증용 제한이다. `DAILY_READY.sample=true`는 후속 운영 적재 대상에서 제외한다. `candidate_scope_complete`는 일일 후보 범위 완료만 의미한다. `catalog_complete=false`는 전체 영화 카탈로그를 수집하지 않았음을 명시한다.
- `SUCCESS.json`의 `kmdb_truncated_movies`와 상세 후보 manifest의 `truncated`를 후속 매칭 품질 판단에 반영한다. API 실패는 NO_RESULTS로 숨기지 않는다.

## 후속 연결 계약

일일 DAILY_READY Asset event를 발행하지만 현재 이를 소비하는 분석 적재 DAG는 없습니다. Phase 1에서 Python → PostgreSQL STG → dbt DW/Mart → 품질 검사 → 서비스 게시 경로를 새로 구현합니다.

초기 snapshot은 아직 이 일일 Asset 자동 연결 범위가 아니며 별도 backfill 입력으로 관리한다.
원본 보관과 downstream 반영 완료는 계속 구분한다. 게시/승인 상태는 배치가 강제로 바꾸지
않고, downstream은 처리한 manifest와 source hash를 기록해 재실행 중복을 막는다.

최근 7일+첫 페이지 방식은 과거 영화의 모든 변경을 포착하는 CDC가 아니다. 당해연도 후속 페이지나 오래된 영화 수정의 보정이 필요하면 별도 정기 보정 실행을 추가한다. 기존 `pop_talk_movie_raw_initial`은 그때 사용할 수 있는 수동 보충 수집 도구이며 초기 전체 재수집을 자동 실행하지 않는다.

검증: 기존 raw 테스트 14개 + 일일 범위/연도 경계/합집합/샘플/재시도 테스트 3개 통과. Airflow 3.3.1 DAG 파싱 통과.

2026-09-09 첫 정규 실행 `scheduled__2026-09-08T18:00:00+00:00`의 원천 수집 및 완료 manifest 검증 성공. 일일 후보 107건, sample=false, candidate_scope_complete=true. KMDb 후보 상한 초과 6건은 truncated로 표시되어 후속 추가 조회가 필요하다.

완료 경로: `manifests/movie_api_daily/v1/collection_date=2026-09-09/run_id=4834bb8200e5a76f2cf8b408/DAILY_READY.json`.
