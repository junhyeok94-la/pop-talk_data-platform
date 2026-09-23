# 초기 영화 원천 수집 DAG

`pop_talk_movie_raw_initial`은 KOFIC(KOBIS) 영화 목록·상세, KMDb 검색 후보와 선택한 기간의 일별 박스오피스를 S3에 수집한다. `schedule=None`, `catchup=False`, 동시 실행 1개다. 처음 등록 시 paused이며 수동 실행 전에 unpause한다. 외부 API 호출은 파싱 시점이 아닌 태스크 실행 시점에만 발생한다.

## 기존 코드에서 가져온 책임

참조: `D:/01.DEV/pop_talk_project/pop_talk-product/pop_talk_batch/scheduler`의 `kofic_list.py`, `kofic_detail.py`, `kmdb_client.py`, `daily_sync.py`, `config.py`.

- KOFIC 목록: 개봉연도 범위와 페이지 순회, movieCd 기준 상세 호출.
- KMDb: 한글/영문 제목으로 후보 조회. 원본 제목과 검색 후보를 보관한다.
- 박스오피스: targetDt 기준 일별 응답. 전국 전체 상영 데이터가 아닌 해당 API의 상위 목록임을 기록한다.
- 기존 코드의 PostgreSQL 직접 적재, 원천 장르 삭제, staffs 제거, 즉시 최적 후보 매칭은 호출하지 않는다. 원천은 보존하고 제외 정책·영화 매핑은 Silver가 담당한다.
- 기존 코드는 오류를 빈 결과로 처리하거나 URL이 포함된 예외를 출력할 수 있어 그대로 import하지 않았다. API 오류와 검색 결과 없음은 구분한다.

## DAG 흐름

```text
plan_collection
  ├─ collect_movie_list → collect_details → collect_kmdb_candidates ─┐
  └─ collect_boxoffice ──────────────────────────────────────────────┤
                                                         validate_raw_run
```

최종 태스크까지 성공해야 `SUCCESS.json`이 생긴다. API 요청 실패·잘못된 응답·해시 불일치·목록 수 불일치는 태스크 실패이며 성공 manifest를 만들지 않는다. Airflow 태스크 상태/로그가 실패 기록이고, 실패한 실행의 원천 객체는 재시도를 위해 보존한다.

## 자격 증명

기존 `.env`에서 지정한 두 변수만 읽어 전용 Connection password로 등록한다.

```powershell
cd D:\01.DEV\pop_talk_dw_project
.\orchestration\airflow\scripts\register-movie-api-connections.ps1
```

| Connection | 용도 |
|---|---|
| pop_talk_aws | 기존 AWS Connection, S3 읽기·쓰기에 사용 |
| pop_talk_kofic | KOFIC_API_KEY → password |
| pop_talk_kmdb | KMDB_API_KEY → password |

등록 스크립트는 키를 명령 인자·임시 파일·출력으로 보내지 않고 stdin으로만 전달한다. 원본 env 전체나 DB 자격 증명은 컨테이너에 복사하지 않는다. CSV로 여러 키가 있으면 첫 키만 사용하며 자동 키 회전을 하지 않는다. 키 교체 후 등록 스크립트를 재실행하면 해당 Connection 두 개만 갱신한다. 메타 DB의 Fernet 설정을 사용하는 Airflow 저장 방식이다.

현재 기존 canary와 같은 버킷을 사용한다: `amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an`. 등록된 AWS 역할에는 아래 prefix에 대한 GetObject/PutObject가 필요하다. 비밀값은 DAG Params, XCom, manifest에 포함하지 않는다.

## 실행 범위

기본값은 **2026년 목록에서 API 순서상 10편**이며 전체·무작위·대표 표본이 아니다. 원본 목록 응답 한 페이지(최대 100행)는 그대로 저장하고 그중 선택한 영화만 상세·KMDb 수집을 진행한다.

```json
{
  "start_year": 2026,
  "end_year": 2026,
  "max_movies_per_year": 10,
  "max_pages_per_year": 100,
  "kmdb_max_pages": 3,
  "boxoffice_start": "2026-09-01",
  "boxoffice_end": "2026-09-08"
}
```

초기 전체 수집은 `max_movies_per_year=0`으로 하고 2020년부터 **한 연도씩** 실행하는 것을 권장한다. 날짜 범위는 2020년~실행 시작일의 한국 연도까지다. 영화 유형은 제한하지 않는다. 목록 API가 제공하는 개봉연도 조회 범위이며 개봉일 미상 영화·별도 재개봉 일정까지 전수 포괄한다고 주장하지 않는다.

`max_pages_per_year` 도달 전에 요청한 수집 범위를 채우지 못하면 실패한다. full 모드에서 partial success로 끝나지 않는다. 페이지 순회 중 보고 총수가 달라지거나 동일 연도에서 movieCd가 중복되면 새 실행을 요구한다. 이 검사는 API의 변경 가능한 페이지가 완전한 시점 스냅샷이라는 보장은 아니다.

KMDb는 제목마다 최대 `kmdb_max_pages × 100`개의 후보를 요청한다. 초과 결과는 `truncated=true`, `kmdb_truncated_movies`로 명시한다. 매칭하지 않으므로 후보 수집 성공이 영화 연결 성공은 아니다. 정상 0건만 `NO_RESULTS`로 기록하며 API 오류는 실패다. 잘못된/지나치게 넓은 제목은 Silver에서 후보 품질을 평가하고 재수집 범위를 정한다.

박스오피스 날짜 둘을 모두 비우면 생략한다. 지정하면 과거 날짜로 최대 31일만 수집한다. 당일 미완결 결과를 가져오지 않는다. 누적 관객·매출을 이후 날짜별 합산하면 안 된다.

HTTP timeout은 연결 10초/읽기 40초, 정상 호출 간 최소 0.4초다. 네트워크·5xx는 최대 3회 재시도하고, 429/인증/업무 오류는 태스크를 실패시킨다. Airflow 재시도는 5분부터 지수 증가, 최대 2회다. 일일 쿼터 소진이면 한도가 회복된 후 **같은 실행의 실패 태스크를 clear**해서 이어서 수집한다.

## 저장 및 후속 Silver 계약

```text
raw/movie_api/v1/run_id=<run-hash>/kofic/movie/searchMovieList/<request-hash>.json
raw/movie_api/v1/run_id=<run-hash>/kofic/movie/searchMovieInfo/<request-hash>.json
raw/movie_api/v1/run_id=<run-hash>/kmdb/search/<request-hash>.json
raw/movie_api/v1/run_id=<run-hash>/kofic/boxoffice/searchDailyBoxOfficeList/<request-hash>.json
manifests/movie_api/v1/run_id=<run-hash>/movie_list.json
manifests/movie_api/v1/run_id=<run-hash>/details.json
manifests/movie_api/v1/run_id=<run-hash>/kmdb_candidates.json
manifests/movie_api/v1/run_id=<run-hash>/boxoffice.json
manifests/movie_api/v1/run_id=<run-hash>/SUCCESS.json
```

run hash는 Airflow run_id와 수집 범위/collector 계약 버전에서 계산한다. 요청 hash는 credential을 제외한 소스·리소스·파라미터로 계산한다. 응답 envelope에는 원형 JSON payload, source, 수집 시각, 요청 범위, payload SHA-256이 포함된다. JSON 의미를 보존하는 방식이며 원 HTTP 바이트 그대로 저장하는 형식은 아니다. 서버가 비밀값을 반사한 경우는 마스킹한다.

각 S3 객체는 SHA-256 metadata와 조건부 PutObject로 기록한다. 같은 실행을 재시도하면 저장된 응답을 검증 후 재사용하고, 같은 키의 다른 내용을 덮어쓰지 않는다. 새 DAG run은 새 원천 스냅샷이며 이전 실행 캐시를 재사용하지 않는다. 서비스 DB 업데이트가 없는 수집이므로 DB ID 발급이나 게시 승인은 하지 않는다.

후속 DAG가 받을 XCom은 `{bucket, success_manifest_key}`다. 대량 원문·키는 XCom으로 넘기지 않는다. Silver는 SUCCESS manifest의 `stage_manifests`를 읽고 각 manifest의 `objects[].key`를 따라 payload를 로드한다. KOFIC 상세와 KMDb 후보의 연결은 `movie_cd`를 조사 키로 사용한다. 이것은 KMDb 영화와의 확정 매핑이 아니다. 표본 여부와 후보 잘림 여부를 Silver 품질 지표에도 전달해야 한다.

기존 `movie_sample_e2e`의 admin snapshot JSON 구조와 다르다. 기존 Databricks 샘플 노트북에 그대로 넣지 말고 이 envelope 전용 Bronze reader를 연결한다. 기존 `dw_serving.movie_catalog`나 사용자/관리자 테이블은 이 DAG에서 변경하지 않는다.

## 테스트

2026-09-09 실제 검증 완료:

- Airflow 3.3.1 DAG import 오류 0, 태스크 6개.
- 단위 테스트 14개 통과(페이지 처리·표본 범위·중간 실패 재개·원본 보존·체크섬·자격 증명 마스킹·쿼터·영문 영화 코드 포함).
- 실제 run `raw_smoke_20260909_0553`: 6개 태스크 모두 SUCCESS.
- KOFIC 2026년 보고 총수 1,540편, 첫 페이지 원본 100행 보관, 그중 1편 상세 수집.
- KMDb 검색 요청 2개, 중복 제거 후보 1건, 잘림 0건. 후보를 영화와 확정 매핑하지 않음.
- 2026-09-08 박스오피스 1일, 10행 원본 보관.
- S3 SUCCESS manifest: `manifests/movie_api/v1/run_id=c0649a0956e913d8c2794942/SUCCESS.json`.
- 검사 중 `2026A754` 형태의 실제 영화 코드를 발견해 숫자 전용 검증을 수정했다. 기존 실패 run을 clear하여 기존 원본과 완료 박스오피스를 재사용했고 최종 성공했다.
- 검증을 위해 신규 DAG만 unpause했다. 현재 수동 실행 가능하며 자동 schedule은 없다. 전체 연도 수집은 실행하지 않았다.

증빙: [실행 결과 JSON](../../.local/qa/docs/movie-raw-dag-smoke-2026-09-09.json).

Airflow UI에서 `pop_talk_movie_raw_initial` → Trigger를 선택하고 위 Params를 입력한다. 실패 후 같은 실행 재개는 해당 DAG run에서 실패 태스크를 Clear하거나, 이 버전 CLI의 run 선택을 사용한다.

```powershell
docker --config .docker-local compose exec -T airflow-scheduler airflow dags clear pop_talk_movie_raw_initial --run-id <재개할-run-id> -y
```

이 명령은 선택한 run의 6개 태스크를 다시 처리한다. 기존 응답·완료 stage manifest를 재사용하며 다른 DAG run에는 적용하지 않는다. collector의 API 계약이나 수집 의미를 바꾸면 VERSION을 올리고 새 run을 시작한다.

```powershell
docker compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests/pipelines/collectors -t /opt/airflow -v
```

Airflow DAG import도 실제 3.3.1 이미지에서 검사한다. 추가 런타임 패키지나 Compose 변경 없이 기존 orchestration/airflow/modules/pipelines/scripts 읽기 전용 mount를 사용한다.

참고: [KOFIC 제공 서비스](https://www.kobis.or.kr/kobisopenapi/homepg/apiservice/searchServiceInfo.do?serviceId=searchMovieList), [KMDb 가이드](https://www.kmdb.or.kr/info/api/guide2), [Airflow Params](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html). KMDb 가이드 웹 조회는 403이어서 기존 팀 코드와 실제 API 응답으로 계약을 교차검증한다.
