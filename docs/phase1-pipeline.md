# Phase 1 PostgreSQL 파이프라인

## 반영 상태

- `dw_control` 테이블 17개·시퀀스 3개를 재검사 후 삭제했습니다. 외부 의존성 차단에 RESTRICT를 사용했고,
  백업은 Git 제외 경로 `.local/qa/phase1-dw-control-before.sql`에 보관합니다. 공유 DB 역할은 삭제하지 않았습니다.
- Airflow 시스템 71개와 Workbench 13개를 보존했습니다. 대시보드 공용 저장소는 변경하지 않습니다.
- 플랫폼 `pop_talk_platform`에 ops 2개·STG 3개·DW 5개 테이블과 mart 3개 뷰를 사용합니다.
  Airflow와 중복인 `pipeline_runs`는 제거했으며 실행 이력은 Airflow에만 기록합니다.
  기존 초기 영화 JSON 5,985건을 실제 Airflow DAG로 적재했습니다. 테스트 fixture는 업무 테이블에 넣지 않았습니다.
- 실제 서비스 DB는 중지된 상태입니다. 서비스 소스의 DDL을 격리 DB에 적용해 게시 계약을 검사했지만,
  실제 서비스의 migration 적용 여부와 운영 데이터는 아직 확인하지 않았습니다.

## 실행

```powershell
python scripts/configure.py
docker compose build airflow-init
docker compose up -d --no-build
```

설정 생성기는 기존 값을 유지하고 누락된 플랫폼 실행 계정만 추가합니다.
관리자 계정은 init에만 전달하고, `platform_pipeline`은 ops 기록·STG INSERT/SELECT·DW/Mart 생성 권한을 갖습니다.
Airflow 메타데이터 DB와 분석 DB의 계정·테이블을 분리합니다.
dbt-core 1.12.5 / dbt-postgres 1.11.0은 별도 가상환경에 설치해 Airflow 의존성을 변경하지 않습니다.

### 영화·흥행

1. `pop_talk_environment_check`로 파일·실행 계정의 플랫폼 DB 접근을 확인합니다.
2. 기존 수집 DAG를 실행하려면 AWS/KOFIC/KMDb Connections를 구성합니다.
3. `pop_talk_warehouse`는 일일 수집의 READY Asset을 소비합니다. 기본 일시정지이며, 자동 실행에는 unpause가 필요합니다.
4. 수동 실행은 아래 Params 중 하나만 지정합니다.
   - `ready_manifest_key`: `manifests/movie_api_daily/v1/collection_date=YYYY-MM-DD/run_id=<24hex>/DAILY_READY.json`
   - `initial_success_manifest_key`: `manifests/movie_api/v1/run_id=<24hex>/SUCCESS.json`
   - `initial_snapshot_sha256`: `.local/data/initial/<sha256>/`에 보존한 기존 영화 JSON과 manifest의 해시.
     기존 초기 파일에는 원본 관측 시각이 없으므로 NULL로 보존하며, 시각이 있는 API 관측을 우선합니다.
5. 기본 `publish_service=false`는 STG 적재와 dbt 검사까지 수행합니다.

초기 수집은 `max_movies_per_year=0`이며 모든 지정 연도가 완전 수집된 경우만 허용합니다.
초기 수집을 다른 저장 형식으로 억지 변환하지 않고, 초기 SUCCESS의 완전성 조건을 검사한 뒤 공통 정제를 적용합니다.
Raw 원문은 S3에 남기고 STG에는 정제 관측값·S3 위치·hash·원본 시각을 저장합니다.
표본 수집은 운영 적재에 사용하지 않습니다. 일일 후보는 최근 흥행과 올해 장편 중심이며 전체 카탈로그 CDC가 아닙니다.

### 리뷰

`pop_talk_reviews_warehouse`는 수동 DAG입니다. `.local/config/airflow.env`에 별도 읽기 전용 계정의
`POP_TALK_REVIEW_POSTGRES_HOST`, `PORT`, `DB`, `USER`, `PASSWORD`를 설정한 뒤 컨테이너를 재생성합니다.
계정에는 서비스 `dev.reviews`의 조회 컬럼과 `dev.popcorn_movies(id,kofic_movie_cd)` 조회 권한만 부여합니다.
서비스 역할·DDL 관리는 애플리케이션 저장소에서 수행합니다.

REPEATABLE READ / READ ONLY 전체 스냅샷으로 수정·숨김·논리 삭제·물리 삭제를 반영합니다.
100,000건 초과 시 일부만 성공 처리하지 않고 실패합니다. 확장 시 일관된 스냅샷과 페이지 추출을 먼저 구현합니다.
리뷰 본문·회원 ID·외부 작성자 ID·비밀번호·대화는 복제하지 않습니다.
내부 출처는 `internal:pop_talk`, 외부는 `external:<source_system>`으로 구분합니다.
현재 서비스는 `ACTIVE/HIDDEN`, 0.5~5점·0.5 단위입니다. 외부 원래 작성일은 알 수 없어 NULL을 유지합니다.
매핑 대상 영화가 DW에 없으면 리뷰를 버리지 않고 품질 검사에서 실패합니다. 먼저 영화 초기 적재를 수행합니다.

### 서비스 게시

`POP_TALK_POSTGRES_*` 게시 계정과 서비스 migration 016을 확인한 뒤,
영화 DAG 수동 실행의 `publish_service=true`를 사용합니다. 기본 자동 실행은 게시하지 않습니다.
서비스 스키마는 `dev`, 서비스 시스템은 `pop_talk` 한 개를 지원합니다.

dbt 전체 build/test가 통과해야 게시 입력을 동결합니다. 기존 `popcorn_movies` ID·승인/노출 상태·
editorial·media를 유지하며 영화 원천 속성과 일별 흥행만 게시합니다.
플랫폼 요청과 서비스 성공 이력은 서로 다른 DB 트랜잭션입니다. 응답 유실 시 동일한 입력·cutoff의
`batch_runs` 성공 기록으로 복구합니다. 이미 더 최신 게시가 성공한 경우에도 과거 성공 기록 조회는 안전하며,
성공한 적 없는 과거 게시의 역전 적용은 거부합니다.

## 데이터 기준과 복구

- 같은 URI+loader version은 한 번만 적재합니다. 동일 identity의 내용 변경은 실패합니다.
- STG와 적재 장부는 같은 트랜잭션입니다. 실패 후 영화 일부만 남거나 성공 장부만 남지 않습니다.
- 최신 원본 관측 시각을 선택하므로 늦게 들어온 과거 파일이 최신 값을 덮지 않습니다.
  최신 관측 시각에 서로 다른 업무 값이 있으면 dbt 검사에서 차단합니다.
  이후 시각의 정상 관측을 다시 수집하면 이전 충돌 원본을 보존하면서 복구할 수 있습니다.
- 모든 적재와 dbt build/게시가 같은 PostgreSQL advisory lock을 사용합니다. 겹치면 대기 대신 Airflow 재시도합니다.
- DW 모델은 현재 Phase 1 규모에서 전체 재계산합니다. 증분 갱신·분산 실행 제어는 추가하지 않았습니다.
- dbt 모델 교체 자체가 전체 모델에 걸친 단일 트랜잭션은 아닙니다. 실패 시 일부 분석 모델은 갱신될 수 있지만
  서비스 게시 단계는 차단됩니다. 문제 원천을 수정된 새 manifest로 적재하거나 원인을 수정한 후 build를 재실행합니다.
- 실행 상태·재시도·실패 로그는 Airflow에서 확인합니다. `ops`에는 원본 적재 장부와 서비스 게시 복구 기록만 유지합니다.
- 리뷰 재시도는 같은 Airflow run의 저장된 스냅샷을 재사용합니다. 새 원천 상태는 새 run으로 수집합니다.
- dbt 산출물은 `orchestration/airflow/logs/phase1-dbt/<uuid>`의 console.log·manifest·run_results에 보관합니다.
  스케줄을 운영하기 전 로그·STG 보존 기간과 백업 정책을 결정해야 합니다.

박스오피스 상위 목록에 없는 날짜를 0으로 보정하지 않습니다. `observed_daily_audience/sales`는 관측 기간 합계이며
전체 시장 매출이 아닙니다. 누적 관객은 가장 최근 대상일의 값이며 날짜별 합산하지 않습니다.
`movie_performance`는 영화·리뷰 출처별 행이므로 출처별로 반복된 흥행 지표를 다시 합산하면 안 됩니다.
흥행 관측일과 리뷰 관측일은 각각 별도 컬럼입니다.

## 검증

```powershell
docker compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests/pipelines -t /opt/airflow -q
docker compose exec -T airflow-scheduler python /opt/airflow/tests/check_airflow_dag_contracts.py
./scripts/check-phase1.ps1
```

통합 검사는 임의 이름의 `phase1_test_<uuid>` DB를 만들고 마지막에 삭제합니다. 실제 서비스 DB를 건드리지 않습니다.
검증 범위: 멱등성·입력 변조·적재 롤백·동시 실행 차단·과거 관측 재처리·같은 시각의 충돌·
초기 수집 완전성·미매핑 게시 차단·리뷰 숨김/삭제·게시 응답 유실 복구·서비스 ID/상태 보존.
추가로 애플리케이션 DDL 001·009·016을 적용한 별도 DB에서 기존 서비스 게시 통합 검사 3개가 통과했습니다.
이는 실제 운영 서비스의 연결·마이그레이션 검증을 대체하지 않습니다.

다음 운영 단계는 AWS/API Connections 등록과 최신 흥행 수집, 실제 서비스 DB의 카탈로그·migration 016 확인,
읽기/게시 계정 연결, 리뷰 스냅샷 적재, 마트 검토 후 서비스 게시입니다.

## 최초 데이터 적재 결과 — 2026-09-23

- DAG/run: `pop_talk_warehouse` / `manual__phase1_initial_snapshot_20260923`.
  resolve_inputs → load_stg → build_and_publish 모두 첫 시도 성공. 완료 후 다시 일시정지했습니다.
- 원본: 기존 `pop_talk-local_dev/batch/initial_dataset/data/movies_final.json`, 12,553,297 bytes.
- SHA-256: `7e0e303fa1224ea15d864b90c0f98677089c3fddd3dc7e709f6785cf52a651fc`.
  원본 바이트와 manifest는 `.local/data/initial/<sha256>/`에 보존하며 컨테이너에는 읽기 전용 마운트합니다.
- STG 영화 5,985건 → DW 영화 5,985건 → `mart.movie_performance` 5,985건, 날짜 차원 4,278건.
- 정책 적격 4,320건 / 제외 1,665건. 제외 표시를 보존하며 DW에서 영화 자체를 삭제하지 않았습니다.
- 원본 관측 시각은 파일에 없어 NULL입니다. `ops.source_loads.loaded_at`은 실제 적재 시각입니다.
- 흥행·리뷰는 이 파일에 없어 0건입니다. 서비스 게시도 0건입니다.
- `pipeline_runs` DDL·기록 래퍼·실제 테이블을 제거했고 초기화 후에도 재생성되지 않음을 확인했습니다.
  기존 검사 기록 백업은 `.local/qa/pipeline-runs-retired.sql`에 있습니다.
- 일반 테스트 70개와 격리 DB 통합 검사 8개, DAG 계약 검사가 통과했습니다.
  실제 적재 DAG에서도 dbt 모델 8개·품질 검사 19개가 통과했습니다.
