> 저장소 분리(2026-09-23) 전의 설명입니다. 현재 설치·DB 분리·연결 방법은 저장소 루트 README.md를 우선합니다.

# Pop Talk Airflow 3.3.1 local orchestration

DAG 작성과 검토에는 [DAG 개발 표준](DAG_DEVELOPMENT_STANDARD.md)을 적용합니다. DAG 파일은
전체 실행 흐름과 태스크 계약을 한글로 설명하고, 실제 처리 로직은 Airflow 없이 테스트할 수
있는 `orchestration/airflow/modules/pipelines/` 모듈에 둡니다. 함수·태스크·Connection ID는 실행 호환성을 위해 영어를
사용합니다.

이 구성은 기존 Pop Talk PostgreSQL 컨테이너를 공유하지만 Airflow 메타데이터는
별도 `airflow` 데이터베이스와 `airflow_user`에 저장합니다. 서비스 데이터베이스의
스키마와 Airflow 테이블은 섞이지 않습니다.

## 준비 및 실행

PowerShell에서 한 번 실행합니다.

```powershell
.\orchestration\airflow\scripts\prepare-airflow.ps1
docker compose --profile airflow build
docker compose --profile airflow up airflow-init
docker compose --profile airflow up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

UI는 <http://localhost:8080>에서 열립니다. 최초 로그인 정보는
`.local/config/airflow.env`의 `_AIRFLOW_WWW_USER_USERNAME`과
`_AIRFLOW_WWW_USER_PASSWORD`에 있습니다.

**왼쪽 사이드바 → 대시보드 / Model Lab**에서 계정별 커스텀 대시보드와 모델 실험 화면을
사용할 수 있습니다. 별도 Grafana 서비스 없이 ECharts·GridStack·CodeMirror를 플러그인에
포함한 Dashboard Studio에서 쿼리·그래프·드래그 배치·저장 이력을 편집합니다. 모델 비교, 임베딩 평가,
학습 레시피와 환경별 원격 Job API/Kubernetes 실행기의 자원·DAG 계약 검사를 제공합니다.
대시보드·프리셋·실험·환경 설정과 워커 작업 이력은 Airflow PostgreSQL의 `workbench`
스키마에 저장합니다. 모델 워커는 기존 인증키로 플러그인 상태 API를 호출하며 DB 계정을
따로 만들거나 메타 DB에 직접 접속하지 않습니다. 학습 데이터·가중치·산출물은 실행 대상의
파일/객체 저장소가 담당합니다.
재사용·권한·학습 실행기 연결 방법은 [플러그인 안내](plugins/README.md)를 참고하세요.

로컬 개발 환경에서는 Connection 편집 화면의 `Test` 기능과 Admin의 Config
화면을 노출합니다. Config 화면의 민감한 값은 Airflow가 마스킹합니다. 외부에
공개하는 운영 환경에서는 Connection Test를 비활성화해야 합니다.

첫 검증은 일시 정지된 `pop_talk_environment_check` DAG를 활성화한 후 수동으로
실행합니다. 이 DAG는 프로젝트 파일 마운트와 기존 서비스 PostgreSQL 연결만
검증하며 데이터는 변경하지 않습니다.

`pop_talk_s3_canary` DAG는 `pop_talk_aws` Connection을 사용해 개발 버킷과
`raw/`, `manifests/`, `exchange/` prefix를 읽기 전용으로 검사합니다. S3 객체를
생성하거나 삭제하지 않습니다.

Airflow 작업용 Python 패키지와 dbt 패키지는 이미지 내부에서 분리되어 있습니다.
dbt 명령의 경로는 `/opt/dbt-venv/bin/dbt`입니다. 이렇게 하면 dbt 어댑터가
Airflow 자체의 암호화·통신 패키지 버전을 변경하지 않습니다.

Airflow 컴포넌트만 중지하려면 다음을 실행합니다. 기존 서비스와 PostgreSQL은
중지하지 않습니다.

```powershell
docker compose stop airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

## 데이터베이스 경계

- `pop_talk_local`: 애플리케이션 서비스 데이터 (`dev`)
- `airflow`: DAG 정의를 제외한 실행 이력, 태스크 상태, 연결 메타데이터
- `airflow.workbench` 스키마: 플러그인 개인 대시보드·실험·실행 환경·워커 작업/예약

Airflow DAG 파일과 프로젝트 코드는 파일시스템/Git에 유지합니다. Airflow 메타
DB는 DAG 소스 코드를 저장하는 저장소가 아닙니다.

## 코드 품질 검사

현재 명령과 검사 범위는 [테스트 안내](tests/README.md)를 참고합니다.

## 마운트와 설정

| 호스트 경로 | 컨테이너 경로 | 역할 |
|---|---|---|
| `orchestration/airflow/dags` | `/opt/airflow/dags` | DAG 정의 |
| `orchestration/airflow/plugins` | `/opt/airflow/plugins` | Workbench 플러그인 |
| `orchestration/airflow/modules` | `/opt/airflow/modules` | 공통 처리 모듈, 읽기 전용 |
| `orchestration/airflow/config` | `/opt/airflow/config` | 공유 설정, 읽기 전용 |
| `orchestration/airflow/scripts` | `/opt/airflow/scripts` | 관리 명령, 읽기 전용 |
| `orchestration/airflow/tests` | `/opt/airflow/tests` | 개발 검증, 읽기 전용 |
| `orchestration/airflow/logs` | `/opt/airflow/logs` | 실행 로그 |
| `.local/data/airflow-workbench` | `/opt/airflow/workbench` | 평가 산출물 |
| `.local/data/dbt-deployments` | `/opt/airflow/dbt-deployments` | 불변 dbt 실행 스냅샷, 읽기 전용 |
| `.local/data/dbt-attempt-artifacts` | `/opt/airflow/dbt-attempt-artifacts` | dbt 실행 증빙 |
| `.local/config/snowflake` | `/keys` | 로컬 키, 읽기 전용 |

`PYTHONPATH`에 `modules`를 포함하므로 DAG는 `pipelines.*`를 직접 import합니다.
`POP_TALK_PROJECT_ROOT`는 공통 모듈의 기준 디렉토리, `POP_TALK_DBT_DEPLOYMENT_ROOT`는
불변 실행 스냅샷 위치입니다. 두 경로를 분리해 실행 코드를 읽기 전용으로 유지합니다.

`config/model-lab-profile.json`은 Pop Talk 평가 자료를 등록하는 프로젝트 프로필입니다.
기존 프로젝트를 단순히 재시작할 때 다시 등록하지 않습니다. `config/model-lab-local.json`은 로컬 실행 환경 예시입니다.
실제 비밀번호·토큰은 루트 `.env`와 `.local/config/*.env`에서 관리합니다.

새 설치에서는 dbt 소스를 준비한 뒤 `scripts/deploy_dbt_cosmos_artifact.py --help`의
옵션에 따라 `.local/data/dbt-deployments`에 검증된 실행 스냅샷을 마련해야 모델 DAG를 파싱할 수 있습니다.
기존 설치는 정리 전 스냅샷과 current 포인터를 그대로 사용합니다.

## DW에서 서비스 화면까지

현재 연결은 `원천 S3 → Databricks → Snowflake STAGING → dbt 모델 → PostgreSQL dev → WAS → FE`입니다.
일일 운영 DAG는 `pop_talk_movie_raw_daily`, `pop_talk_movie_databricks_daily`, `pop_talk_dw_serving`의 3개입니다.
`pop_talk_movie_databricks_daily`는 원천 적재 확인 후 `pop_talk_dw_serving`에 인계합니다.
따라서 이 일일 DAG의 성공은 적재와 인계 완료를 뜻하며, 최종 서빙 완료는
`pop_talk_dw_serving`의 성공 상태에서 확인합니다. 기존 일일 DAG에서 Gold를 중복 계산하지 않습니다.

DW DAG는 `prepare → dw(DbtTaskGroup) → quality_checks → publish` 구조입니다.
`dw`를 펼치면 `models/staging`, `models/dw`, `models/mart` 그룹 안에서 실제 모델 7개의 실행 상태를 볼 수 있습니다.
Cosmos가 고정 manifest의 ref 의존성을 연결하며 각 모델은 기존 실행별 입력·출력 격리 함수를 사용합니다.
각 모델에 별도 실행 디렉터리를 사용하므로 병렬 실행 로그가 섞이지 않습니다.
전체 검사 43개는 마지막 품질 태스크에서 실행하며 전부 통과한 뒤에만 서비스에 반영합니다.
모델별 dbt 프로세스 시작 비용은 추가되지만 실패한 모델을 개별 재시도할 수 있습니다.
같은 시점의 원천 테이블 3개를 고정하고 실행별 출력 테이블을 사용하며, 검사 43개가 모두 통과해야 게시합니다.
게시 실패는 기존 활성 데이터를 유지하고, 입력 고정 시각이 오래된 실행은 최신 서빙을 덮어쓰지 못합니다.
실패한 기능 태스크부터 Airflow에서 재시도합니다. 입력을 다시 만들려면 새 DAG run으로 실행합니다.
dispatcher·outbox/inbox·이벤트 복구 경로는 사용하지 않습니다. 통합 전 DAG 소스는 삭제했습니다.
현재 DAG 정의는 `dags/pop_talk_dw_serving.py`, 기능 구현은 `modules/pipelines/orchestration/dw_pipeline.py`에서 읽으면 됩니다.
공통 plan/입력은 태스크 실행 중 같은 run의 XCom에서 조회하며, 그래프에는 단계 간 실행 순서를 표시합니다.
초기 수집의 영화/박스오피스 분기와 Databricks의 `stage_bundle → publish_exchange` 매핑 연결은 유지합니다.
후자는 원격 operator에 return_value가 없어 stage 결과로 mapped 게시 개수와 입력을 정하는 연결입니다.
실행별 clone/출력 테이블과 dbt 실행 로그의 보존 기간 정리는 아직 자동화하지 않았습니다.

FE 영화 상세의 `일별 흥행` 영역은 `/catalog/movies/{movie_id}/boxoffice` API로
`dev.movie_boxoffice_daily`를 조회해 기준일·순위·일일 관객·누적 관객을 표시합니다. 미수집 값은 0으로 만들지 않습니다.

2026-09-11 직접 적재 검증에서는 `dev_direct_service_20260911`의 6개 태스크가 성공했습니다.
모델 7개와 dbt 검사 43개를 통과했으며 동일 데이터 재실행에서 추가 변경은 0건이었습니다.
정책 적격 영화는 `dev.popcorn_movies`에 KOFIC 코드로 UPSERT하고 흥행은 `dev.movie_boxoffice_daily`에 적재합니다.
기존 영화 ID·승인 상태·운영 편집은 보존하고 신규 영화는 DRAFT/PENDING으로 등록합니다.
`dev.batch_runs`에 적재 이력을 남기며 별도 서빙 스냅샷과 `dw_serving` 스키마는 제거했습니다.
공개 API는 PUBLISHED/APPROVED 영화만 노출합니다.
일일 원천 수집·Databricks DAG의 스케줄 활성화 여부는 별도로 확인해야 합니다.

## 기존 초기 데이터 적재

`pop_talk_movie_initial_load`는 기존 S3 영화 5,985건을 현재 STAGING에 추가하고
`pop_talk_dw_serving`의 계산·검사·PostgreSQL `dev` 직접 적재 완료까지 기다리는 수동 DAG입니다.
기존 candidate 적재 DAG를 대체하며 API 초기 수집 DAG를 실행할 필요가 없습니다.

1. `pop_talk_movie_initial_load`와 `pop_talk_dw_serving`을 Unpause합니다.
2. `pop_talk_movie_initial_load`에서 초기 데이터 경로(`manifest_key`)를 확인하고 Trigger합니다.
3. 마지막 `build_dw_and_serve`가 성공하면 DW와 서빙 게시까지 완료된 것입니다.

원문 manifest/checksum, 영화 5,985건(정책 적격 4,320 / 제외 1,665건)을 검증합니다.
기존 변환과 S3 Exchange 형식은 재사용하고 최종 적재 대상만 운영 STAGING으로 연결합니다.
초기 원천에 없는 관측 시각은 NULL로 보존하기 위해 영화 observation 시각 컬럼의 NOT NULL 제약만 해제합니다.
DW는 `source_observed_at DESC NULLS LAST`로 최신 일일 관측을 우선합니다.
초기 원천에는 흥행 기록이 없으므로 기존 일일 흥행 데이터를 유지합니다.
재실행은 동일 원천·변환 버전의 데이터 중복을 막는 기존 loader를 사용합니다.
부모 재시도 시 동일한 하위 DW run을 다시 실행하며, 이미 적재한 STAGING은 재사용합니다.

서비스의 기존 영화 ID·승인·편집 정보는 그대로 유지됩니다.
초기 데이터의 실제 이관과 서비스 반영은 완료했습니다. 신규 영화는 자동 승인하지 않습니다.
직접 적재 구현은 `modules/pipelines/orchestration/service_sync.py`에서 확인합니다.

## 그래프에서 수집·처리 범위 읽기

- 일일 Raw: 박스오피스 수집 → 영화 후보 선정 → KOFIC 상세 → KMDb 후보 → 검증·READY.
  박스오피스는 기본 어제부터 7일간 API 순위 목록이며, 후보는 해당 영화와 올해 장편 첫 페이지 최대 100건이다.
- 초기 Raw: 연도별 KOFIC 목록·상세·KMDb 후보와 선택 날짜 박스오피스가 별도 태스크다.
- Databricks: 원천 입력 준비 → 단일 원격 Bronze/Silver 변환 → S3 Exchange → Snowflake STAGING → 대사 → DW 실행.
  원격 작업 내부 단계를 별도 Airflow 실행인 것처럼 표시하지 않는다.
- DW staging: 성공 적재 목록, 영화 관측, 흥행 관측.
- DW dw: dim_movie, fct_boxoffice_daily.
- DW mart: mart_boxoffice_daily, mart_data_quality.

태스크 표시명은 수집 대상과 적재 목적지를 설명한다. 표시명 변경은 API 경로나 XCom 태스크 ID를 바꾸지 않는다.
2026-09-12부터 DW는 모델별 태스크로 변경됐으므로 이전 6개 태스크 실행 이력과 현재 그래프는 다르다.
`graph_models_20260912` 실 실행은 모델 7개·전체 품질 검사·서빙을 포함한 10개 태스크가 약 74초에 모두 성공했다. 일일 수집 회귀 3개, 입력 전달 4개, DW 실행 검증 5개와 전체 13개 DAG 구조 계약을 확인했다. 이번 변경 후 외부 API 원천 수집은 새로 실행하지 않았다.
