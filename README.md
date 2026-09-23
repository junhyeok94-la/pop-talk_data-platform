# Pop Talk Data Platform

영화·흥행·리뷰 데이터를 수집하고 PostgreSQL 기반 DW·분석 마트를 구축하는 프로젝트입니다.
[애플리케이션 저장소](https://github.com/junhyeok94-la/pop-talk_application)와 분리해 관리합니다.
[코드 출처](docs/provenance.md) · [저장소 간 계약](docs/repository-boundary.md)

현재 구현은 **S3 원본 수집과 순수 Python 정제·식별 로직**입니다.
PostgreSQL STG 적재와 dbt DW/Mart, 리뷰 추출 및 게시 DAG는 다음 Phase 1 작업입니다.
기존 클라우드 웨어하우스 파이프라인과 전용 dbt 프로젝트는 제거했습니다.
향후 Snowflake PoC는 별도로 개발하며 현재 실행 경로에 포함하지 않습니다.

| 경로 | 내용 |
|---|---|
| `orchestration/airflow/dags` | 원본 수집·연결 점검 및 기존 선택적 모델 실험 DAG |
| `orchestration/airflow/modules/pipelines/collectors` | KOFIC/KMDb·흥행 원본 수집 |
| `orchestration/airflow/modules/pipelines/transforms` | 저장소와 무관한 Python 원본 검증·정제 |
| `orchestration/airflow/modules/pipelines/modeling` | 영화 식별·매핑 계약 |
| `orchestration/airflow/modules/pipelines/orchestration` | Raw Asset 계약·PostgreSQL 서비스 게시 함수 |
| `orchestration/airflow/tests` | 파이프라인·플러그인 검사 |
| `orchestration/airflow/plugins` | 기존 Airflow Workbench |
| `orchestration/model_worker`, `datasets`, `distribution` | 선택적인 모델 실험·플러그인 배포 도구 |

## 실행

Docker Compose v2와 Python 3.10+가 필요합니다. 저장소 루트에서 실행합니다.

```console
python scripts/configure.py
docker compose up -d --build
```

`airflow-init`이 Airflow DB·관리자·Workbench·모델 실행 Pool을 준비합니다.
초기화에 클라우드 계정, 원본 수집 API 키, 서비스 DB 연결은 필요하지 않습니다.
Airflow는 [localhost:8080](http://localhost:8080), 플랫폼 PostgreSQL은 55433 포트입니다.
최초 관리자 정보는 Git에서 제외된 `.local/config/airflow.env`에 있습니다.
기존 비밀번호 파일은 설정 생성 시 덮어쓰지 않습니다.

```console
docker compose ps
docker compose logs airflow-init
docker compose stop
```

기본 구성은 DB·Airflow 프로세스 4개·일회성 초기화 서비스 1개입니다.
triggerer는 기존 선택적 모델 실험 DAG의 비동기 대기를 지원합니다.

## 원본 수집

`pop_talk_movie_raw_initial`은 수동 초기 수집, `pop_talk_movie_raw_daily`는 일일 수집입니다.
두 DAG 모두 기본 일시정지 상태입니다. 실행하려면 `pop_talk_aws`, `pop_talk_kofic`,
`pop_talk_kmdb` Connections와 수집 대상 S3 버킷을 구성합니다.
현재 버킷 기본값은 기존 환경 기준이므로 실행 전에 확인합니다.
원본 수집 성공은 분석 DB 적재나 서비스 게시 완료를 뜻하지 않습니다.

`pop_talk_environment_check`는 플랫폼 DB·파일 마운트, `pop_talk_s3_canary`는 S3를 점검합니다.
상세: [초기 수집](orchestration/airflow/MOVIE_RAW_DAG.md) · [일일 수집](orchestration/airflow/MOVIE_DAILY_DAG.md)

## 서비스 DB와 선택 기능

서비스 DB는 별도 저장소·볼륨입니다. 향후 게시 연결은 `airflow.env`의
`POP_TALK_POSTGRES_*`로 설정합니다. Docker Desktop 기본 주소는 `host.docker.internal:55432`입니다.
일반 Linux에서는 컨테이너에서 접근 가능한 주소를 지정합니다.
서비스 게시 함수는 보존했지만 이를 실행하는 Phase 1 DAG는 아직 없습니다.

GPU 모델 실험이 필요한 경우에만 실행합니다.

```powershell
./orchestration/airflow/scripts/prepare-model-lab.ps1
docker compose -f compose.yaml -f compose.mlops.yaml up -d --build
```

## 다음 단계와 검증

[Phase 1 계획](docs/phase1-transition.md) · [아키텍처](docs/project-plan-and-architecture.md) ·
[회귀 검사](orchestration/airflow/tests/README.md)

현재 범위와 검사 결과는 [Phase 1 정리 검증](docs/phase1-cleanup-validation.md)을 참고합니다.

`docs/qa`의 과거 검증 기록은 현재 실행 성공의 증빙이 아닙니다.
