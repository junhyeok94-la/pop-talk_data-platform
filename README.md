# Pop Talk Data Platform

기존 영화 수집 배치를 DW·분석 마트로 확장하는 데이터 플랫폼입니다.
[애플리케이션 저장소](https://github.com/junhyeok94-la/pop-talk_application)와 분리해
Airflow DAG, 처리 모듈, dbt, 품질 검사와 서비스 게시를 관리합니다. [코드 출처](docs/provenance.md)

**현재 커밋은 개발 코드의 저장소 분리 기준선입니다. PostgreSQL 기반 Phase 1 전환은 아직 구현하지 않았습니다.**
기존 실행 경로는 S3 → Databricks → Snowflake/dbt → 서비스 PostgreSQL입니다.

| 경로 | 내용 |
|---|---|
| `orchestration/airflow/dags` | 수집·변환·DW DAG |
| `orchestration/airflow/modules/pipelines` | 수집·정제·적재·서비스 게시 로직 |
| `orchestration/airflow/modules/pipelines/dbt` | dbt 모델·매크로·품질 검사 |
| `orchestration/airflow/plugins` | 기존 Airflow Workbench 플러그인 |
| `orchestration/airflow/tests` | 파이프라인·플러그인 회귀 검사 |
| `orchestration/model_worker`, `datasets` | 선택적인 기존 모델 실험 도구·평가 초안 |
| `distribution/airflow-workbench` | 기존 플러그인 패키징 템플릿 |

분리 초기에는 import와 마운트 호환성을 위해 폴더 경로를 유지했습니다.
Workbench·학습 기능 확장은 Phase 1 필수 작업이 아닙니다.

## 로컬 준비

Docker Compose v2와 Python 3.10+ 설정 도구가 필요합니다. 저장소 루트에서 실행합니다.

```console
python scripts/configure.py
docker compose up -d --build
```

`airflow-init`이 플랫폼 DB의 제어 스키마, Airflow DB·관리자·Workbench·Pool,
dbt 실행 스냅샷을 준비한 뒤 Airflow 프로세스가 시작됩니다.
dbt 준비는 parse와 모델·검사 목록만 검증하며 실제 클라우드 수집·적재 작업은 실행하지 않습니다.
초기화 로그는 `docker compose logs airflow-init`으로 확인합니다.
Airflow는 8080, 플랫폼 PostgreSQL은 55433을 사용합니다. Airflow metadata DB와 서비스 DB를 분리했습니다.
최초 관리자 접속 정보는 Git에서 제외된 `.local/config/airflow.env`에 있습니다.

기본 구성은 DB와 Airflow 프로세스 4개, 일회성 초기화 서비스 1개입니다.
기존 영화 DAG의 비동기 대기를 위해 triggerer를 포함합니다. 모델 워커는 기본 실행에 포함하지 않습니다.
dbt 소스 또는 profile을 수정하면 다음 명령으로 실행 스냅샷을 갱신합니다.

```console
docker compose run --rm --no-deps airflow-init
```

이 명령은 이미 실행 중인 플랫폼 DB를 사용합니다. 초기화는 재실행 가능하며 기존 설정·데이터를 유지합니다.
일반 종료는 `docker compose stop`을 사용합니다.

이후 AWS·Databricks·Snowflake Connections, `.local/config/snowflake`의 개인키,
`airflow.env`의 Snowflake profile 환경변수를 설정해야 기존 클라우드 DAG를 실행할 수 있습니다.
현재 DAG의 S3 버킷·워크스페이스·DB 이름은 기존 환경 기준이므로 본인 환경에 맞게 확인합니다.
DAG는 기본 일시정지 상태이며 설치만으로 원격 데이터 작업을 시작하지 않습니다.

## 서비스 DB 연결

기본 설정은 Docker Desktop의 `host.docker.internal:55432`를 사용합니다.
`.local/config/airflow.env`의 `POP_TALK_POSTGRES_*`에 애플리케이션 DB 연결 정보를 지정하세요.
플랫폼 자체 DB의 `POSTGRES_*`와 구분합니다. 서비스 비밀번호 기본값은 비어 있습니다.

서비스 DB 연결은 영화·흥행 게시 시 필요하며 플랫폼 초기화에는 필요하지 않습니다.
별도 Compose 연결 파일 없이 `airflow.env`에서 접속 대상을 관리합니다.
Docker Desktop 이외의 Linux 환경에서는 컨테이너에서 접근 가능한 DB 주소와 포트를 지정해야 합니다.
서비스 ID·승인 상태·리뷰·관리자 수정값의 소유권은 [연결 계약](docs/repository-boundary.md)을 따릅니다.

## 선택 기능: 모델 워커

GPU 모델 실험이 필요할 때만 워커 인증 설정을 준비하고 추가 파일을 사용합니다.

```powershell
./orchestration/airflow/scripts/prepare-model-lab.ps1
docker compose -f compose.yaml -f compose.mlops.yaml up -d --build
```

GPU가 필요합니다. 함께 종료하려면 `docker compose -f compose.yaml -f compose.mlops.yaml stop`을 사용합니다.

## 검증과 다음 단계

[회귀 테스트](orchestration/airflow/tests/README.md), [분리 검증 결과](docs/repository-split-validation.md),
[Compose 단순화 검증](docs/compose-simplification.md),
[Phase 1 전환 계획](docs/phase1-transition.md)을 참고하세요.
`docs/qa`의 날짜가 붙은 문서는 분리 전 검증 기록이며 이번 커밋의 성공 증빙이 아닙니다.
해당 문서에서 참조한 로컬 화면 캡처·운영 데이터는 공개 저장소에 포함하지 않습니다.
