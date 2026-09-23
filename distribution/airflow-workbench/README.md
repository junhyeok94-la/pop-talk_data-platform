# Airflow Workbench — distribution candidate

Apache Airflow 3.3.1 위에서 개인 대시보드, 운영 모니터링, Model Lab을 제공하는 플러그인입니다. 공통 이미지는 공식 `apache/airflow:3.3.1-python3.12` 이미지를 확장합니다. GPU 학습은 별도 워커가 수행합니다.

이 디렉터리는 배포용 템플릿입니다. 개발 저장소에서는 아래 명령으로 소스를 포함한 독립 패키지를 먼저 만드세요. 이미 내보낸 패키지를 받은 경우 바로 빠른 시작으로 진행하세요.

```console
python orchestration/airflow/scripts/export_airflow_workbench.py .local/releases/airflow-workbench-0.1.2-dev --archive
```

현재는 공개 전 검증 후보입니다. 작성 코드의 라이선스는 아직 선택하지 않았습니다. 공개 저장소/이미지 배포 전에 [공개 준비 항목](RELEASE.md)을 완료하세요.

명령은 소스 폴더와 같은 이름의 ZIP, `.zip.sha256`을 함께 만듭니다. 기존 산출물은 덮어쓰지 않습니다. 처음 사용하는 사람은 [수동 실행 가이드](WALKTHROUGH.md), 이전 설치 사용자는 [변경 기록](CHANGELOG.md)을 참고하세요.

## 포함 범위

| 구성 | 제공 기능 |
| --- | --- |
| 공통 Airflow 이미지 | 대시보드 편집·조회, 실행 이력·운영 모니터링, Model Lab UI/API, 평가·비교·버전 등록·품질 검토·적용 구성 내보내기 |
| 초기화 서비스 | Airflow DB migration, 별도 `workbench` schema migration, 최초 관리자와 보조 역할 생성 |
| DAG 생성 | 추론 연결별 평가 DAG, 선택 설치한 실행 환경별 진단·LLM SFT·임베딩 학습 DAG |
| 별도 워커 | HTTP 실행 제어, 학습 데이터·가중치 점검, 취소, GPU 학습, 산출물 참조 |

프로젝트 업무 DAG, dbt 실행 래퍼, 데이터 웨어하우스 migration, 실제 계정·실험·접속 정보·데이터·모델 가중치는 포함하지 않습니다. 공식 기본 이미지 자체에 포함된 provider는 그대로 남아 있습니다.

## 빠른 시작

Docker Compose v2와 Python 3.10 이상이 필요합니다. Linux 컨테이너를 사용하세요. Windows에서는 Docker Desktop Linux 컨테이너로 실행합니다.

```console
python scripts/configure.py --port 8080 --ollama-url http://host.docker.internal:11434
docker compose build airflow-init
docker compose up -d
docker compose ps -a
```

이미 8080을 사용하는 서비스가 있다면 `--port 18080` 등 빈 포트를 지정하세요. `.env`의 `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`로 `http://localhost:8080`에 로그인합니다. 비밀번호는 명령 출력에 표시하지 않습니다. `configure.py`는 기존 `.env`를 덮어쓰지 않습니다.

초기화가 끝나면 `model_lab_eval_ollama` DAG가 생성됩니다. Model Lab의 환경·연결에서 접속 정보의 출처, 연결된 DAG 링크, 운영 상태, 평가 설정 점검을 함께 확인합니다. DAG 목록 반영 후 활성화하고 모델 목록을 확인하세요. Ollama 서버 실행과 모델 준비는 사용자가 별도로 수행합니다. 최초 프로젝트·평가셋·실험은 UI에서 만듭니다.

처음 생성하는 DAG는 수동 실행 전용이고 일시정지 상태입니다. 설치만으로 평가·학습 작업이 실행되지는 않습니다. 설치 후 새로 생성한 파일이 DAG 목록에 나타나기까지 기본 갱신 주기에 따라 약 5분이 걸릴 수 있습니다. 대시보드는 기존 Airflow 메타데이터를 조회하므로 별도 대시보드 DAG가 필요하지 않습니다. 외부 SQL 모니터링은 별도 읽기 전용 Connection과 조회 대상 준비가 필요합니다.

## 설치 설정과 권한

추론 연결을 등록할 때 Airflow 접속 정보를 목록에서 선택하거나 ID를 직접 입력할 수 있습니다. 환경변수 Connection도 표시하며 외부 Secrets Backend를 쓰는 경우 실제로 조회된 출처를 표시합니다. 목록에는 해당 계정이 조회할 수 있는 ID만 노출하고 주소·토큰·비밀번호는 반환하지 않습니다. 외부 저장소의 전체 ID 목록을 열거하지는 않으므로 목록에 없는 ID는 직접 입력하세요.

추론 연결 ID는 평가 설정, Connection ID는 서버 접속 정보, DAG ID는 Airflow 작업을 구분합니다. 새 설정에는 생성할 DAG 이름과 파일명이 미리 표시됩니다. 이 식별자는 같을 필요가 없으며 기존 설치의 이름을 자동 변경하지 않습니다.

내 대시보드와 Model Lab의 활성화·일시중지 버튼은 같은 Airflow DAG 권한과 상태 변경 API를 사용합니다. Model Lab 태그만으로 차단하지 않습니다. DAG 운영 상태와 평가·학습 준비 상태를 별도로 표시하고, 실제 실행을 제출할 때 DAG·Pool 등 설정을 다시 검사합니다. 활성화는 추론 서버 연결이나 모델 품질을 보증하지 않습니다.

처음 설치할 추론 연결은 `config/bootstrap.json`에서 설정합니다. 실제 URL·토큰은 Airflow Connections 또는 환경변수/Secrets Backend로 공급합니다. 기본 Connection은 `.env`의 JSON으로 공급되므로 Airflow Connection 관리 화면에는 저장된 레코드로 나타나지 않을 수 있습니다. 설정을 바꿔 이미지를 다시 빌드해도 기존 DB의 연결·실행 환경 정의가 우선합니다. 설치 후 변경은 Model Lab 관리자 화면에서 저장하고 DAG를 다시 생성하세요.

| 역할 조합 | 의도한 범위 |
| --- | --- |
| Viewer + WorkbenchAccess | 플러그인 접근, 자신의 프로젝트·개인 설정 관리, 허용된 실행 조회 |
| 위 조합 + ModelLabRunner | 등록된 평가 DAG 실행 |
| Op 또는 Admin | 현재 구현에서 Workbench 전역 관리, 전체 Model Lab 프로젝트 조회 |

설치기는 보조 역할만 만들며 일반 사용자에게 자동 부여하지 않습니다. 사용자 관리 화면에서 필요한 역할을 부여하세요. `ModelLabRunner`는 `can_create on DAG Runs`와 초기화 시 등록된 평가 DAG 각각의 `can_edit` 권한을 갖습니다. 새 평가 DAG를 나중에 추가했다면 해당 DAG 권한도 추가해야 합니다. 학습 DAG 권한은 별도로 부여합니다. 플러그인 접근 권한과 실제 Airflow DAG 실행 권한은 함께 검사합니다.

Model Lab 프로젝트의 owner/member 권한은 Airflow 역할과 별개입니다. 프로젝트 ID는 설치 내에서 유일한 식별자이며 프로젝트 이름·사용자 ID와 다릅니다. 현재 `WorkbenchAccess`는 Model Lab 전용 접근 역할이 아니라 전체 Workbench 플러그인 접근 역할입니다.

## 별도 워커

[워커 설치](WORKER.md)를 따르세요. 제어 프로토콜 검증용 `control`, 실제 CUDA 학습용 `training`, 선택적인 Kubernetes 실행용 `batch` 빌드 대상이 있습니다. 공통 Airflow 컨테이너에 GPU나 학습 라이브러리를 추가하지 않습니다.

## 업그레이드와 보존

플러그인 소스를 수정한 뒤 새 경로로 패키지를 내보내고 새 버전 태그의 이미지를 빌드하세요. API 서버·scheduler·DAG processor·triggerer에 같은 버전을 사용합니다. UI vendor를 수정할 때는 `plugins/airflow_workbench/dashboard/frontend`에서 `npm ci`, `npm run build` 후 생성한 정적 파일과 notice를 함께 배포하세요.

DB와 artifacts/DAG/log 볼륨을 백업하고 실행 중인 작업을 정리한 유지보수 시간에 서비스를 교체합니다. 초기화 서비스는 재실행 가능하며 기존 관리자 비밀번호와 등록 설정을 유지합니다. Airflow migration과 Workbench migration은 각각의 버전 기록을 사용합니다. 이미지 되돌리기만으로 DB schema가 되돌아가지는 않습니다.

```console
docker compose stop airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
docker compose run --rm airflow-init
docker compose up -d
```

`docker compose down`은 명명된 데이터 볼륨을 보존합니다. `down -v`는 DB와 결과를 삭제하므로 초기화 명령으로 사용하지 마세요.

이 Compose는 단일 호스트 설치 예제입니다. 다중 호스트 설치에서는 모든 Airflow 실행 구성요소에 같은 DAG와 평가 artifact 저장소가 필요합니다. 현재 평가 취소·중복 실행 방지는 공유 파일시스템의 POSIX 잠금 의미에 의존합니다. PostgreSQL과 Airflow 웹 서비스의 운영 배포 설정은 배포 환경에 맞춰 관리하세요.

## 검증

내보낸 소스는 `SOURCE_MANIFEST.json`에 SHA-256으로 기록됩니다. 배포 전 별도 DB/볼륨에서 초기화 두 번, DAG import, 인증된 페이지/API, 회귀 검사를 수행합니다.

패키지를 받은 뒤 Docker 없이 소스 파일의 누락·변경을 검사할 수 있습니다. 설치 후 생긴 `.env`, 데이터 등 추가 파일은 검사 대상에 포함하지 않습니다. 배포자가 제공한 ZIP 체크섬도 함께 비교하세요. 이 검사는 파일 무결성을 확인하며 작성자를 인증하는 서명은 아닙니다.

```console
python scripts/verify_source.py
```

```console
docker compose run --no-deps --rm -v ./tests:/tests:ro -v ./worker:/worker:ro -e PYTHONPATH=/opt/airflow/plugins:/tests:/worker airflow-init python -m unittest discover -s /tests -p "test_*.py"
docker compose run --no-deps --rm -v ./tests:/tests:ro -v ./worker:/worker:ro -e PYTHONPATH=/opt/airflow/plugins:/tests:/worker airflow-init python -m unittest discover -s /worker -p test_worker.py
```

테스트는 PostgreSQL 내 임시 `wb_test_*` schema를 만들고 정리합니다. 학습 가중치나 외부 모델 API 호출 없이 계약·권한·취소 등의 동작을 검사합니다.

이미 사용 중인 설치에서는 다음 명령으로 페이지/API, 연결된 DAG·Pool, 접속 정보 목록을 점검합니다. 재초기화하거나 프로젝트·DAG 활성 상태를 변경하지 않습니다. 별도 워커 연결까지 검사하려면 `--worker`를 추가하세요. 워커 옵션은 기본 Compose의 `model-worker`와 `model_lab_worker` Connection을 검사합니다.

```console
docker compose run --no-deps --rm airflow-init python /opt/workbench/scripts/verify_installation.py --existing
```

검증용 로그인은 `.env`의 관리자 계정을 사용합니다. UI에서 비밀번호를 변경했다면 검증 실행에 사용할 자격 증명도 갱신하세요. 이 명령은 비밀번호를 변경하지 않습니다.

프로젝트를 아직 생성하지 않은 별도 검증 설치에서는 아래 명령으로 재초기화 보존과 로그인·페이지·API·DAG 목록을 확인할 수 있습니다. 워커까지 설치했다면 `--worker`를 추가합니다. 모든 생성 DAG를 일시정지 상태로 두고, DAG 목록 갱신 후 실행하세요.

```console
docker compose run --rm airflow-init python /opt/workbench/scripts/verify_installation.py --repeat-init
```

이번 배포 후보의 확인 범위는 [검증 결과](VALIDATION.md)를 참고하세요.

공식 참고: [Airflow 이미지 확장](https://airflow.apache.org/docs/docker-stack/build.html), [공식 entrypoint](https://airflow.apache.org/docs/docker-stack/entrypoint.html), [FAB 권한](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/access-control.html).
