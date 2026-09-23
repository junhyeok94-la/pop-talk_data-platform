# Airflow 관리 도구

PowerShell 명령은 프로젝트 루트에서 실행합니다. Python 도구는 필요한 Airflow 패키지와
접속 환경을 가진 컨테이너 안에서 실행하며, 도움말·모듈 설명에 적힌 조건을 먼저 확인합니다.

| 도구 | 용도 |
|---|---|
| `prepare-airflow.ps1` | 로컬 Airflow 설정과 데이터 디렉토리 준비. 기존 비밀번호 보존 |
| `prepare-model-lab.ps1` | 학습 워커 설정과 연결 토큰 준비 |
| `start-local-ollama.ps1` | `.local/models/ollama`의 모델로 로컬 추론 서버 실행 |
| `register-movie-api-connections.ps1` | 영화 API Connection 등록 |
| `initialize_platform.py` | Compose 일회성 초기화: 플랫폼 제어 스키마·Airflow DB·dbt 스냅샷·Workbench·Pool |
| `migrate_dw_control.py` | 플랫폼 제어 스키마 마이그레이션. `POP_TALK_CONTROL_POSTGRES_*` 대상에 `POP_TALK_CONTROL_ADMIN_USER/PASSWORD`로 접속 |
| `migrate_workbench_postgres.py`, `migrate_executor_state.py` | 이전 SQLite에서 PostgreSQL로 이전하는 일회성 관리 도구 |
| `prepare-workbench-*.cjs`, `workbench-monitoring.sql` | 모니터링용 연결·조회 권한 구성 |
| `prepare_model_lab_workflow.py` | 표준입력의 프로젝트 프로필을 Model Lab에 등록 |
| `deploy_dbt_cosmos_artifact.py` | dbt 소스로 불변 실행 스냅샷 생성 |
| `dbt_cosmos_runner.py`, `dbt_strict_runner.py` | 이미지에 포함되는 dbt 실행 진입점 |
| `register_model_dag_deployment.py` | 실행 스냅샷의 모델 DAG registry 등록 |
| `export-movie-sample.ps1`, `upload*`, `reconcile_movie_baseline.py`, `capture_candidate_production_fingerprint.py` | 데이터 준비·이전·대사 |
| `export_airflow_workbench.py` | 현재 소스로 독립 패키지 생성 |

예시:

```powershell
./orchestration/airflow/scripts/prepare-airflow.ps1
./orchestration/airflow/scripts/prepare-model-lab.ps1
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/register_model_dag_deployment.py
```

DB 마이그레이션·연결 등록·스냅샷 게시 도구는 상태를 변경합니다. 일반 회귀 검사는
`../tests`에서 실행하고, 프로젝트 전체 준비·복원 도구는 루트 `scripts`에서 관리합니다.
