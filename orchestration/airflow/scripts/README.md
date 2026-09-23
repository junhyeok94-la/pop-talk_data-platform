# Airflow 관리 도구

프로젝트 루트에서 실행합니다. Python 도구는 필요한 패키지가 설치된 Airflow 컨테이너를 사용합니다.

| 도구 | 용도 |
|---|---|
| `initialize_platform.py` | Airflow DB·관리자·Workbench·모델 Pool 초기화 |
| `prepare-airflow.ps1` | 로컬 설정 생성기의 PowerShell 진입점 |
| `register-movie-api-connections.ps1`, `register_movie_api_connections.py` | 영화 API Connection 등록 |
| `upload-legacy-snapshot.ps1`, `upload_legacy_snapshot.py` | 지정한 원본을 S3에 보존 |
| `export-movie-sample.ps1` | 지정한 서비스 원본의 샘플 추출 |
| `prepare-model-lab.ps1`, `prepare_model_lab_workflow.py` | 선택적 모델 워커·실험 설정 |
| `start-local-ollama.ps1` | 로컬 모델 실행 |
| `migrate_workbench_postgres.py`, `migrate_executor_state.py` | 과거 Workbench 저장소 이전 |
| `prepare-workbench-*.cjs`, `workbench-monitoring.sql` | 모니터링 연결·권한 구성 |
| `export_airflow_workbench.py` | 독립 Workbench 패키지 생성 |

초기화에는 `.env`의 플랫폼 관리자 계정을 사용하며 일반 Airflow 프로세스에 이 관리자 계정을 전달하지 않습니다.
클라우드 dbt 배포와 모델 실행 제어 도구는 제거했습니다.
