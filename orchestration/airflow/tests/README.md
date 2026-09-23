# Airflow 검증

프로젝트 루트에서 실행합니다. 일반 회귀 테스트는 외부 학습·S3 작업을 실행하지 않습니다.
PostgreSQL 검증은 각 테스트가 마련한 격리 공간을 사용합니다.

전체 회귀 검사에는 워커 소스를 import하는 테스트도 포함됩니다. 일회성 컨테이너에 해당 소스를 추가합니다.

```powershell
docker compose run --rm --no-deps -T -v "${PWD}/orchestration/model_worker:/app:ro" -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/opt/airflow/modules:/opt/airflow/plugins:/opt/airflow/scripts:/opt/airflow/tests:/opt/airflow:/app airflow-scheduler python -m unittest discover -s /opt/airflow/tests -t /opt/airflow -v
docker compose exec -T airflow-scheduler python /opt/airflow/tests/check_airflow_dag_contracts.py
docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json
node --test orchestration/airflow/tests/test_workbench_navigation.cjs orchestration/airflow/tests/test_workbench_refresh.cjs orchestration/airflow/tests/test_workbench_theme.cjs
```

- `pipelines/`: 수집·변환·식별·Raw Asset·PostgreSQL 서비스 게시 계약 검증.
- `pipelines/platform/`: Phase 1 적재·dbt·리뷰·게시 복구. 실행 중인 Docker에서 `./scripts/check-phase1.ps1`로 별도 DB를 생성해 검증하고 자동 삭제합니다. 업무 DB에 fixture를 넣지 않습니다.
- `smoke/`: 실제 API·GPU·DB를 대상으로 수동 실행하는 검사. 파일의 설명과 전제 조건을 확인한 뒤 개별 실행합니다.
- `benchmarks/`: 부하·성능 측정. 일반 회귀 검사와 별도로 실행합니다.

과거 결함을 재현하기 위해 당시 상태를 가정한 일회성 코드는 `.local/qa`에 보존합니다.
