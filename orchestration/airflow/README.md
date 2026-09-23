# Airflow

실행과 설정은 [루트 README](../../README.md)를 따릅니다.

| DAG | 역할 |
|---|---|
| `pop_talk_movie_raw_initial` | 수동 초기 원본 수집 |
| `pop_talk_movie_raw_daily` | 일일 영화·흥행 원본 수집 |
| `pop_talk_environment_check` | 플랫폼 DB·파일 마운트 검사 |
| `pop_talk_s3_canary` | S3 연결·원본 prefix 검사 |
| `workbench_retry_verification` | 기존 Workbench 재시도 검증 |
| `workbench_managed/*` | 기존 선택적 모델 실험 |

클라우드 변환·웨어하우스 DAG는 제거했습니다. PostgreSQL 분석 적재 DAG는 후속 구현입니다.
DAG는 기본 일시정지 상태이며 UI는 localhost:8080입니다.

- [DAG 개발 표준](DAG_DEVELOPMENT_STANDARD.md)
- [원본 수집](MOVIE_RAW_DAG.md), [일일 수집](MOVIE_DAILY_DAG.md)
- [관리 도구](scripts/README.md), [검사](tests/README.md), [Workbench](plugins/README.md)

플랫폼 DB는 서비스 DB와 분리합니다. Airflow DB는 플랫폼 PostgreSQL의 별도 `airflow` 데이터베이스입니다.
모델 실험은 `compose.mlops.yaml`을 사용하는 선택 기능이며 Phase 1 필수 범위가 아닙니다.
