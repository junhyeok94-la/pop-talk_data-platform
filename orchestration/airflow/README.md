# Airflow

실행과 설정은 [루트 README](../../README.md)를 따릅니다.

| DAG | 역할 |
|---|---|
| `pop_talk_movie_raw_initial` | 수동 초기 원본 수집 |
| `pop_talk_movie_raw_daily` | 일일 영화·흥행 원본 수집 |
| `pop_talk_warehouse` | 초기/일일 원본 → STG → dbt 품질 검사 → 선택적 게시 |
| `pop_talk_reviews_warehouse` | 읽기 전용 리뷰 스냅샷 → dbt 리뷰 마트 |
| `pop_talk_environment_check` | 플랫폼 DB·파일 마운트 검사 |
| `pop_talk_s3_canary` | S3 연결·원본 prefix 검사 |
| `workbench_retry_verification` | 기존 Workbench 재시도 검증 |

클라우드 변환·웨어하우스 DAG는 제거했습니다. [PostgreSQL Phase 1 DAG](../../docs/phase1-pipeline.md)를 사용합니다.
DAG는 기본 일시정지 상태이며 UI는 localhost:8080입니다.

- [DAG 개발 표준](DAG_DEVELOPMENT_STANDARD.md)
- [원본 수집](MOVIE_RAW_DAG.md), [일일 수집](MOVIE_DAILY_DAG.md)
- [관리 도구](scripts/README.md), [검사](tests/README.md), [Workbench](plugins/README.md)

플랫폼 DB는 서비스 DB와 분리합니다. Airflow DB는 플랫폼 PostgreSQL의 별도 `airflow` 데이터베이스입니다.
모델 실험 DAG는 제거했습니다. `compose.mlops.yaml`은 선택적 모델 워커 구성만 제공하며 DAG를 생성하지 않습니다.
