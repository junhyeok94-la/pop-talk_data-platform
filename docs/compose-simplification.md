# Compose 구성

기본 명령은 `python scripts/configure.py` 후 `docker compose up -d --build`입니다.
공통 이미지·환경변수·마운트·의존성은 `x-airflow`에 모았습니다.

- `db`: 플랫폼 PostgreSQL. 애플리케이션과 별도 볼륨입니다.
- `airflow-init`: Airflow DB·관리자·Workbench·모델 실행 Pool을 준비하고 종료합니다.
- API·scheduler·DAG processor·triggerer: Airflow 실행 프로세스입니다.
- 모델 워커: `compose.mlops.yaml`을 선택한 경우에만 실행합니다.

클라우드 실행 스냅샷 준비와 모델 제어 스키마 생성은 제거했습니다.
서비스 DB는 `airflow.env`의 `POP_TALK_POSTGRES_*`로 지정하며 초기화에는 필요하지 않습니다.
triggerer는 선택적 모델 실험 DAG를 위해 유지합니다.

현재 검사 결과는 [Phase 1 정리 검증](phase1-cleanup-validation.md)을 참고합니다.
