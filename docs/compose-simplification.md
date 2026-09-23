# Compose 단순화 — 2026-09-23

기본 실행 명령은 `python scripts/configure.py` 후 `docker compose up -d --build`입니다.

- 공통 Airflow 이미지·환경변수·마운트·의존성은 `x-airflow`에 모았습니다.
- DB 생성, 제어 스키마 마이그레이션, dbt 스냅샷 준비, Airflow 초기화를 `airflow-init`으로 통합했습니다.
- 플랫폼 관리자 계정은 초기화 컨테이너에만 전달합니다. 제어 스키마는 서비스 DB가 아닌 플랫폼 DB에 생성합니다.
- 모델 워커는 선택 파일 `compose.mlops.yaml`로 이동했습니다.
- `compose.application.yaml`을 제거하고 서비스 DB 연결은 `airflow.env`의 `POP_TALK_POSTGRES_*`로 관리합니다.
- 기존 영화 초기 적재 DAG가 deferrable 대기를 사용하므로 triggerer는 유지했습니다.

## 검증

기존 로컬 `pop-talk-airflow:3.3.1-local` 이미지와 별도 Compose 프로젝트
`pop-talk-compose-check`, 새 전용 PostgreSQL 볼륨, 임의 호스트 포트를 사용했습니다.
이미지 Dockerfile은 변경하지 않았으며 이번 검증에서 이미지를 다시 빌드하지 않았습니다.

- 기본 Compose 및 모델 워커 파일 병합 구성 검사 통과.
- 새 DB에서 통합 초기화 종료 코드 0. 제어 스키마 마이그레이션 7개와 Airflow 관리자·Workbench·Pool 생성 확인.
- `dw_control_runtime`에 superuser·DB 생성·role 생성 권한이 없음을 확인.
- API health 응답에서 metadata DB·scheduler·triggerer·DAG processor 모두 healthy.
- 메타데이터 DB에서 DAG import 오류 0개, 활성화된 DAG 0개 확인.
- 설정 생성기 재실행 후 `.env`와 `airflow.env` 파일 해시 동일.
- 동일 DB에서 `airflow-init` 재실행 종료 코드 0. 기존 관리자를 재사용하고 마이그레이션·스냅샷·Pool 준비 완료.
- Linux 이미지의 파이프라인 회귀 검사: 204개 중 186개 통과, 외부 DB 통합 검사 18개 skip.
  실행 명령은 `python -m unittest discover -s /opt/airflow/tests/pipelines -t /opt/airflow -q`입니다.

실제 클라우드 작업, 서비스 DB 게시, GPU 모델 워커 실행은 검증 범위에 포함하지 않았습니다.
