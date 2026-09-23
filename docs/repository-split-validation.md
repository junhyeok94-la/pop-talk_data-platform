# 저장소 분리 검증 — 2026-09-23

분리한 후보 저장소와 새 플랫폼 전용 PostgreSQL 볼륨을 사용했습니다.

| 항목 | 결과 |
|---|---|
| 파이프라인 회귀 | 기존 Airflow Linux 이미지에서 204 tests: 186 passed, 18 skipped |
| Workbench 브라우저 로직 | Node 테스트 9 passed |
| DAG 구조 | 분리본과 새 dbt 스냅샷에서 13개 DAG 계약 검사 통과 |
| Compose | 기본 설정 및 compose.application.yaml 병합 설정 검사 성공 |
| dbt 실행 스냅샷 | 새 소스에서 parse·manifest·모델/검사 목록 검증 및 current 포인터 생성 성공 |
| Airflow 초기화 | 새 플랫폼 DB에서 migration, Workbench schema, 실행 Pool 생성 성공 |
| 신규 설정 | 임시 디렉터리에서 설정 생성·재실행 후 비밀번호 파일 해시 동일 |
| 소스 | Python 및 PowerShell 구문 검사 성공 |
| 공개 파일 | API 키·토큰·개인키 패턴 검사에서 실제 자격증명 탐지 없음 |

18개 skip은 명시적으로 활성화해야 하는 외부 PostgreSQL 통합 시나리오입니다.
dbt 스냅샷 검증은 모델 7개·테스트 정의 43개의 구성을 검사한 것이며 Snowflake에서 dbt build/test를
실행한 결과가 아닙니다. 실제 클라우드 DAG, 서비스 DB 게시·리뷰 추출, GPU 학습은 실행하지 않았습니다.
Workbench의 전체 DB 기능 회귀 테스트를 이번 분리 검증에서 다시 실행하지는 않았습니다.

검증 중 dbt 준비 작업에 Airflow 기본 entrypoint가 DB 초기화를 먼저 시도하는 문제를 발견했습니다.
airflow-dbt-prepare에 Python entrypoint를 지정하고 DB 없이 다시 실행하여 성공했습니다.
Windows에서는 Linux 전용 파일 보호 검사와 Airflow import가 불가능해 공식 실행 환경인 Linux 컨테이너에서
파이프라인 테스트를 최종 검증했습니다.

기존 개발 경로를 보존한 소스 배포입니다. PostgreSQL 기반 Phase 1 엔진 전환은 후속 작업입니다.
