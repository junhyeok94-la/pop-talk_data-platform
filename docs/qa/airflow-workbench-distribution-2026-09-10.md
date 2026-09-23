# Airflow Workbench 독립 배포 검증

사용자 선택: 공통 이미지 + 별도 학습 워커. 외부 공개는 수행하지 않았다.

## 산출물

- 유지보수 템플릿: `distribution/airflow-workbench`
- 소스 내보내기: `scripts/export_airflow_workbench.py`
- 독립 소스: `.local/releases/airflow-workbench-0.1.0-dev`
- 압축 파일: `.local/releases/airflow-workbench-0.1.0-dev.zip`
- 압축 파일 SHA-256: `c601d6fb80ecc21ce531cc69d0ba3f3403bbbc5051548ce53a72857d7bee7e14`
- 소스 파일 143개와 SHA-256 manifest. ZIP CRC 및 Python 구문 검사 통과.

프로젝트 업무 DAG, dbt 래퍼, 업무 DB migration, .env, 데이터, 가중치, 로그, 계정/실험 기록을 제외했다. 플러그인 정적 UI와 빌드 소스/lockfile/third-party notice는 포함했다.

## 빌드

공통 Airflow 기반은 `apache/airflow:3.3.1-python3.12`와 digest를 고정했다. 추가 의존성은 해당 이미지의 constraints를 사용한다. 공식 entrypoint는 유지한다.

| 로컬 이미지 | 최종 이미지 ID |
| --- | --- |
| `airflow-workbench:0.1.0-dev-airflow3.3.1` | `sha256:c7e376d872583eda41242b79aa78db6dd7bf5f34fb498d1a0032dbfbd6b79973` |
| `airflow-workbench-worker:0.1.0-dev-control` | `sha256:e1a902f3d44d6ff3f1caec4b5a51be9a3c3fd4bd5518b906a80930d1e07f3d24` |

두 이미지 모두 독립 소스에서 빌드했다. GPU training/batch target은 제공하지만 전체 빌드·GPU 학습·Kubernetes 실행은 검증하지 않았다.

## 결과

- 독립 `workbench-package-qa` Compose 프로젝트, PostgreSQL DB/볼륨, 호스트 포트 18080에서 검사했다.
- Airflow 및 Workbench migration 완료. Workbench migration ID는 1, 3, 4, 5, 6이다.
- 초기화 재실행 시 migration 기록, Model Lab 레코드, 관리자 비밀번호 hash 동일.
- 최초 관리자, WorkbenchAccess, ModelLabRunner 생성 확인. 일반 사용자 자동 역할 부여 없음.
- 대시보드/운영 모니터링/Model Lab 문서, 공통 호스트 JS/vendor JS, bootstrap API가 인증 후 200.
- 기본 평가 DAG 1개, 선택 워커 설치 후 진단/LLM 학습/임베딩 학습 DAG 3개 추가. 모두 일시정지, import 오류 없음.
- 워커는 token 없이 401, 올바른 token으로 자원·PostgreSQL 상태 API 조회 성공.
- 공통 회귀 검사 53개, 워커 회귀 검사 8개 통과.
- 기존 워커 테스트 1개의 예전 입력 fixture를 현재 recipe/storage 계약으로 수정했다. 실행 코드 변경 없음.
- 새 파일의 DAG 목록 반영은 기본 bundle 갱신 주기 때문에 약 5분 걸리는 것을 확인해 문서에 반영했다.
- 브라우저 렌더링 자동 검사는 수행하지 않았다. 모델 추론이나 실제 GPU 학습을 이 검증 스택에서 실행하지 않았다.

## 유지 상태

사용자가 접속 정보를 요청하여 검증 스택은 18080에서 유지했다. 관리자 접속 정보는 `.local/workbench-distribution-qa/.env`에만 저장한다. 해당 파일은 공개 후보 소스/ZIP에 포함되지 않는다. 기존 8080 개발 스택과 계정/비밀번호는 변경하지 않았다.

마지막 소스의 변경은 검증 스크립트·문서·워커 기반 이미지 digest 고정이다. 실행 중인 검증 컨테이너의 플러그인/워커 실행 소스는 최종 패키지와 동일하다. 공개 프로젝트명·작성 코드 라이선스 결정과 외부 저장소/레지스트리 게시가 남아 있다.
