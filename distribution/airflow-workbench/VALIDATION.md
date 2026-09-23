# 0.1.2-dev 검증 결과

검증일: 2026-09-10~11. Docker Desktop의 Linux 컨테이너와 독립 PostgreSQL 17 DB/볼륨에서 확인했습니다.

| 검사 | 결과 |
| --- | --- |
| 공식 Airflow 3.3.1 / Python 3.12 기반 이미지 build, pip check | 통과 |
| Airflow와 Workbench DB 초기화 | 통과 |
| 초기화 재실행 | 기존 migration 기록·Model Lab 레코드·관리자 비밀번호 hash 보존 |
| 최초 관리자 및 WorkbenchAccess / ModelLabRunner 역할 | 생성 확인 |
| 대시보드·운영 모니터링·Model Lab 페이지와 정적 자산 | 인증 후 HTTP 200 |
| Model Lab bootstrap API / 평가 DAG 권한 확인 | 통과 |
| 공통 설치의 DAG | `model_lab_eval_ollama`만 등록, 일시정지 상태 |
| 선택 워커 설치의 DAG | 진단·LLM 학습·임베딩 학습 3개 추가, import 오류 없음 |
| 제어 워커 독립 이미지 build, pip check | 통과; Airflow 패키지 설치 없이 실행 |
| 워커 인증 / 상태 API 왕복 | 무인증 401, 토큰 인증 후 PostgreSQL 기반 상태 조회 성공 |
| 공통 회귀 검사 | 74개 통과 |
| 워커 취소·데이터 스냅샷 회귀 검사 | 8개 통과 |
| 호스트 내 화면 이동 검사 | 2개 통과; Connection 관리 화면 이동 포함 |
| 변경한 JavaScript 문법 검사 | 5개 파일 통과 |
| 배포 도구 검사 | 9개 통과; 원본 무결성, 초기 비밀번호 보존, 기존 설치의 DAG 상태 검사 규칙 |
| 개발 저장소 export 검사 | 3개 통과; 버전명 보존, ZIP 재현성, 덮어쓰기 거부, 템플릿 파일 허용 목록 |
| Connection 목록·상태 API | 환경변수·DB·외부 Secrets Backend 출처 구분, 권한 및 비밀값 비노출 확인 |
| DAG 운영 상태·실행 준비 분리 | Model Lab DAG 활성화·일시중지 허용, 권한 부족·상태 충돌·실행 설정 불일치는 계속 차단 |
| 0.1.2-dev 이미지의 전체 회귀 검사 | 공통 74 + 배포 9 + 워커 8 = 91개 통과 |
| 기존 설치 읽기 전용 검사 | `--existing --worker` 통과; 전후 레코드·관리자 비밀번호 hash·DAG 활성 상태 동일 |
| 0.1.2-dev 적용 | API 서버·scheduler·DAG processor·triggerer·제어 워커 교체; API 서버 healthy |

GPU training/batch 이미지의 전체 빌드, 실제 GPU 학습, Kubernetes 클러스터 실행은 이번 배포 검증에 포함하지 않았습니다. 해당 Dockerfile target과 실행 설정을 제공하지만 GPU·모델·데이터를 갖춘 환경에서 별도 확인이 필요합니다. 공통 회귀 검사는 외부 모델 서비스를 모의 처리하므로 이 결과가 모델 품질이나 GPU 학습 성공을 뜻하지 않습니다.

0.1.0-dev의 독립 설치 검증에 이어, 0.1.1-dev에서는 기존 DB·볼륨을 보존한 채 공통 이미지를 교체했습니다. 브라우저에서 환경·연결 카드, 환경변수 Connection 선택, 연결 ID에 따른 DAG·파일명 미리보기, Model Lab 일시중지와 대시보드 활성화 흐름을 확인했습니다. 전체 평가·학습 흐름의 브라우저 재실행은 포함하지 않았습니다.

0.1.2-dev는 설치·배포 도구와 문서를 보완합니다. 실행·대기 DAG Run과 진행 중 워커 작업이 없는 것을 확인하고 기존 검증 설치를 업그레이드했습니다. 내보낸 150개 파일의 무결성을 확인했으며, 개발 저장소 export 검사는 위 91개와 별도로 3개가 통과했습니다.

| 로컬 검증 이미지 | image ID |
| --- | --- |
| `airflow-workbench:0.1.2-dev-airflow3.3.1` | `sha256:d70e6a9794b4dc9d8b6ed148a016b710161af4c4497b94d85a252710a458b4ad` |
| `airflow-workbench-worker:0.1.2-dev-control` | `sha256:b4f21052d659abef7b2b151094797b9db72d03212b8e6719a1bca9be1aadac44` |

새 설치의 DAG 목록에 업무용 DAG가 없음을 확인했습니다. 공개 소스 패키지에는 `.env`나 설치 DB/볼륨, 테스트 계정, 실제 데이터·가중치가 포함되지 않습니다. 공개 라이선스 결정과 외부 게시 작업은 남아 있습니다.
