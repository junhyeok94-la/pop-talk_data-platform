# Airflow Workbench v2 검증 — 2026-09-10 KST

검증 환경: Airflow 3.3.1 / Python 3.12 / FAB / LocalExecutor / Windows Docker.
현재 화면: http://localhost:8080/ 왼쪽 **대시보드**, **Model Lab**.

## 구현과 브라우저 확인

- 최상위 sidebar 항목으로 직접 이동. 별도 주소를 입력할 필요 없음.
- SQL 편집기, 데이터소스/테이블 스키마, 결과 미리보기, Stat/선/영역/막대/Gauge/테이블.
- X/Y/series, 단위·소수점·색상·임계값 방향·크기, 패널 복제/이동/제거/드래그 배치.
- 카테고리, 다중 보드, 변수, 기간·집계·자동 갱신, JSON 가져오기/내보내기, 저장 충돌 검사.
- 실제 브라우저에서 성공률 패널을 95% 이하 경고로 저장하고 새로고침 후 유지 확인.
- 브라우저에서 `Model Lab DAG 모니터링` 보드와 변수 `dag_prefix`를 만들고 SQL을 직접
  작성하여 `time, series, value` 결과 1행을 확인한 뒤 저장. 현재 기본 2개 + 사용자 정의 1개.
- 새 보드의 임시 카테고리 표시와 편집 취소를 확인.
- Model Lab 실행·리소스 탭에서 GPU, 컨테이너 RAM, CPU, pool, DAG 계약, 작업 이력 확인.
- 파인튜닝 검증은 존재하지 않는 테스트 revision을 입력한 음성 검사:
  데이터 없음 / DAG paused / RAM 부족 / 원본 가중치 없음 / 학습 패키지 없음을 표시하고
  학습 시작 버튼 비활성 확인. 실제 revision 또는 학습 완료 모델을 생성한 검사가 아님.

![대시보드](airflow-dashboard-v2.png)

![SQL 편집기](airflow-query-editor-v2.png)

![Model Lab 실행·리소스](airflow-model-lab-resources-v2.png)

## 자동 검사

| 검사 | 결과 |
|---|---|
| 기존 Workbench 인증·저장·입력·평가·학습 요청 | 17 tests 통과 |
| SQL sandbox·바인딩·크기/시간·보드·RBAC·DAG 계약 | 9 tests 통과 |
| 별도 worker 취소 경합·예약·자원·파일 사본·8B 메모리 추정 | 7 tests 통과 |
| DAG 구조 | 기존 6 + Model Lab 3, 총 9개 계약 통과 |
| 기본 보드의 모든 SQL | 패널 오류 0 |
| 익명 접근 / CSRF / 내부 worker 인증 | 거부 확인 |

SQL 쓰기, ATTACH, PRAGMA, extension, sqlite_master, 재귀 CTE, 복수 문장을 거부했습니다.
쿼리 변수를 문자열 결합으로 실행하지 않는 것을 검사했습니다. PostgreSQL 문장/변수 변환과
위험 함수 차단은 단위 검사 통과, 실제 DB 계정 연결 검사는 승인 대기입니다.

## 실제 GPU와 Airflow 실행

GPU: NVIDIA RTX 3060, VRAM 12,288MiB. 별도 worker 제한: CPU 2, RAM 6,144MiB.
cgroup v1의 실제 제한을 읽어 표시합니다. 가중치가 준비되면 safetensors 헤더의 파라미터 수와
학습 방식으로 VRAM 하한을 추가 추정합니다. 비양자화 8B LoRA가 12GiB를 초과하는 검사 통과.

성공 진단:

- DAG: `pop_talk_model_lab_diagnostic`
- run: `workbench__b5adb4e0-9e72-49c2-84ab-b2642762f60e`
- 외부 job: `393b27cd1bad3c959a6c318b1c8a9f0c`
- 별도 PID에서 실제 CUDA 64MiB 할당·초기화·동기화·해제, 0.0134초.
- Airflow task `deferred` 관측, 그동안 GPU pool occupied=1.
- 완료 후 Airflow success, 결과의 `training_performed=false` 확인.

20,000MiB VRAM 요청은 DAG 생성 전에 409로 거부했습니다. 같은 요청 ID/DAG run과
worker key를 재전송하면 동일 실행을 반환합니다. 두 번째 GPU 작업, 학습 중 추론 예약,
추론 예약 중 학습 작업은 원자적으로 거부했습니다.

취소 전달 검사(최종 코드):

- run: `workbench__99f7848e-97c0-4b8f-b596-3ebae0d4abf8`
- 외부 job: `520406ab323d378198da71949121c3c8`
- deferred를 관측한 후 취소 → 외부 프로세스 종료 / job cancelled
  → Airflow failed → GPU pool occupied=0.
- 이 진단 DAG의 failed 이력은 의도한 취소 테스트 결과입니다.
- queued 취소 중 지연된 preflight가 돌아와도 subprocess를 시작하지 않는 회귀 검사 통과.

원본 검사 결과: [GPU 검사 JSON](../../.local/qa/docs/airflow-workbench-gpu-v2.json),
[취소 전달 JSON](../../.local/qa/docs/airflow-workbench-cancel-v2.json).

## 실제 모델 호출

공통 GPU 예약을 적용한 상태에서 두 실험 성공:

| 모델 | 실험 ID | 확인 |
|---|---|---|
| bge-m3:latest | `8b1034790b8143209145ecacf77b733f` | 1,024차원, 3.649초, 두 문서의 작은 검색 예제에서 Recall@1/nDCG@1/MRR@1=1 |
| qwen3:8b | `be2fa702e3a24b4aa5122191b5e8bfd9` | 4.547초, 응답 `연결 테스트 성공` |

위 결과는 호출 경로와 기록 기능 검사이며 전체 챗봇 성능 평가가 아닙니다.

## 검증하지 않은 범위 / 승인 대기

- 실제 파인튜닝: training image 미빌드, HF 원본/분리된 실데이터 미준비.
  현재 6GiB worker RAM에서 QLoRA 8B 프로필의 10,000MiB 조건은 충족하지 않습니다.
  trainer 코드와 별도 이미지 stage를 제공했지만 학습 성공이나 품질 개선을 주장하지 않습니다.
- PostgreSQL 직접 연결: 커넥터/UI/준비 스크립트까지 구현. DB 로그인 역할 생성,
  지정 테이블 SELECT 권한, Connection 비밀번호 저장은 자동 승인 검토가 명시 승인 부족으로
  거부했습니다. 실행하지 않았으며 기존 관리자 계정으로 우회하지 않았습니다.
- 다중 GPU/호스트 운영, Airflow 다른 버전/인증 관리자, proxy 하위 경로 배포, Grafana 알림
  엔진/PromQL/Loki/보드별 ACL, 학습 artifact의 Ollama 등록/서빙 승격은 미검증 또는 미구현입니다.

재현 명령과 운영 계약: [플러그인 안내](../../orchestration/airflow/plugins/README.md).
