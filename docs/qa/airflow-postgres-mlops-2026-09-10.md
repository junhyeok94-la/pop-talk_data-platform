# Airflow PostgreSQL · 환경별 MLOps 검증 — 2026-09-10

## 반영된 구조

- Airflow 왼쪽 사이드바의 대시보드/Model Lab 경로를 유지한다. 별도 상단 Workbench 메뉴나
  Grafana 서비스 없이 기존 ECharts/GridStack/CodeMirror 번들을 사용한다.
- 개인 대시보드·이력·템플릿·마지막 선택, 공용 보관본, 프리셋·실험·환경 설정을
  Airflow의 PostgreSQL `workbench` 스키마로 이전했다.
- 모델 워커는 **기존 bearer → 플러그인 상태 API → 같은 PostgreSQL**을 사용한다.
  워커용 PostgreSQL 로그인·비밀번호·직접 DB 연결은 만들지 않았다.
- HTTP/Kubernetes 환경 등록, 환경별 Connection·DAG prefix·Pool·동시 실행·timeout·
  CPU/RAM/GPU/이미지 설정, DAG 파일 배포/내려받기를 제공한다.
- 학습 데이터·가중치·산출물은 실행 대상이 관리한다. Airflow의 학습 데이터 마운트를 제거했다.
- pipeline 실행 상태는 Airflow API, 비교·임베딩 결과는 workbench.experiments,
  제공 워커의 작업/예약은 executor_jobs/executor_reservations에서 읽는다.

## 이전 및 보존

플러그인 원본 SQLite를 읽기 전용으로 열어 한 트랜잭션으로 이전하고 모든 행을 대조했다.

| 이전 데이터 | 행 수 |
|---|---:|
| documents | 12 |
| studio_boards | 4 |
| studio_revisions | 8 |
| studio_templates | 1 |
| studio_preferences | 1 |
| experiments | 5 |
| leases | 0 |
| 워커 jobs | 4 |
| 워커 reservations | 0 |

플러그인 데이터 해시: `52cde0eb817b522e36ba4f0049dd6167c4bdbf7e80609f829fa1eef1fc90a911`.
워커 데이터 해시: `57d97592f84a109b849ff2c3488a5cac6f63ad2b0176901a4a77b8ba5bd85541`.
원본 SQLite 파일은 보관했으며 이후 실행에서 갱신되지 않았다.
최종 감사 시 documents는 환경 설정/이전 marker를 포함해 14건, executor_jobs는 진단/취소
검증을 포함해 7건이었다. 개인 보드 4건·이력 8건·템플릿 1건·실험 5건을 확인했다.
진행 중 작업/예약은 없었다. 상세: [저장소 감사](../../.local/qa/docs/airflow-postgres-storage-2026-09-10.json).

## 자동 검사

통합 회귀 70개 통과 후, 빈 워커 산출물 디렉터리의 첫 실행 검사를 추가하고 워커 8개를
다시 통과했다. 총 71개 고유 검사이며 실제 PostgreSQL 임시 스키마에서 실행 후 정리한다.

| 검사 | 수 | 결과 |
|---|---:|---|
| Workbench 인증·저장·실험·학습 계약 | 17 | 통과 |
| Airflow 계정별 대시보드 격리·동시 저장 | 9 | 통과 |
| Studio 빌더·차트 구성·권한·시간 범위 | 12 | 통과 |
| 이전 SQL 차단·PostgreSQL 쿼리 검증·보드 계약 | 6 | 통과 |
| 환경 버전·DAG 생성/활성화·외부 제출·Kubernetes 선언/완료 | 10 | 통과 |
| 상태 API 인증·다른 환경 접근 차단·원자적 admission | 6 | 통과 |
| 워커 자원·예약·취소·체크섬·첫 실행 | 8 | 통과 |
| batch 데이터 체크섬·완료 manifest·부분 업로드 실패 | 3 | 통과 |

DAG 구조 검사: 기존 파이프라인 6 + 생성 Model Lab 3 = 9개 파싱/구조 계약 통과.
JavaScript 구문 검사 통과. UI 콘솔 오류 없음.
초기에는 장기 실행 DAG processor에 이전 모듈이 남아 import가 실패했으며, 프로세스 갱신 후
3개 관리 DAG의 `contract:v2`/환경/fingerprint tags가 현재 설정과 일치했다.

## 실제 실행과 UI

Airflow 3.3.1/FAB/PostgreSQL 17, 로컬 RTX 3060 12GiB, 별도 워커 RAM 6GiB/CPU 2 환경이다.

- 개인 보드 4개의 실제 쿼리가 정상 실행되고 Airflow 좌측 메뉴에서 두 화면이 열린다.
- HTTP/Kubernetes 전환에 따라 Connection/Pool 및 Pod 이미지·자원 설정이 나타난다.
- UI 진단 DAG `workbench__044fe460-2b07-4ceb-9df9-8e32bcff9f8a` 성공.
- 최종 실행 검사 DAG `workbench__f260825f-8997-4b7c-a4bb-0d532d376419` 성공.
  외부 작업 `1d489c280fbe50e09b68c38a8cf1b69e`에서 CUDA 64MiB 할당·해제 확인.
- Airflow task가 deferred 상태로 전환되어 작업 프로세스를 해제하는 동안 Pool 1 slot을
  유지했다. 같은 request ID 재요청은 같은 DAG run을 반환했다.
- VRAM 부족 요청 거부, 중복 작업/추론 예약의 동시 점유 방지, 취소 후 late success 방지,
  미인증 워커 요청 401을 확인했다. [실제 실행 결과](../../.local/qa/docs/airflow-postgres-executor-2026-09-10.json).
- 파인튜닝 사전 검사는 선택 환경의 paused DAG, RAM 부족, 데이터·가중치·패키지 부재를
  표시하고 학습 요청 버튼을 비활성화했다. 실제 학습은 시작하지 않았다.

![환경 설정](images/model-lab-environments-2026-09-10.png)

![학습 준비 검사](images/model-lab-preflight-2026-09-10.png)

## 검증 범위

Kubernetes는 설치된 provider의 native operator로 선언/입력/완료 계약을 검사했다.
실제 클러스터 제출, training/batch 이미지 빌드, 모델별 파인튜닝은 수행하지 않았다.
참조 batch는 파일당 20MiB JSONL과 S3/GCS adapter를 제공한다. 대형/분산 학습이나 다른
스토리지는 해당 환경의 실행 이미지/Job API 구현과 IAM을 준비해야 한다.
모든 학습기의 step별 loss 스트림 수집과 모델 registry/승격은 이번 구현 범위에 없다.
직접 SQL용 `workbench_ro_airflow` Connection은 등록되지 않아 연결 대기로 표시된다.
Airflow API 빌더와 플러그인의 PostgreSQL 저장에는 이 별도 Connection이 필요하지 않다.

설치·API 계약·복구 설명: [플러그인 README](../../orchestration/airflow/plugins/README.md).
