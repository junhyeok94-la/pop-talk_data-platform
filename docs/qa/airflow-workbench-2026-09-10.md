# Airflow Workbench 구현·검증

검증일: 2026-09-10 (Asia/Seoul)

## 적용 결과

- Airflow 3.3.1 / FAB 환경의 탐색 메뉴에 Workbench Dashboard와 Model Lab을 등록했다.
- `plugins/workbench_plugin.py`가 FastAPI 앱과 내부 iframe 메뉴를 등록한다.
- 별도 웹 서버·Streamlit·Grafana·CDN·프론트 빌드 없이 플러그인 폴더에서 실행한다.
- API 서버 컨테이너에 영속 저장소와 datasets 읽기 전용 마운트를 추가했다.
- 기존 DAG/챗봇/학습 가중치/서빙 벡터는 변경하지 않았다.

## 검사 결과

| 검사 | 결과 |
|---|---|
| Workbench 단위/API 검사 | 17개 통과 |
| 기존 Airflow DAG 계약 | 6개 DAG 통과 |
| `airflow plugins --output json` | `airflow_workbench`, 2개 external_views, `/workbench` FastAPI 등록 |
| 익명 화면/API/JS 요청 | 인증 거부 |
| Admin 읽기·편집 | 허용 |
| 변경 요청 헤더 누락·타 출처 | 거부 |
| 잘못된 파라미터·경로 이탈·holdout | 거부 |
| 동시 대시보드 저장 | version 충돌 409 |
| API worker 간 GPU 실험 | lease 직렬화·중단 후 복구·불확실한 통신 실패 시 잠금 유지 |
| 학습 재전송 | 같은 요청은 기존 DAG run 조회, 다른 데이터 hash는 409 |
| 학습 DAG 미연결·paused | 실행 거부 |
| 요청 크기 | Content-Length 없는 chunked 요청도 256KB 제한 |
| JavaScript | `node --check` 통과 |
| 브라우저 | Airflow 메뉴 및 iframe 표시, 패널 추가/삭제/제목 저장, 새로고침 후 유지, 생성 프리셋 저장, 임베딩/학습/이력 탭, 실험 결과 조회 확인 |

## 실제 모델 연결 실험

작은 연결 확인용 입력을 사용했다. 아래 수치를 모델의 일반 품질/성능 벤치마크로 해석하지 않는다.
지연시간은 첫 로딩을 포함한 단일 관측값이다.

| 모델 | 실험 ID | 결과 |
|---|---|---|
| `bge-m3:latest` | `9de8a0ab24f044ad9876d2c58b8e2dc9` | 1024차원, 18.904초, 두 가상 문서 중 정답 검색 확인 |
| `qwen3:8b` | `8d53df5752894343a55fe29b856ff0e7` | `연결 테스트 성공` 출력, 59.589초 |

BGE-M3 digest: `7907646426070047a77226ac3e684fbbe8410524f7b4a74d02837e43f2146bab`

Qwen3 digest: `500a1f067a9f782620b40bee6f7b0c89e17ae61f686b92c24933e4ca4b2b8b41`

실험 원본은 `.local/airflow-workbench/workbench.sqlite3`에 보관한다. 화면의 실험 이력에서
입력·설정·digest·결과를 확인하고 JSON으로 내보낼 수 있다.

최종 재시작 후 브라우저의 임베딩 평가 버튼으로 한국어 질문·가상 문서 4건을 직접 실행해
1024차원 결과와 순위를 확인했다 (10.92초). 생성 프리셋의 Temperature=0.3이 재시작 후
다시 불러와지는 것도 확인했다. 브라우저의 Workbench 관련 console error는 없었다.

## 현재 범위와 후속 연결

- 대시보드는 설치당 하나를 공유하며 8개 내장 소스의 패널을 편집한다. 범용 SQL/외부 데이터소스,
  알림/여러 dashboard 기능은 현재 범위에 포함하지 않는다.
- 파인튜닝은 **레시피·검증·DAG 실행 연결 인터페이스**를 구현했다. 학습 실행기 자체는 포함하지
  않는다. 현재 프로젝트의 학습 원본 가중치·검토된 학습 데이터·GPU 학습 DAG는 미준비 상태다.
- 임베딩 평가는 입력 후보 집합 내 검색 평가다. 실제 RAG 전체 평가나 Golden QA 자동 채점은 아니다.
- 기본/튜닝 모델 2개 비교는 API 테스트로 검증했고, 실제 설치 모델에는 튜닝 모델이 없어
  실제 GPU 비교는 각각의 기본 모델 단일 실행으로 확인했다.
- 저장소는 단일 호스트용 SQLite다. 다중 호스트 운영이나 Airflow 버전 변경은 별도 검증이 필요하다.
- 네트워크 호출을 포함한 학습 실행은 실제 trainer 준비 후 검증해야 한다.

설치/재사용/학습 실행기 계약: [플러그인 안내](../../orchestration/airflow/plugins/README.md).

화면 확인: [대시보드](airflow-workbench-dashboard-2026-09-10.png),
[Model Lab](airflow-workbench-model-lab-2026-09-10.png),
[학습 제어판](airflow-workbench-training-2026-09-10.png).
