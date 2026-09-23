# Model Lab 개발 흐름 검증

날짜: 2026-09-10. Airflow 3.3.1 / PostgreSQL 17 / FAB / 현재 로컬 compose에서 확인.

## 검증한 흐름

- 프로젝트별 기록과 역할 검사, 개인 초안, 불변 평가셋 버전, 동일 요청 중복 방지와 제출 복구.
- 고정된 평가 입력과 artifact signature 대조, 결과 없이 수동 성공 처리한 DAG의 모델 등록 차단.
- 초안·미라벨·미판정 검색 문서의 점수 제외, 부분 JSON 조건, graded relevance, 사람 검토.
- 원격 제공자의 요청 계약, 학습 설정·환경 fingerprint·Pool·Kubernetes/HTTP 계약, 실행기 상태·예약.
- Qwen 생성 평가 2건 → 기준선/후보 비교 → 사례 검토 → 모델 등록 → 검토 → 적용 구성 생성.
- BGE-M3 실제 문서·질문 임베딩 → 검색 순위·지표 기록.

실제 실행 결과: [JSON 기록](../../.local/qa/docs/model-lab-workflow-smoke-2026-09-10.json). 기능 확인용 프로젝트와 사례로 수행했으며 챗봇 일반 품질 평가가 아니다.

## 자동 검사

```text
python -m unittest test_model_lab_workflow test_airflow_workbench test_workbench_portable test_workbench_batch test_executor_state
52 tests: PASS

node --test scripts/test_workbench_theme.cjs
5 tests: PASS
```

테스트는 실제 PostgreSQL의 무작위 `wb_test_*` 격리 스키마를 만들고 정리한다. batch_entrypoint 모듈은 테스트 환경의 PYTHONPATH에 포함해야 한다. 초기 실행에서 이 모듈 경로가 누락되어 테스트 로딩 오류가 있었고 경로를 수정한 전체 실행이 통과했다.

## 실제 모델 실행

`scripts/smoke_model_lab_workflow.py`가 실행 ID 중복 제출, Airflow 상태/deferral, 실제 응답, 모델 등록과 구성 내보내기를 검증했다. 로컬 추론 연결은 `qwen3:8b`, `bge-m3:latest`; GPU Pool과 기존 HTTP 워커 예약을 사용했다. 각 실행의 메타데이터는 PostgreSQL, 결과 파일은 기존 workbench 공유 볼륨에 저장됐다.

## 프로젝트 준비

`챗봇 모델 개발` 프로젝트에 질문 해석 16개·가상 근거 답변 6개 QA 초안과 두 실험을 등록했다. 프로젝트 전용 내용은 `datasets/evaluation/model-lab-profile.json`에 두며 플러그인 공통 코드에는 영화 필드나 모델 이름을 기본 전제로 넣지 않는다. 검색/DW QA는 실제 코퍼스·정답 데이터가 준비되지 않아 검증된 평가셋으로 등록하지 않았다.

## 화면 확인

Airflow 좌측 메뉴에서 Model Lab을 열어 프로젝트 전환, 평가셋 선택, 고급 옵션 입력, 실제 Qwen 평가 제출과 결과 조회를 확인했다. UI로 제출한 실행 `78e24cccc8494e4587131003a08c4a76`도 성공했고 결과에 실제 적용 옵션과 모델 digest가 기록됐다. 학습 폼의 SFT/임베딩 방법 제한을 확인했다. Light/Dark 표시를 확인한 뒤 기존 시스템 설정 따르기와 챗봇 개발 프로젝트로 복원했다.

화면: [Light 개요](images/model-lab-workflow-light-2026-09-10.png), [Dark 실행 결과](images/model-lab-workflow-dark-2026-09-10.png).

## 남아 있는 검증·기능 경계

실제 GPU 파인튜닝, HF 산출물의 추론 패키징, 실제 원격 OpenAI/Gemini 계정, Kubernetes 클러스터 실행, 다중 호스트의 공유 파일시스템/triggerer 장애 전환은 이번 실제 실행으로 검증하지 않았다. 제공 학습 이미지·데이터·원본 가중치 준비가 필요하다.

적용 구성 생성은 실제 서비스 배포가 아니다. 자동 패키징·재색인·서비스 배포·운영 적용 확인·자동 롤백은 후속 구현 범위다. 기존 대시보드의 `experiments` 데이터셋은 이전 실험 기록을 조회하며, 신규 평가 DAG의 실행 상태는 Airflow DAG 데이터셋으로 모니터링할 수 있다.
