# 대시보드·Model Lab 코드 리뷰

검토일: 2026-09-10. 운영 코드 수정 없이 소스 검토, 기존 테스트, mock 기반 재현을 수행했다. 화면을 직접 조작하거나 실제 학습·모델 추론·DAG 제출은 하지 않았다.

## 확인된 개선 사항

### 1. [P1] 학습 산출물과 평가 모델의 연결을 검증하지 않는다

위치: orchestration/airflow/plugins/airflow_workbench/model_lab/api.py:812-835.

register_model은 학습 실행의 kind/status/experiment_id만 확인한다. 실제로 평가한 서빙 모델 digest가 해당 학습 산출물에서 만들어졌는지는 확인하지 않는다. 동일 실험의 Qwen 베이스 평가에 무관한 adapter 학습 실행과 다른 artifact_uri를 붙여도 등록된다. 이 기록을 파인튜닝 효과의 증거로 사용할 수 없다.

개선: 학습 산출물 체크섬 → 변환·양자화 산출물 체크섬 → 서빙 모델 digest → 평가 run의 모델 digest를 연결한 manifest를 검증한다. 외부에서 준비한 모델은 별도 준비 모델로 등록하되 검증되지 않은 학습 연결임을 명시한다.

재현: workbench-review-reproductions.py의 test_register_accepts_unrelated_training_artifact.

### 2. [P2] 서로 다른 K의 검색 점수를 동일 조건 비교로 열 수 있다

위치: model_lab/api.py:792-800, model_lab/static/lab.js:609-627.

비교 API는 데이터셋 해시 일치만 필수 검사한다. 동일 데이터셋에서 baseline k=1, candidate k=10도 허용한다. 같은 순위라도 정답이 두 번째면 Recall@1=0, Recall@2=1이므로 검색기 개선 없이 값이 상승한다. UI는 실제 K를 드러내지 않고 동일 평가셋으로 표시하며 시스템 프롬프트 차이만 별도 설명한다.

개선: 검색 지표를 비교할 때 동일 K와 scorer 버전을 요구하거나, 다른 조건 비교로 명시하고 Recall@1/Recall@10을 별도 열에 표시한다. 변화량을 모델 개선으로 해석하지 않는다.

재현: test_compare_accepts_different_retrieval_cutoffs.

### 3. [P2] Ollama 모델을 매 질문·임베딩 배치마다 내린다

위치: model_lab/evaluation.py:172,265.

generate와 embed의 각 요청에 keep_alive=0을 고정한다. 순차 평가인데도 질문마다 모델을 다시 적재하며, 임베딩은 코퍼스 16개 묶음마다, 이어지는 질의마다 반복한다. 현재처럼 모델 첫 로딩이 긴 환경에서 평가 시간이 늘어나고 mean_latency_ms에 매번 로딩이 포함된다. 웜 상태의 실제 챗봇 응답시간과 비교하기 어렵다.

개선: GPU 예약을 보유한 실행 동안 모델을 유지하고 정상 완료·확정 취소 후 한 번 내린다. 응답 불확실 상태에서는 진행 중인 추론을 방해하지 않도록 기존 예약 정책을 유지한다. 콜드 로딩과 웜 추론 지연을 구분해 기록한다.

재현: test_generation_unloads_model_after_every_case. 실제 시간 증가는 이번 리뷰에서 측정하지 않았다.

### 4. [P2] 사람의 채점이 기준선 비교에 반영되지 않는다

위치: model_lab/api.py:771-775, model_lab/static/lab.js:609-627.

사람 검토는 case_reviews에 저장되지만 compareResults는 row.score만 비교한다. 현재 프로젝트의 draft QA는 자동 score가 null이므로, 사람이 baseline 실패/후보 통과로 채점해도 비교는 미산정이다. 개별 사례에는 사람 판정이 보이지만 개선·회귀 비교에 사용할 수 없다.

개선: 자동 조건 점수와 사람 판정을 합치지 말고 각각의 비교 열과 검토 완료 건수를 제공한다. 같은 case_id에서 양쪽 사람 판정이 존재할 때만 사람 평가 변화량을 계산한다. 이 항목은 저장·표시 경로의 정적 검토로 확인했으며 브라우저 재현은 하지 않았다.

## 구조 평가

WorkbenchPlugin은 하나이며 app.py가 dashboard/model_lab/shared 라우터를 조립한다. 최상위 호환 모듈은 구현 복제가 아니라 새 모듈의 재노출이다. 평가 DAG는 모델 라이브러리를 로드하지 않고 원격 추론을 사용한다. 프로젝트 권한, 요청 ID 멱등성, 고정 데이터셋 체크섬, 미라벨 지표 제외, 운영 상태와 품질 승인 분리 구조가 구현돼 있다.

대시보드는 DAG 권한 범위를 포함한 집계 캐시, PostgreSQL 집계, 전용 읽기 연결과 시간 제한을 사용한다. 검토한 범위에서 별도의 확정 결함을 보고하지 않는다. 화면 시각·상호작용 QA 또는 모든 경로의 보안 검증 완료를 의미하지 않는다.

## 검증

- 기존 test_model_lab_workflow, test_model_lab_storage, test_workbench_performance, test_workbench_modules: Airflow API 컨테이너에서 42건 실행, 39건 통과, 3건 실행환경 import 오류.
- 오류 3건: storage 테스트가 요구하는 worker/train 모듈이 API 컨테이너 import 경로에 없어 ModuleNotFoundError. 제품 결함으로 분류하지 않는다. 기존 테스트는 격리된 wb_test_* 스키마와 임시 파일을 사용한다.
- 추가 재현 3건 통과: 결함이 없다는 뜻이 아니라 위의 허용 동작과 요청 설정이 실제로 재현됐다는 뜻이다. 추가 재현은 DB·추론·DAG 호출을 mock 처리했다.
- 추가 파일: workbench-review-reproductions.py. 기존 애플리케이션과 데이터셋은 변경하지 않았다.

우선순위: 모델 산출물 추적 → 비교 조건/사람 채점 → 모델 로딩 최적화. 현재 도구는 베이스라인 수집에 활용할 수 있지만 파인튜닝 성능 개선을 입증하려면 위 비교 계약부터 보완해야 한다.
