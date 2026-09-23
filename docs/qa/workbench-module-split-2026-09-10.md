# 대시보드 / Model Lab 코드 분리 검증

검증일: 2026-09-10. Airflow 3.3.1 / 기존 PostgreSQL 메타 DB / 로컬 HTTP 모델 워커.

## 변경

구현을 `airflow_workbench/dashboard`, `model_lab`, `shared`로 나눴다. `app.py`는 라우터 조립만 담당하며 페이지별 HTML·상태·초기화·전용 정적 파일을 분리했다. Model Lab은 대시보드 차트·배치·쿼리 편집기 번들을 로드하지 않는다. 대시보드도 Model Lab 화면과 학습 런타임을 로드하지 않는다.

기존 URL, PostgreSQL 저장 기록, 계정별 대시보드, 프로젝트 권한 및 테마 연동은 유지했다. 기존 DAG·trigger·워커 import 경로는 작은 호환 모듈로 연결한다. 학습 이미지 COPY 경로도 새 구조로 수정했다.

자세한 구조와 수정 위치: [MODULES.md](../../orchestration/airflow/plugins/MODULES.md).

## 자동 검증

- 기존 Python 회귀 테스트 90개 통과: Workbench, 계정별 대시보드, Studio, 쿼리, portable executor, batch, executor state, Model Lab workflow, storage.
- 분리 검증 5개 통과: 상대 기능 없이 import, 페이지별 자산 및 HTTP/CSP, 기존 직렬화 경로의 클래스 동일성, metadata CLI 진입점, Airflow/DB 없는 워커 계약 import.
- 테마 JavaScript 테스트 5개 통과.
- 이동된 `dashboard/frontend`에서 `npm run build` 성공.
- shared/dashboard/model_lab 정적 JavaScript 문법 검사 통과.
- API 서버, triggerer, DAG processor, model-worker 재시작 후 health 및 시작 로그 정상.

## 실제 DAG 실행

기존 `model-lab-verification` 프로젝트의 생성 평가 1건을 `qwen3:8b`, 최대 32토큰으로 재실행했다. `queued → running → success`와 native task의 `deferred` 상태를 확인했다. 이동된 평가 코드가 API → DAG → triggerer → Ollama → 결과 저장까지 이어진다.

- 실행: `2d2cf89a667546748eacf41410bb93c4`
- DAG: `model_lab_eval_default_ollama`
- DAG run: `model_lab__2d2cf89a-6675-4674-8eac-f41410bb93c4`
- 사례 1개 완료, 실행 오류 0개. 짧은 동작 확인 사례이며 모델 품질 벤치마크가 아니다.

## 실제 화면

- Airflow 왼쪽 대시보드 메뉴: 저장된 개인 보드 v9와 7개 패널, 실제 DAG 통계·차트·실행 기록 표시 확인. 보드 설정은 수정하지 않음.
- 왼쪽 Model Lab 메뉴: 기존 챗봇 모델 개발 프로젝트, 데이터·실험 기록 표시 확인.
- 저장소 탭에서 기존 프로필의 데이터 `/datasets/training`, 모델 `/models` 읽기 및 결과 `/state/artifacts` 쓰기 접근 확인 성공.
- 실험 → 학습 후보 만들기: 실행 환경·저장소 프로필 선택과 학습 파라미터, 데이터/모델/결과 경로 미리보기 표시 확인. 학습 시작은 하지 않음.

![대시보드](images/workbench-split-dashboard-2026-09-10.png)

![Model Lab 저장소](images/workbench-split-model-lab-2026-09-10.png)

## 범위

이번 검증은 코드 분리의 회귀 확인이다. GPU 학습 이미지를 새로 빌드하거나 실제 파인튜닝을 수행한 검증은 포함하지 않는다. 모델 자동 패키징·서비스 배포·재색인·롤백 완성을 의미하지 않는다.
