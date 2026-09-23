# 대시보드·운영 모니터링·Model Lab 코드 구조

세 메뉴는 하나의 Airflow 플러그인으로 등록하지만 구현·API·화면 시작 코드·전용 정적 자산을 분리합니다. `app.py`는 공통 처리와 기능별 라우터를 조립합니다.

```text
airflow_workbench/
├── app.py                      # 라우터 조립과 기존 import 호환
├── shared/
│   ├── auth.py                 # Airflow 권한·CSRF 검사
│   ├── identity.py             # Auth Manager와 계정 ID로 개인 소유자 결정
│   ├── airflow_api.py          # 사용자 자격으로 native API 요청
│   ├── database.py             # PostgreSQL 연결과 공통 예외
│   ├── metadata.py             # 기존 스키마와 마이그레이션
│   ├── contracts.py            # 작은 공통 입력 타입
│   ├── execution_contract.py   # 오케스트레이션·모니터링의 DAG 이름 계약
│   ├── web.py / assets.py      # 응답 정책과 정적 파일 허용 목록
│   └── static/                 # UI·테마·도우미, host.js React 연결부와 navigation.js
├── dashboard/
│   ├── api.py                  # 대시보드 페이지·조회·계정별 저장 API
│   ├── work.py                 # 두 탭의 계정별 담당 범위·검색·필터, 업무 목록 집계
│   ├── actions.py              # native Clear 미리보기·재실행, DAG 활성화·일시중지
│   ├── studio.py               # 패널·보드·빌더·시각화 데이터 처리
│   ├── aggregate.py            # 권한으로 제한된 PostgreSQL 원본의 일괄 집계
│   ├── performance.py          # 요청 권한·조회 기준 시각·집계 라우팅
│   ├── cache.py                # PostgreSQL 결과 캐시·중복 병합·동시 집계 제한
│   ├── postgres.py             # 등록 PostgreSQL 조회 소스
│   ├── mlops_source.py         # 실행 환경 메타데이터의 읽기 전용 조회 연동
│   ├── schemas.py / boards.py / legacy_store.py / query.py
│   ├── static/                 # workspace.js 공통 필터/갱신, work, Studio, vendor
│   └── frontend/               # ECharts·GridStack·CodeMirror 빌드와 lockfile
├── monitoring/
│   ├── api.py                  # 관리자 URL 등록·화면별 frame-src 정책
│   └── static/                 # URL 설정·빈 상태·외부 iframe 표시
└── model_lab/
    ├── pages.py                # Model Lab 페이지·화면 설정 API
    ├── api.py                  # 프로젝트·평가·실험·모델·저장소 API
    ├── operations_api.py       # 실행 환경·DAG·Pool·학습 제어 API
    ├── legacy_api.py           # 이전 평가·프리셋·복합 snapshot API
    ├── contracts.py / schemas.py / store.py / legacy_store.py
    ├── evaluation.py / evaluation_dag.py / models.py
    ├── environments.py / dag_factory.py / mlops.py
    ├── training.py / storage.py / portable_training.py
    ├── worker_client.py / executor_state.py / kubernetes_executor.py
    └── static/                 # index, bootstrap, runtime, Lab, MLOps
```

## 의존 방향

- `dashboard`와 `model_lab`은 각각 `shared`에 의존합니다. 기능 구현은 상대 기능의 Python 런타임을 import하지 않습니다.
- 대시보드의 Model Lab 관측은 PostgreSQL에 저장된 실행 환경 계약을 읽는 `dashboard/mlops_source.py`를 통해 이어집니다. 기존 실험 데이터 조회·DAG 상태 조회도 유지합니다.
- 각 HTML은 `shared`와 자기 기능의 자산만 로드합니다. 대시보드는 학습 레시피·Model Lab JS를 로드하지 않고, Model Lab은 ECharts·GridStack·CodeMirror 번들을 로드하지 않습니다.
- 공통 `ui.js`에는 인증된 API 호출, 알림, 표·날짜 형식 등의 도우미만 있습니다. 화면 상태와 초기화는 각 기능의 `runtime.js`·`bootstrap.js`가 소유합니다.
- 학습 워커는 `model_lab.schemas`, `training`, `storage`, `portable_training`을 사용합니다. 이 모듈들은 Airflow·대시보드·메타 DB를 import하지 않습니다. batch 이미지에는 필요한 계약 파일만 복사합니다.

## 유지되는 계약

- Airflow 왼쪽 메뉴: `/plugin/workbench-dashboard`, `/plugin/workbench-monitoring`, `/plugin/workbench-models`.
- 플러그인 페이지: `/workbench/dashboard`(담당 DAG), `/workbench/dashboard?view=charts`, `/workbench/monitoring`, `/workbench/models`.
- 기존 대시보드·Model Lab·워커 API 주소와 PostgreSQL 테이블·스키마·저장 기록.
- 계정별 대시보드, 프로젝트별 Model Lab 권한, Airflow Light/Dark/System 테마.
- 기존 `/api/config`는 호환용으로 유지합니다. 새 화면은 `/api/dashboard/config`와 `/api/model-lab/config`를 각각 사용합니다.

최상위 `lab_api.py`, `lab_dag.py`, `mlops.py`, `training.py` 등의 작은 파일은 이전 DAG·trigger 직렬화 경로와 외부 실행기 import를 위한 호환 모듈입니다. 실제 코드를 복제하지 않고 새 모듈을 가리킵니다. 새 코드는 기능 폴더 경로로 작성하세요. `metadata` 명령행 마이그레이션 진입점도 유지합니다.

이번 분리는 코드 경계 정리입니다. 독립 배포용 Airflow 플러그인 두 개로 패키징하거나 별도 서비스를 설치한 변경은 아닙니다.

## 수정·검증

대시보드 UI·SQL·차트 작업은 `dashboard`, 외부 운영 화면 연결은 `monitoring`, 모델 평가·학습·저장소 작업은 `model_lab`에서 합니다. `shared` 변경은 세 메뉴를 함께 검증합니다. 담당 DAG 첫 화면과 운영 모니터링은 차트 번들을 로드하지 않습니다.

```powershell
cd orchestration/airflow/plugins/airflow_workbench/dashboard/frontend
npm ci
npm run build
```

위 빌드는 `dashboard/static/studio-vendor.*`와 라이선스 안내를 갱신합니다. Model Lab 변경에는 이 빌드가 필요하지 않습니다. `node_modules`는 배포하지 않습니다.

분리 검증은 `orchestration/airflow/tests/test_workbench_modules.py`에 있습니다. 상대 기능을 import하면 실패하는 조건에서 각 API를 로드하고, HTML이 자기 자산만 읽는지, 기존 DAG import와 metadata CLI가 유지되는지 확인합니다.
