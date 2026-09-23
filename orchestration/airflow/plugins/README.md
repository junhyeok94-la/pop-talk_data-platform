# Airflow Workbench

화면별 기능과 사용 순서는 [화면 사용 안내](USER_GUIDE.md)를 참고하세요.
현재 Model Lab의 프로젝트·평가셋·실험·모델 개발 순서는 [Model Lab 작업 안내](MODEL_LAB.md)를 참고하세요.
대시보드와 Model Lab의 기능별 폴더·공통 코드·호환 경로는 [코드 구조 안내](MODULES.md)에 정리했습니다.
대시보드의 집계·캐시 구조와 측정 범위는 [성능 안내](DASHBOARD_PERFORMANCE.md)를 참고하세요.
Model Lab의 공용 MLOps 흐름과 구현 우선순위는 [설계 검토안](../../../docs/model-lab-workflow-review-2026-09-10.md)에 정리했습니다. 이 문서의 제안 기능은 현재 구현 범위와 구분합니다.

Airflow **왼쪽 사이드바 → 내 대시보드 / 운영 모니터링 / Model Lab**에서 엽니다.
별도 상단 Workbench 메뉴는 없습니다. Airflow 3.3.1의 `react_apps.nav_top_level`,
`url_route`, FastAPI plugin mount를 사용하며 주소는 `/plugin/workbench-dashboard`,
`/plugin/workbench-monitoring`, `/plugin/workbench-models`입니다.

**내 대시보드**의 첫 화면은 계정별 담당 DAG와 업무 태그, 실패·장기 실행 목록,
실패 태스크 미리보기와 재실행을 제공합니다. 기존 개인 보드와 편집기는 페이지 안의
**차트·추세**에서 사용합니다. 보드·배치·쿼리·저장 이력은 그대로 유지합니다.
넓은 화면에서는 업무 목록을 나란히 배치하며, 담당 DAG의 활성화·일시중지도 지원합니다.
담당 DAG와 차트·추세는 계정별 담당 범위·검색·실행 상태·활성 여부·기간을 공유합니다.
자동 갱신 토글과 수동 아이콘 버튼도 공통으로 제공하며 30초·1분·5분 간격을 선택합니다.
차트의 별도 분석 모드는 SQL·MLOps 등 담당 DAG에 연결할 수 없는 자료를 구분해 조회합니다.
얇은 React 연결부가 플러그인 링크를 Airflow 라우터에 전달하므로 실행 상세에서 다시
Airflow 안에 Airflow가 중첩되거나 iframe 탐색 제한으로 클릭이 무시되지 않습니다.

**운영 모니터링**은 관리자가 등록한 외부 대시보드를 iframe으로 표시합니다.
외부 서비스 설치·메트릭 수집·인증 프록시는 포함하지 않습니다. URL이 없으면 미등록 안내를
표시하며, 외부 로그인·조회 권한은 해당 서비스가 관리합니다.
설정·권한·재실행 범위는 [개인 업무와 운영 모니터링 안내](WORKSPACES.md)를 참고하세요.

## Airflow 테마 연동

`shared/static/theme.js`가 같은 출처의 Airflow 부모 문서에서 해석된 light/dark 상태와
Chakra 공통 색상·본문 글꼴 토큰을 읽고, 루트 속성 변경을 구독합니다. Airflow의
사용자 → 외관 설정이 기준이며 별도의 테마 저장소나 토글을 만들지 않습니다.
시스템 설정을 따르는 경우에도 Airflow가 결정한 최종 테마를 그대로 반영합니다.
플러그인 직접 URL 또는 부모 문서 접근이 불가능한 임베딩에서는 OS 설정과 기본 팔레트를 사용합니다.

ECharts에는 OKLCH 색상을 브라우저에서 sRGB로 변환해 전달하고 기존 차트의 테마만
갱신합니다. 범례 선택·확대 범위·사용자 지정 시리즈 색상은 유지합니다. CodeMirror는
Compartment 재설정으로 문서·선택·실행 취소 이력을 유지합니다. 부모 DOM/Chakra 토큰을
사용하므로 Airflow 업그레이드 때 세 플러그인 경로의 테마 전환을 재확인해야 합니다.
회귀 검증: 프로젝트 루트에서 `node --test orchestration/airflow/tests/test_workbench_theme.cjs`.

## Dashboard Studio

Grafana 서버·iframe·Prometheus 없이 Airflow 플러그인 안에서 동작합니다.
대시보드 편집 기능은 프로젝트 코드로 구현하고, 차트는 Apache ECharts 6.1.0,
배치는 GridStack 13.2.0, PostgreSQL 편집기는 CodeMirror 6을 사용합니다.
라이브러리는 정적 파일로 번들되어 CDN·Node 실행 서버가 필요하지 않습니다.

- **대시보드 편집**: 패널 헤더를 드래그하고 오른쪽 아래 핸들로 너비·높이를 조절합니다.
  12열 좌표와 크기를 저장하며, 충돌 패널은 GridStack이 재배치합니다.
- **패널 편집**: 넓은 미리보기, 데이터·쿼리 영역, 오른쪽 시각화 옵션을 함께 제공합니다.
  패널마다 A–F 쿼리 최대 6개를 실행하고 전체 또는 특정 쿼리를 표시합니다.
- 선·영역·막대·숫자·게이지·도넛·테이블·산점도·히트맵 9종. 다중 값 필드,
  시리즈, 범례 토글, 시간축, 범위 확대·축소, 누적, 보간, 가로 막대, 단위·소수점,
  임계값, 축 범위, 시리즈별 색상·보조축을 설정합니다.
- **고급 ECharts 옵션**: grid/xAxis/yAxis/legend/textStyle/color/backgroundColor/animation
  JSON을 설정합니다. 임의 JavaScript·HTML formatter·외부 데이터 호출은 받지 않습니다.
- **데이터 확인**: 실제 결과 테이블 정렬·페이지 이동·JSON 내보내기와 쿼리 상태를 확인합니다.
- 카테고리별 보드 검색, 변수, 상대·절대 시간 범위, 자동 갱신, JSON 가져오기·내보내기,
  패널 템플릿, 최근 20개 저장 이력 복원을 지원합니다. 복원 후 저장하면 새 버전입니다.
- 편집 적용과 영구 저장은 구분됩니다. 동시 저장 충돌은 409로 알리며 덮어쓰지 않습니다.

처음에는 실제 실행 이력 기반의 `Airflow 운영 모니터링`과 `Model Lab 관측` 보드를 제공합니다.
외부 의존 서비스를 늘리지 않고 직접 개발하는 대시보드 편집기의 현재 구현 범위입니다.
Grafana의 알림 엔진, 대시보드별 ACL, 임의 데이터소스 플러그인 생태계나 실시간 스트리밍까지
동등하게 구현했다고 주장하지 않습니다.

## 계정별 대시보드

**내 대시보드**는 로그인한 Airflow 계정마다 독립적으로 저장됩니다.
패널 배치·쿼리·시각화·변수·기본 조회 기간·자동 갱신 설정과 패널 템플릿,
최근 20개 저장 이력이 계정에 귀속됩니다. 편집 후 **저장**하면 다른 브라우저로
접속해도 같은 구성을 사용합니다. 목록에서 마지막으로 연 보드는 다음 접속에 복원됩니다.

- 최초 접속은 각 계정에 기본 보드 2개를 만듭니다. 다른 계정의 편집본을 상속하지 않습니다.
- 인증된 `get_user().get_id()`와 Auth Manager 종류로 서버가 소유자를 결정합니다.
  표시 이름·브라우저 저장소·요청의 owner 값은 소유권 근거가 아닙니다.
- 보드·이력·템플릿의 모든 조회/쓰기는 소유자와 ID를 함께 비교합니다.
  관리자도 일반 개인 보드 API에서는 다른 계정의 구성을 열 수 없습니다.
- Plugins 조회 권한이 있으면 자신의 대시보드를 편집할 수 있습니다.
  PostgreSQL 소스 등록·SQL 직접 조회·이전 Model Lab API는 관리자/custom 권한을 요구합니다. 신규 Model Lab은 프로젝트 역할과 실제 DAG 권한을 함께 검사합니다.
  개인 구성 저장 권한으로 DAG 데이터 조회 권한이 늘어나지는 않습니다.
- 동일 계정의 여러 창에서 수정하면 버전 검사와 트랜잭션으로 덮어쓰기를 막습니다(409).

계정별 저장 이전의 공용 구성에는 소유자 정보가 없으므로 자동 배정하지 않습니다.
Workbench 관리자는 **내 대시보드 → 기존 공용 구성 → 공용 구성 가져오기**에서
보드·저장 이력·패널 템플릿을 자신의 계정으로 한 번 복사할 수 있습니다.
새 ID를 부여하고 기존 개인 보드를 덮어쓰지 않으며 공용 원본도 유지합니다.
동일 메뉴에서 공용 v3 전체 구성과 이전 v2 보드의 JSON 보관본을 내려받을 수 있습니다.
팀 공유·다른 계정에 대한 편집 ACL은 아직 제공하지 않습니다.

개인 구성은 Airflow PostgreSQL `workbench` 스키마의 `studio_boards`, `studio_revisions`,
`studio_templates`, `studio_preferences` 테이블에 저장합니다. DB 백업에 이 스키마를 포함합니다.
Airflow 계정 ID가 바뀌는 재생성이나 Auth Manager 교체 시에는 별도의 소유권 이전이 필요합니다.

## 데이터소스와 PostgreSQL

**현재 Airflow 메타 DB는 PostgreSQL 17의 `airflow` 데이터베이스입니다.**
SQLite 메모리 DB에 API 결과를 복사해 SQL을 실행하던 중간 계층은 현재 조회 경로에서
제거했습니다. 이전 `/api/query`, `/api/boards/query`는 410을 반환합니다.

| 소스 | 실제 조회 경로 | 쿼리 편집 |
|---|---|---|
| Airflow 실행 이력 | Airflow 권한 검사 → PostgreSQL 읽기 전용 집계·공유 결과 캐시 | 데이터셋·그룹·집계·필터 빌더 |
| Model Lab 실험 기록 | Airflow PostgreSQL workbench.experiments → 기록 API | 데이터셋·그룹·집계·필터 빌더 |
| Airflow PostgreSQL 직접 조회 | 전용 `workbench_ro_airflow` Connection → 모니터링 뷰 | CodeMirror PostgreSQL SQL |
| 프로젝트 PostgreSQL | 등록한 `workbench_ro_*` Connection → 허용 테이블·뷰 | CodeMirror PostgreSQL SQL |

Airflow 빌더는 현재 Auth Manager의 실행 조회 권한과 DAG 목록을 매 요청 확인하고,
허용된 DAG의 PostgreSQL 원본을 읽기 전용 SQL로 집계합니다. 임의 SQL을 받지 않으며
원본은 보드의 패널들이 하나의 materialized CTE로 공유합니다. count/sum/avg/min/max/P95/일치 비율,
여러 그룹·집계, 시간 버킷, 필터, 정렬·표시 필드를 지원합니다.
`percent`는 선택 필드의 null을 제외한 값 중 match와 같은 비율입니다.
완료 실행 성공률은 success/failed 필터를 먼저 적용합니다. P95는 nearest rank입니다.

Airflow 집계는 원본 5,000건 제한 없이 선택 기간 전체를 대상으로 하며, 표시 결과만 쿼리당
최대 2,000행으로 제한합니다. PostgreSQL 집계는 2초 제한, 캐시 결과는 보드당 2MB 제한입니다.
30초 결과 캐시·동일 요청 병합·동시 집계 2개 제한이 모든 API 프로세스에 적용됩니다.
현재 DAG 권한 범위가 같은 요청끼리만 캐시를 공유합니다. 패널에는 조회 기준 시각을 표시합니다.
이전 Model Lab 실험 소스는 최대 5,000개 원본을 읽으며, 초과 시 전체 통계처럼 집계하지 않고
기간 축소 안내를 표시합니다. 직접 SQL과 실험 소스에는 새 Airflow 집계 캐시가 적용되지 않습니다.
긴 보드는 25초가 지나면 다음 쿼리를 중단합니다. 편집 중 자동 갱신은 중지합니다.

SQL은 단일 SELECT/CTE, 등록 함수, 바인딩 변수, 2초 statement timeout과 전용 읽기 계정을
사용합니다. 관리자·쓰기 계정은 거부합니다. SQL 직접 조회는 Workbench 편집 권한이
필요하며, DB 계정의 SELECT 범위가 접근 범위입니다. Airflow의 DAG별 RBAC를 임의 SQL에
자동으로 적용하는 것은 아니므로 API 빌더의 조회 권한과 구분합니다.

```sql
SELECT state, count(*) AS value
FROM workbench_monitor.dag_runs
WHERE run_ts BETWEEN :from_ts AND :to_ts
  AND dag_id LIKE '%' || :dag || '%'
GROUP BY state
ORDER BY state
```

`:from_ts`, `:to_ts`는 Unix 초, `:bucket_seconds`는 버킷 크기입니다. 보드 변수는 `:이름`으로
바인딩하고 문자열에 직접 삽입하지 않습니다. SQL의 시간 조건은 쿼리에 명시해야 합니다.

**직접 PostgreSQL Connection은 아직 생성하지 않았습니다.** 이전 계정 생성 요청은 자동
승인 검토에서 DB 권한·자격 증명 변경으로 거절되었습니다. 현재 직접 SQL 소스는 연결 대기로
표시하고 API 빌더의 실제 데이터로 기본 대시보드를 사용할 수 있습니다.

검토용 변경안:

- `workbench_airflow_ro`: login, 기본 읽기 전용, 연결 4개, 쿼리 2초 제한.
- `airflow.workbench_monitor`의 `dag_runs`, `task_instances`, `dags`, `dag_tags`, `pools`
  뷰 생성 및 이 5개 뷰에만 SELECT. 원본 Connection/Variable/XCom/DAG conf는 노출하지 않음.
- `.local/config/airflow.env`에 `workbench_ro_airflow` Connection과 무작위 비밀번호 보관.
- SQL: `../scripts/workbench-monitoring.sql`. `orchestration/airflow/scripts/prepare-workbench-airflow-monitor.cjs`는
  기본 실행 시 설명만 출력합니다. `--apply`는 위 변경에 대한 사용자 승인 후 실행합니다.
  승인 후 진행할 때 API 서버 재생성 전 실행 중인 DAG를 확인합니다.

보드·프리셋·실험·실행 환경·워커 작업/예약은 모두 **Airflow PostgreSQL의 `workbench`
스키마**에 저장합니다. API 서버가 Airflow에 설정된 기존 DB 연결을 사용합니다.
새 DB 로그인이나 워커용 DB 비밀번호는 만들지 않습니다. 개인 대시보드 동시 저장은
버전 검사로, 실행기 admission은 환경별 PostgreSQL advisory lock과 unique index로 보호합니다.
Airflow core 테이블과 Alembic 버전은 수정하지 않습니다.

이전 `.local/data/airflow-workbench/workbench.sqlite3`, `.local/data/model-worker/jobs.sqlite3`는
이전 검증용 보관본이며 실행 중 읽거나 쓰지 않습니다. 보드 3개의 공용 원본 JSON도 보존합니다.
이전 SQL을 자동 변환하지 않으며 SQLite 조회 엔진은 제거했습니다.

## 정적 라이브러리 빌드

`airflow_workbench/dashboard/frontend`의 package-lock.json에 버전을 고정합니다.

```powershell
cd orchestration/airflow/plugins/airflow_workbench/dashboard/frontend
npm ci --no-audit --no-fund
npm run build
```

결과는 `dashboard/static/studio-vendor.js`, CSS와 THIRD-PARTY-NOTICES.txt입니다.
빌드한 정적 파일을 포함해 플러그인을 복사하면 실행 서버에 npm이 필요하지 않습니다.
`dashboard/frontend/node_modules`는 배포 대상에서 제외합니다.

## Model Lab의 실행 구조

신규 평가는 프로젝트별 **실험 → 평가 DAG → 사례 검토 → 기준선/후보 비교 → 모델 등록**으로 연결됩니다.
추론 제공자와 평가 DAG, 공유 artifact 경로는 [Model Lab 작업 안내](MODEL_LAB.md)에 설명합니다.
환경·연결의 실행 환경 관리에서 **환경 등록 → Pool·DAG 설정 배포 → 필요 DAG 활성화** 순서로 학습을 준비합니다.
실험의 학습 후보 만들기에서 실행 환경과 모델·고정 revision·데이터·학습 파라미터를 선택하고,
설정 검증을 통과하면 학습 DAG를 요청합니다. 환경 이름·Connection·DAG prefix·Pool·작업 수·
timeout은 PostgreSQL에 버전과 함께 저장합니다. 같은 환경을 동시에 편집하면 409로 알려 줍니다.

| 실행 방식 | 실제 작업 주체 | Airflow의 역할 |
|---|---|---|
| 원격 Job API | 선택한 Connection의 외부 실행기 | 제출, deferral, 상태 확인, 완료 결과 검증 |
| Kubernetes Pod | 선택한 클러스터의 학습 이미지/Pod | KubernetesPodOperator로 Pod 제출, deferral, XCom 결과 검증 |

HTTP 환경은 resource profile을, Kubernetes 환경은 이미지·namespace·service account·
CPU/RAM/GPU 요청/한도·node selector를 지정합니다. Pool은 deferred 태스크도 점유하도록
검사합니다. Kubernetes의 실제 자원 할당은 클러스터 scheduler가 담당하며, 사전 연결 검사는
namespace 조회 권한만 확인합니다. 이미지 pull·Pod 생성 권한·quota·가용 GPU는 실제 제출에서 검증됩니다.

학습 코드·torch·가중치를 Airflow 태스크 프로세스에 올리지 않습니다. DAG 파싱에도 DB,
Connection, 네트워크 접근이 없습니다. 로컬 GPU는 등록된 환경 하나일 뿐이며 다른 프로젝트에서도
환경 설정을 통해 Connection과 실행 위치를 선택합니다.

### 상태 저장과 워커 API

```text
Airflow DAG ──작업 제출/조회──> 외부 모델 워커 ──학습──> GPU 프로세스
                                  │
                                  └─기존 bearer로 상태 API 호출
                                      → Airflow API 서버 → PostgreSQL workbench
```

제공 워커는 `MODEL_WORKER_STATE_API_URL=<Airflow URL>/workbench/executor-state`,
`MODEL_WORKER_ENVIRONMENT_ID=<등록한 HTTP 환경 ID>`, 기존 `MODEL_WORKER_TOKEN`을 사용합니다.
토큰은 해당 환경의 Airflow Connection password와 같아야 합니다. 워커에 DB 연결 문자열은
전달하지 않습니다. Airflow 태스크용 `/execution/` API를 확장하거나 대신 사용하지 않습니다.

상태 API는 jobs 생성/조회/상태 전이, reservations 예약/해제, admission 확인, 재시작 복구만
허용합니다. 서버가 환경 ID와 Connection 토큰을 대조하며 모든 DB 조회에 환경 ID를 적용합니다.
대시보드·실험·다른 환경·Connection 조회나 임의 SQL 실행 기능은 없습니다.
사람용 Workbench API는 기존 Airflow 인증과 권한 검사를 계속 사용합니다.

제공 HTTP 워커는 **환경 하나당 프로세스 하나**로 운영합니다(`uvicorn --workers 1`).
재시작하면 해당 환경의 미완료 작업만 interrupted로 처리합니다. 복제 워커를 같은 환경 ID로
동시에 실행하지 않습니다. 여러 GPU 노드/분산 학습은 Kubernetes나 별도 Job API 실행기를
선택합니다. 기존 key와 입력이 같으면 같은 작업을 재사용하며 입력이 다르면 409입니다.
취소 상태는 늦은 결과가 덮어쓸 수 없고 작업과 추론 예약은 원자적으로 상호 배제됩니다.
API 장애 때 로컬 SQLite로 우회하지 않으며, 마지막 상태와 결과 파일로 복구 여부를 판단합니다.

Job API 어댑터의 요청 계약은 `worker.py`/`mlops.py`에 있습니다.

- `GET /resources`: 자원과 active_job, 가용 profile. `GET /jobs`, `GET /jobs/{id}`: 실행 이력/상태.
- `POST /preflight`: `{key, kind, profile, payload}`. payload에 recipe와 recipe_sha256을 전달합니다.
  `ready`, `blockers`와 검증한 train/validation `datasets` 2개를 반환해야 합니다.
- `POST /jobs`: 같은 입력과 확정된 datasets manifest. `{id, status, ...}` 반환 후 비동기 실행.
- `POST /jobs/{id}/cancel`: 외부 프로세스를 종료하고 취소 확정. 성공 결과는
  `training_performed=true`와 `artifact_id` 또는 `artifact_uri`를 반환해야 합니다.
- `POST /reservations`, `POST /reservations/{id}/release`: 제공 워커의 추론/학습 GPU 경합 방지.

다른 실행기를 연결할 때도 이 작업 API 계약을 구현해야 합니다. 임의 클라우드 학습 API를
Connection에 넣기만 하면 자동 호환되는 것은 아닙니다.

### 대시보드 데이터의 출처

파이프라인/학습 실행 상태는 인증된 Airflow API → PostgreSQL 메타 DB 경로에서 읽습니다.
`dag_runs.environment_id`, `workload` 필터로 등록 환경별 Model Lab DAG를 모니터링할 수 있습니다.
생성 비교·임베딩 품질 지표는 PostgreSQL `workbench.experiments`의 실행 결과에서 읽습니다.
HTTP 워커의 작업/예약은 `executor_jobs`, `executor_reservations`에서 상태 API로 읽습니다.
Kubernetes 작업 결과는 해당 DAG의 XCom과 artifact URI로 확인합니다. 모든 학습기의 step별
loss 스트림을 수집하는 공통 학습 지표 백엔드는 아직 제공하지 않습니다.

생성/임베딩 비교는 설정된 Ollama 서버를 사용합니다. 모델 digest·설정·응답·latency·tokens/sec,
임베딩 차원과 수동 정답 기반 Recall@K/nDCG@K/MRR@K를 기록합니다. 정답 미지정은 점수를
계산하지 않습니다. Ollama 연결 여부가 외부 학습 환경 설정을 막지는 않습니다.

## Model Lab DAG 표준과 배포

`dag_factory.py`가 환경 설정으로 DAG를 생성합니다. **Pool·DAG 설정 배포**는 없는 Pool을
생성하고 `dags/workbench_managed/<환경 ID>.py`를 원자적으로 게시합니다. 기존 Pool 설정과
불일치하면 자동 덮어쓰기 대신 차이를 알립니다. DAG 디렉터리를 Git sync 또는 읽기 전용으로
운영할 때는 **DAG 파일 내려받기**로 받은 파일을 기존 배포 과정에 포함합니다.
배포 파일에는 자격 증명이 없으며 환경 설정의 fingerprint를 포함합니다.

| workload | 생성 DAG ID | 실행 |
|---|---|---|
| diagnostic | `<dag_prefix>_diagnostic` | 제공 실행기의 CUDA 64MiB 진단 |
| llm_sft | `<dag_prefix>_llm_train` | 생성 모델 LoRA/QLoRA SFT |
| embedding_contrastive | `<dag_prefix>_embedding_train` | 임베딩 contrastive 학습 |

공통 tags: `model-lab`, `mlops`, `external-executor`, `contract:v2`, `environment:<id>`,
`backend:<type>`, `workload:<kind>`, `config:<fingerprint>`. owner는 `mlops`이고 schedule 없음,
catchup false, 최초 paused, retries 0, DAG당 활성 task 1입니다. Pool slots, DAG 동시 run 수,
실행 제한은 환경 설정을 따릅니다. HTTP는 ExternalModelJobOperator, Kubernetes는
PortableTrainingPodOperator가 외부 제출 후 defer합니다. 입력은 `dag_run.conf.workbench`,
출력은 작은 실행 결과와 artifact 참조입니다. 수동으로 conf를 바꿔 실행해도 실행기에서 재검증합니다.

화면은 환경 fingerprint와 현재 파싱된 DAG의 tags·operator·Pool·timeout을 비교해 최신 설정
불일치를 차단합니다. 환경 설정 변경 후 DAG를 다시 배포해야 합니다. DAG processor 반영에는
시간이 걸립니다. Airflow의 단순 clear/mark가 원격 작업을 즉시 취소하는 것은 아닙니다.
HTTP 작업은 Model Lab의 외부 작업 취소를 사용합니다. Kubernetes는 provider/cluster의
Pod 수명주기와 deadline을 사용합니다. 진단 성공은 학습 성공을 의미하지 않습니다.

## 다른 환경에 설치

검증 환경은 Airflow 3.3.1 / Python 3.12 / FAB / PostgreSQL 17입니다. Airflow 2.x,
다른 auth manager 및 도메인 하위 경로 배포는 미검증입니다.

1. `workbench_plugin.py`, `airflow_workbench/`의 Python 모듈과 빌드된 static 파일을 plugins에
   배치합니다. dashboard/frontend/node_modules는 제외합니다. fastapi/httpx/pydantic>=2/SQLAlchemy,
   PostgreSQL 드라이버와 sqlparse가 필요합니다. Kubernetes는 cncf-kubernetes provider가 필요합니다.
2. Airflow 초기화 단계에서 `PYTHONPATH=<plugins> python -m airflow_workbench.metadata`를 실행합니다.
   Airflow에 설정된 PostgreSQL 연결로 `workbench` 스키마를 준비합니다. Compose airflow-init에
   포함되어 있습니다. PostgreSQL 이외 DB로 자동 대체하지 않습니다.
3. Airflow Connections에 해당 환경의 Job API 또는 Kubernetes 연결을 준비합니다.
   Model Lab에서 환경을 등록합니다. 로컬 예시는 `orchestration/airflow/config/model-lab-local.json`,
   클러스터 예시는 `plugins/examples/model-lab-kubernetes.json`입니다.
4. HTTP 제공 워커라면 상태 API URL·환경 ID·같은 bearer를 설정하고 실행합니다. API 서버에
   연결 가능한 URL을 사용하며 원격 배포는 HTTPS를 사용합니다. 데이터/가중치/산출물 경로는
   워커 배포에서 지정합니다. Airflow에는 학습 데이터 마운트가 필요하지 않습니다.
5. Pool과 DAG를 배포하고 진단·작은 학습으로 해당 실행 환경을 검증합니다. API 서버가 여러
   노드이면 모두 같은 PostgreSQL을 사용하고 DAG 배포는 공유 DAG bundle/Git sync로 관리합니다.

환경 설정은 DB에, 실제 DAG 소스는 배포 파일에 있으므로 DB와 DAG 배포본을 함께 백업합니다.
Connection 자격 증명은 기존 secrets backend/env 관리에 따릅니다. 모델과 학습 데이터는
DB에 바이너리로 넣지 않으며 선택한 파일/객체 저장소를 백업합니다.

### 제공 학습 이미지

HTTP 워커의 `control` stage는 자원 진단용입니다. 현재 로컬 제한은 CPU 2 / RAM 6GiB /
GPU 작업 1개로, QLoRA 8B의 최소 가용 RAM 10,000MiB 조건을 충족하지 않습니다.
다른 환경에서는 실행기 리소스와 profile을 그 환경에 맞춰 설정해야 합니다.

`orchestration/model_worker/Dockerfile`의 `training` stage는 HF 원본 가중치와 JSONL을
읽는 참조 학습기입니다. 모델별 호환성 검증이 필요하며 Ollama GGUF를 HF 원본 대신 사용하지 않습니다.
`batch` stage는 Kubernetes용 `batch_entrypoint.py`를 포함합니다.

- 고정 Hugging Face revision, 학습/검증 URI, 각각의 SHA256, 결과 artifact URI를 받습니다.
- 참조 batch 이미지에는 S3/GCS filesystem adapter가 포함됩니다. 다른 저장소는 학습 이미지에
  해당 adapter/IAM을 준비해야 합니다. 객체 저장소 권한은 workload identity/service account로 부여합니다.
- 각 JSONL을 SHA256으로 검증하고 family_id/동일 내용 중복을 거부합니다. 참조 학습기는
  파일당 20MiB까지이며 더 큰 데이터는 streaming 학습 이미지로 교체해야 합니다.
- 학습 결과·receipt·metrics를 올린 뒤 `_SUCCESS.json`을 마지막에 게시합니다. 업로드 실패는
  성공으로 처리하지 않습니다. 같은 실행/recipe 재시도는 완료 manifest의 artifact를 재사용합니다.
- Pod는 `/airflow/xcom/return.json`으로 training_performed, artifact_uri, recipe_sha256, 작은 metrics를
  반환합니다. 원본 데이터/전체 가중치를 XCom으로 전달하지 않습니다.

**training/batch 이미지를 실제 빌드하거나 파인튜닝을 실행한 검증은 아직 없습니다.**
Kubernetes 리소스 선언·결과 계약과 batch 입출력은 자동 테스트로 검증했으며, 실제 클러스터
제출·이미지 pull·모델별 학습은 대상 환경에서 추가로 검증해야 합니다.
튜닝 모델의 Ollama 등록·챗봇 승격·전체 재임베딩은 자동 수행하지 않습니다.

## 기존 SQLite에서 이전

`migrate_workbench_postgres.py`와 `migrate_executor_state.py`는 기본 실행에서 행 수/해시만
출력합니다. 기존 API/워커의 쓰기를 멈추고 활성 작업·예약이 없는 상태에서 `--apply`로
이전합니다. 빈 목적지에 트랜잭션으로 쓰고 모든 행을 비교한 뒤 import marker를 남깁니다.
재실행은 같은 원본이면 건너뛰며, 다른 원본을 덮어쓰지 않습니다. 원본 파일은 읽기 전용으로
열어 보관합니다. 워커 이전은 `--environment <등록 ID>`로 소유 환경을 지정합니다.
실행은 Airflow 관리 프로세스에서 하며 DB 계정·비밀번호 생성 절차는 없습니다.

## 권한과 검증

사람용 화면/API는 Airflow 인증을 적용합니다. API 빌더와 개인 대시보드 저장은 Plugins 권한,
소스 등록·SQL 직접 조회·환경 편집/배포·학습 실행은 Config 조회(기본 FAB Admin) 또는
Airflow Workbench custom PUT 권한입니다. 변경 요청의 Origin/요청 헤더를 검사하고
DAG 제어 API에 현재 사용자 자격 증명을 전달합니다. 대시보드 집계는 동일 Auth Manager의
실행 조회 권한·허용 DAG를 적용한 SQL과 권한별 캐시를 사용합니다. 실험의 prompt/문서/응답은 Plugins 조회
사용자에게 보이며, 개인 대시보드의 소유권과는 별도입니다.

워커용 상태 API는 Airflow 사람 계정 대신 환경별 Connection bearer를 검증합니다.
토큰은 화면·DAG conf·trigger serialization에 포함하지 않습니다. API 상태를 다른 환경에서
사용할 때 환경 ID뿐 아니라 해당 Connection의 토큰도 일치해야 합니다.

자동 검사는 PostgreSQL의 무작위 `wb_test_*` 스키마에서 실행 후 정리합니다.
`test_airflow_workbench`, `test_workbench_accounts`, `test_workbench_studio`,
`test_workbench_queries`, `test_workbench_portable`, `test_executor_state`는 scripts에,
`test_worker`는 model_worker에 있습니다. `test_workbench_batch`는 같은 model_worker 모듈을
PYTHONPATH에 포함합니다. 워커 런타임 자체에는 테스트를 위한 DB 접근이 필요하지 않습니다.
`check_airflow_dag_contracts.py`는 기존 파이프라인과 관리 DAG를 함께 검사합니다.
실행 없는 실제 UI/API 검사는 `smoke_workbench_studio.py`입니다.
`smoke_airflow_workbench_v2.py`는 실제 GPU 진단·취소 이력을 생성하는 개발 환경 검사입니다.

검증 기록: `docs/qa/airflow-postgres-mlops-2026-09-10.md`.

공식 인터페이스: [Airflow Plugins](https://airflow.apache.org/docs/apache-airflow/3.3.1/administration-and-deployment/plugins.html),
[Airflow public interface](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html),
[KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html).

