# Dashboard Studio 검증 · 2026-09-10

## 결과

Airflow 플러그인 안에 ECharts·GridStack·CodeMirror를 번들한 대시보드 편집기를 배치했다.
Grafana 서비스나 Prometheus를 설치·기동하지 않았다. 검토 중 내려받은 Grafana 이미지도 정리했다.
Airflow 왼쪽 메뉴만으로 대시보드와 Model Lab에 진입하며 중복 Workbench 상단 메뉴는 제거했다.

## 검증

- `test_workbench_studio.py`: **12/12 통과**. API 필터·변수·복수 집계·시간 버킷·P95 계약,
  null/빈 결과, 컬럼 검증, SQLite 임시 SQL 미사용, 소스 재사용, 복수 쿼리 오류 분리,
  PostgreSQL 권한 경계, 조회 timeout 재시도 방지, 배치/버전 충돌/복원.
- `test_airflow_workbench.py`: **17/17 통과**. 기존 Model Lab의 인증·CSRF·실험·학습 계약 회귀.
- `smoke_workbench_studio.py`: 인증 없는 요청 401, 외부 Origin 403, 이전 SQLite SQL API 410,
  번들 파일 제공, 실제 PostgreSQL 메타 DB 기반 Airflow API 데이터, Model Lab 기록,
  Model Lab 3개 DAG 계약·deferred pool 설정 확인.
- 실제 기본 보드 **2개, 12개 패널의 조회 오류 0개**. 이전 보드 **3개 보존**.
- 브라우저: Airflow 왼쪽 라우팅, 차트 변경, A/B 쿼리 실행, 실패한 PostgreSQL 쿼리와 정상 API
  결과의 병행 표시, CodeMirror SQL 편집, 시리즈 색상/축 설정, 템플릿 저장, 패널 적용·저장.
- 브라우저 드래그: 전체 실행 패널 x=0→8. 크기 조절 w=4→3, h=4→6. 서버 저장 v3와
  새로고침한 DOM의 좌표가 일치했다. 저장 이력 v2를 불러와 v4로 복원했고, 최종 기본 막대
  그래프 및 success 색상을 v5로 저장했다. 저장 이력은 남아 있다.
- 새 브라우저 검사에서 console error/warn 없음. 작은 카드의 게이지 겹침은 수정 후 시각 확인.

[API 검증 원본](../../.local/qa/docs/airflow-studio-validation-2026-09-10.json)

## 화면

![Dashboard Studio](airflow-studio-dashboard-2026-09-10.png)

![패널 편집](airflow-studio-editor-2026-09-10.png)

![PostgreSQL SQL 편집과 연결 대기](airflow-studio-sql-2026-09-10.png)

![Model Lab · 중복 상단 메뉴 제거](airflow-model-lab-no-topbar-2026-09-10.png)

## 데이터베이스 구분과 남은 연결

Airflow 메타 DB는 PostgreSQL 17의 `airflow` DB로 확인했다. API 빌더는 Airflow의 인증과
DAG 권한을 거쳐 이 DB의 데이터를 읽고, Python 컬렉션에서 집계한다. API 결과를 임시 SQLite
SQL 엔진으로 복사하는 경로는 종료했다. 플러그인 보드·실험과 GPU worker 큐의 로컬 저장
파일은 별개이며 SQLite를 유지한다.

자유 PostgreSQL SQL을 위한 `workbench_ro_airflow` Connection은 **미생성**이다.
앞서 자동 승인 검토가 지속적인 DB 로그인 역할/SELECT 권한과 로컬 자격 증명 저장을
명시적 승인 없이 진행할 수 없다고 거절했다. 관리자 계정으로 대신 연결하지 않았다.
따라서 SQL 소스는 연결 대기이며 실연결 테스트 완료로 주장하지 않는다.

검토 가능한 SQL: `orchestration/airflow/workbench-monitoring.sql`.
변경 대상은 `airflow` DB의 모니터링 뷰 5개(`dag_runs`, `task_instances`, `dags`, `dag_tags`,
`pools`)와 이 뷰만 SELECT 가능한 `workbench_airflow_ro` 계정이다. 원본 metadata의
Connection/Variable/XCom/conf를 조회할 권한은 포함하지 않는다.
`prepare-workbench-airflow-monitor.cjs`는 기본 모드로 설명만 확인했으며 `--apply`는 실행하지 않았다.

## 범위

현재 원본 API는 기간 내 최대 5,000개, 결과는 2,000행·1MB로 제한하고 표본 여부를 표시한다.
패널당 쿼리 6개, 보드당 40개 패널, 저장 이력 20개. 고급 옵션은 JSON 표현 설정이다.
Grafana 전체의 알림·조직별 권한·스트리밍 기능을 복제한 것은 아니다.
실제 파인튜닝을 이번 변경에서 실행하지 않았으며, 기존 외부 워커·리소스 한도 구조는 유지했다.
