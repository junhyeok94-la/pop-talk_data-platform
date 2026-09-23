# DAG 코드 품질 리팩터링 구현 기록

## 범위

승인된 `orchestration/airflow/DAG_DEVELOPMENT_STANDARD.md`를 기존 6개 DAG에 적용했다.
이번 변경은 데이터 계약, S3 key 형식, DAG/task ID, schedule, Params 기본값, task graph,
재시도와 timeout을 보존하는 리팩터링이다.

## 주요 변경

- 모든 DAG의 목적·전체 흐름·완료 조건을 한글 module docstring으로 설명했다.
- 비자명한 태스크에는 입력, 출력, 부작용과 재시도 의미를 한글 docstring/주석으로 기록했다.
- 함수·변수·task/Connection ID는 실행 호환성을 위해 영어를 유지했다.
- 전체 파이프라인의 실제 실행 로직을 Airflow 비의존
  `pipelines/orchestration/movie_daily_runtime.py`로 분리했다.
- 태스크 사이 dict 계약을 `TypedDict`로 정의했으며 XCom 반환 형태는 기존 단일 dict를
  유지했다. Airflow 3.3.1의 반환 타입 추론을 막기 위해 dict 태스크는
  `@task(multiple_outputs=False)`를 명시했다.
- Databricks/Snowflake/PostgreSQL 연결 종료 경계를 명시했다.
- dbt Bash 명령에서 사용자 정의 run ID 보간을 제거했다. 기존 `max_active_runs=1` 계약을
  이용해 고정 임시 target 경로를 사용한다.
- `scripts/check_airflow_dag_contracts.py`로 리팩터링 전 구조 baseline을 자동 검사한다.

## Artifact 전환

새 `transform_artifact`는 아래 전이 의존성을 모두 path+content digest에 포함한다.

- Databricks bridge
- S3 Exchange publisher
- Silver 변환 core
- Databricks notebook
- movie daily orchestration runtime

따라서 이전 코드와 artifact digest가 달라진다. 기존 READY를 새 코드로 처리할 때는 기존
publication revision을 재사용하지 않고 새 DAG run과 다음 revision을 사용해야 한다.
진행 중 코드 변경은 deploy/stage 또는 stage/publish artifact 비교에서 안전하게 실패한다.

## 검증 결과

- Airflow 3.3.1 `dags list-import-errors --output json`: `[]`
- 수집 단위 테스트: 17개 통과
- Bronze/Silver/Exchange/Snowflake loader 단위 테스트: 33개 통과
- PostgreSQL reverse ETL 단위 테스트: 3개 통과
- 신규 orchestration artifact 단위 테스트: 2개 통과
- DAG 구조 계약: 6개 DAG 통과

구조 계약은 DAG/task ID, edge, Params name/type/default, schedule/catchup/start date/timezone/
pause, max active runs/tasks, trigger rule, retries/delay/backoff/max delay, timeout, pool,
multiple_outputs, Databricks의 정확한 Jinja/XCom field와 dbt shell 명령을 확인한다. 검사 자체의
회귀 방지를 위해 `artifact_version` 참조와 `retry_delay`를 메모리에서 잘못 바꿨을 때 반드시
실패하는 부정 사례도 실행한다.

이번 검증에서는 외부 데이터를 변경하는 전체 파이프라인을 다시 실행하지 않았다. 데이터
처리 모듈의 기존 테스트와 실제 Airflow parser/구조 baseline으로 동작 보존을 확인했고,
코드 검토 승인 후 다음 revision의 최소 표본 통합 실행 여부를 결정한다.
