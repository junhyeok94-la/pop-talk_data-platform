# DAG 리팩터링 코드 재검토 (2026-09-09)

판정: 수정 요청. 큰 데이터 흐름의 추가 차단 결함은 발견하지 않았으나, 동작 보존 계약과 검사 게이트에 아래 두 가지 보완이 필요하다. 구현 코드는 수정하지 않았다.

## P2 — dict 반환 주석이 XCom 분할 저장을 활성화함

대표 위치: orchestration/airflow/dags/pop_talk_movie_databricks_daily.py:90-91.

`@task`에 dict 반환 타입만 추가하면 실제 Airflow 3.3.1은 multiple_outputs=True로 추론한다. 파서에서 deploy_notebook, stage_bundle, publish_exchange, load_snowflake, publish_postgres, 두 Raw DAG의 plan/validate, S3 inspect_prefixes, Snowflake prove_rollback 등 11개 태스크가 True임을 확인했다. 표준 §5의 단일 dict/multiple_outputs=False 계약 및 구현 기록과 다르다. return_value 자체는 유지되므로 기존 Jinja가 즉시 깨진다는 뜻은 아니다. 하지만 필드별 XCom도 추가 저장되어 key 집합과 metadata 쓰기량이 달라진다.

권고: 단일 dict 계약 태스크에 `@task(multiple_outputs=False)`를 명시하고 태스크별 baseline에 해당 값을 검사한다. 기존 출력 계약이 다른 태스크가 있다면 기존 기준을 개별적으로 명시한다.

## P2 — Jinja field 및 실행 설정 회귀를 구조 검사가 놓침

위치: scripts/check_airflow_dag_contracts.py:204-214.

현재 검사는 transform.json 전체에 task ID 문자열이 존재하는지만 확인하므로 notebook_path, landing_dir, artifact_version의 정확한 참조를 보장하지 않는다. 실제 DagBag의 메모리 복사에서 artifact_version을 WRONG_ARTIFACT로 바꾸고 retry_delay를 0으로 바꿔도 check_contracts()가 통과했다. 파일과 외부 상태는 변경하지 않았다. timezone 역시 UTC로 정규화된 start_date만으로는 업무 schedule timezone을 보장할 수 없다.

권고: 각 notebook parameter와 idempotency_token의 task/key 참조를 정확히 비교하거나 가짜 XCom/Params로 렌더링한 결과를 검증한다. DAG timezone, retry_delay, retry_exponential_backoff, max_retry_delay 및 multiple_outputs도 baseline에 포함한다. 잘못된 참조를 넣었을 때 검사가 실패하는 회귀 사례를 추가한다.

## 확인한 범위와 근거

- 6개 DAG와 추출 runtime, artifact 테스트 및 개발 표준을 대조했다.
- Airflow 실제 DagBag import 오류 0건, 현재 구조 검사 6개 통과.
- 단위 테스트 55개 통과: 수집 17, transforms 33, PostgreSQL 게시 3, orchestration 2.
- deploy→stage→transform→exchange→Snowflake→dbt→PostgreSQL 의존 관계 및 최종 upstream 성공 경계를 확인했다.
- artifact 의존성 확대, deploy/stage 및 stage/publish 버전 비교, 기존 archive 검증 인자, revision/attempt token 전달을 확인했다.
- runtime의 Databricks Session 종료와 DAG의 DB finally 종료, dbt shell에서 사용자 run_id 보간 제거를 확인했다.
- artifact digest 변경에 대해 새 DAG run/새 publication revision이 필요하다는 전환 기록은 타당하다.
- 외부 쓰기 통합 실행은 수행하지 않았다. 기존 단일 DAG 직렬 실행 승인 범위 및 서비스 앱 연동의 별도 범위는 유지한다.

위 두 항목 수정 후 재검토하고, 새로운 revision의 최소 표본 통합 실행 증빙으로 리팩터링 이후 실제 연동을 확인하는 것이 적절하다.
