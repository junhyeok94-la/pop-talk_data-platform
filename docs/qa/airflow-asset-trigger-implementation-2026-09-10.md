# Airflow Asset 자동 연결 구현 기록

## 구현 결과

`pop_talk_movie_raw_daily.validate_daily`가 검증된 S3 READY를 게시한 뒤 Asset event를
발행하고, `pop_talk_movie_databricks_daily`가 그 Asset을 schedule로 소비하도록 구현했다.
여러 event가 한 DagRun에 묶이면 동일 READY는 중복 제거하고 서로 다른 READY는 dynamic task
mapping으로 빠짐없이 처리한다.

## 코드 변경

- `pipelines/orchestration/asset_contract.py`
  - event metadata 생성과 READY key/plan lineage 대사
  - 실제 Asset provenance와 event extra 대사
  - 여러 event의 논리 중복 제거, 충돌 거부, 결정적 정렬
  - 수동 run의 기존 Param 입력 분리
  - raw_run_id별 stage READY key/load artifact/revision/Exchange key barrier 대사
- `orchestration/airflow/dags/pop_talk_movie_raw_daily.py`
  - `validate_daily`에 Asset outlet 선언
  - READY 게시 성공 이후에만 event extra 기록
- `orchestration/airflow/dags/pop_talk_movie_databricks_daily.py`
  - 고정 Asset schedule 적용
  - `resolve_ready_inputs`, `prepare_transform_requests`, `confirm_loaded_batch` 추가
  - `resolve_ready_inputs`를 유일한 DAG root로 두어 입력 검증 전 외부 배포 차단
  - stage, Databricks, Exchange, Snowflake를 READY별 mapped task로 전환
  - 네 외부 mapped task에 `max_active_tis_per_dag=1` 적용
  - 완성된 `json` + `idempotency_token` dict를 `expand_kwargs`에 전달
  - 모든 mapped load의 ALL_SUCCESS 및 lineage 대사 뒤 Cosmos/PG를 각각 한 번 실행
- `pipelines/orchestration/movie_daily_runtime.py`
  - stage 결과에 원천 READY key 보존
  - Asset raw_run_id와 실제 DAILY_READY 본문 run_id 대사
- `scripts/check_airflow_dag_contracts.py`
  - 생산 outlet/소비 schedule URI, 19개 task graph, mapped 제한과 요청 생산자 검사
  - Cosmos graph identity와 직렬화 회귀 검사 유지
- `pipelines/orchestration/tests/test_asset_contract.py`
  - 정상/수동/오류/중복/복수 정렬/map 상한/load barrier 단위 테스트 추가

## 파싱 제약 대응

처음에는 DAG 폴더에 공통 `pop_talk_assets.py`를 두었으나 Airflow 3.3.1 DagBag의 파일별
격리 import에서는 형제 DAG helper가 자동으로 Python path에 올라오지 않아 import가 실패했다.
네트워크 없는 결정적 파싱을 유지하기 위해 생산/소비 DAG가 동일 URI의 Asset을 각각 선언하고,
구조 테스트가 두 URI의 동일성을 강제하도록 바꿨다. Airflow Asset identity는 URI이므로 두
Python 객체는 같은 Asset을 가리킨다.

## 검증 결과

Airflow 이미지 `pop-talk-airflow:3.3.1-local`에서 확인했다.

- DagBag import error: 0
- 메인 DAG 정적 task: 19개
- `stage_bundle`, `transform_bronze_silver`, `publish_exchange`, `load_snowflake`:
  모두 mapped, `max_active_tis_per_dag=1`
- DAG 구조 계약: 9개 DAG 통과
- pipeline 단위 테스트: 52개 통과
- Asset 계약 집중 테스트: 7개 통과
- Cosmos 7 model task + 1 project test 및 불변 graph identity 검사 유지
- 잘못된 Asset event에서는 `identify_gold_build`와 `deploy_notebook`도 실행되지 않음
- A/B READY key가 서로 바뀐 lineage를 barrier가 거부하는 회귀 테스트 통과

아직 Raw → Asset-triggered Main의 실제 외부 통합 실행은 하지 않았다. 코드 검토 승인 후 작은
Raw 실행으로 Databricks, S3 Exchange, Snowflake, dbt, PostgreSQL까지 검증한다.

## 구현 검토 요청사항

1. Airflow runtime의 `triggering_asset_events` 정규화가 3.3.1 provenance 계약과 맞는가?
2. mapped task의 READY identity 전달, 완성 요청 `expand_kwargs`, 직렬 동시성 제한이 안전한가?
3. 한 map index 실패/skip 시 barrier가 dbt와 PG를 차단하고 재시도 시 성공 load를 재사용하는가?
4. Asset → S3 READY → stage → Exchange → Snowflake lineage 대사가 충분한가?
