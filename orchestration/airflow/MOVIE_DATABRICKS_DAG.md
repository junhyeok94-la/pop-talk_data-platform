# Movie Databricks DAG

`pop_talk_movie_databricks_daily`는 검증 완료된 S3 일일 Raw 실행을
Databricks Bronze/Silver Delta 계층으로 처리하고, S3 Exchange와 Snowflake Gold를 거쳐
PostgreSQL serving publication까지 활성화한다.

DAG 파일은 흐름과 Connection 조립만 담당한다. 실행 로직과 XCom 결과 타입은
`orchestration/airflow/modules/pipelines/orchestration/movie_daily_runtime.py`에 있으며 Airflow 없이 단위 테스트할 수 있다.
DAG와 runtime의 docstring은 각 단계의 입력·출력·부작용·재시도 의미를 한글로 설명한다.

정규 실행은 `pop_talk_movie_raw_daily.validate_daily`가 발행하는 고정 Asset을 schedule로
사용한다. Asset event metadata의 `ready_manifest_key`와 실제 생산 DAG/run lineage를
검증한 뒤 처리한다. scheduler 지연으로 여러 READY가 한 DagRun에 묶이면 READY별 mapped
task로 모두 처리하며, 같은 READY의 중복 event는 제거한다. 네 외부 mapped 단계는 각각
동시에 1건만 실행하고, 전체 Snowflake load 대사 후 dbt와 PostgreSQL을 한 번 실행한다.

장애 복구나 명시적 재처리는 Airflow UI에서 수동 실행하고 `ready_manifest_key`에 운영
`DAILY_READY.json` S3 key를 전달한다. Asset 실행에서는 Param 기본 key로 fallback하지
않는다. DAG는 pause 상태로 생성된다. 노트북과 Landing 파일은 Airflow의
`pop_talk_databricks` connection에
저장한 PAT로 접근한다. 토큰은 코드, XCom, 로그에 출력하지 않는다.

기본 key는 2026-09-09 최초 실제 통합 검증 실행을 가리킨다. Landing ZIP과
배포 노트북은 변환 모듈+노트북 SHA-256 버전별 불변 경로를 사용한다. 같은
key·버전·`processing_attempt` 재실행은 같은 Jobs API idempotency token을 써서
원격 실행을 중복 생성하지 않는다. 실패한 처리를 새 원격 실행으로 재시도할
때만 `processing_attempt`를 올린다. 성공한 버전은 ledger와 Delta 건수를
재검증한 후 중복 적재 없이 종료한다.

2026-09-09 코드 품질 리팩터링부터 artifact digest에는 bridge, Exchange publisher,
Silver 변환, Databricks notebook뿐 아니라 실행 결과에 영향을 주는 orchestration runtime도
포함한다. 따라서 리팩터링 이전 artifact와 digest가 다르다. 기존 READY를 새 코드로 다시
처리할 때 기존 `publication_revision`을 재사용하지 말고 다음 revision과 새 DAG run을
사용한다. S3 key schema와 원천 run identity는 바뀌지 않는다.

코드 변경을 current로 승격할 때는 기존보다 큰 `publication_revision`을 지정한다.
이 값은 READY별 한 artifact에만 할당되므로 같은 번호를 다른 코드가 재사용하면
실패한다. 구버전의 늦은 재시도는 더 낮은 revision에 머물러 current를 되돌리지
못한다. 의도적인 롤백도 과거 코드를 새로 배포한다는 의미로 다음 revision을 쓴다.

배포 후 staging 전에 로컬 source가 바뀌면 artifact version 비교가 실패한다.
이 경우 진행 중인 실행을 억지로 이어가지 말고 새 DAG run을 시작한다.

2026-09-10 Asset 자동 연결 통합 검증에서는 full 일일 READY 2건이 각각 Asset-triggered
Main run으로 순차 성공했다. 마지막 PostgreSQL serving은 영화 118건, 박스오피스 80건이며
dbt test 43개가 통과했다. `max_movies>0`의 sample READY는 stage에서 거부되어 Gold와
serving을 변경하지 않았다. 검증 종료 후 Raw/Main은 모두 pause 상태다.
