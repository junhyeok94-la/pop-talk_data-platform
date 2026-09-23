# Asset dynamic mapping 설계 재검토

판정: CHANGES REQUESTED — 이전 이벤트 누락 P1과 exactly-once 서술 P2는 설계 수준에서 종결한다. dynamic mapping 자체는 구현 가능하다. 다만 새로 도입되는 mapped 외부 작업의 동시성 제한을 확정해야 한다. P0 없음.

## 이전 지적 종결

모든 triggering event를 검증한 뒤 논리 입력 중복 제거/충돌 거부, 서로 다른 READY 전체 처리, 초과 목록 수동 복구와 대사, queued event와 성공 acknowledge의 구분이 추가됐다. 기존 ‘복수 이벤트면 항상 실패’ 경로는 제거됐다. 신규 backlog 경계 및 성공 후 clear 중복, revision/attempt 수동 승격도 명시돼 이전 두 지적을 해결한다.

## P2 — mapped 외부 작업의 동시성 예산을 명시해야 함

근거: 설계 §5의 stage/Databricks/Exchange/Snowflake expand 및 max_active_runs=1 유지 부분.

현재 승인된 파이프라인은 한 DAG 안에서 Databricks 작업 하나, Snowflake load 하나가 실행되는 구조였다. mapping 이후 max_active_runs=1만으로는 같은 run의 여러 Databricks 작업/적재가 병렬 실행되는 것을 막지 못한다. 기존 Cosmos 전용 1-slot pool도 이 앞단에는 적용되지 않는다. Delta 공용 테이블 및 원격 작업 비용/실패 재시도 계약에 영향을 주므로 단일 run 제한을 외부 작업 직렬화로 해석하면 안 된다.

수정안: 이번 단계에서는 mapped Databricks operator와 Snowflake load 각각 max_active_tis_per_dag=1 또는 명시적 1-slot pool을 사용하여 기존 동일 단계 직렬 실행 경계를 보존한다. stage/Exchange도 동시성 예산을 명시한다. 향후 병렬화는 별도 성능·경합 검증으로 승인한다. remote 실패 뒤 같은 idempotency token의 단순 재시도가 새 Databricks 작업을 만들지 않는 기존 계약도 유지한다. 제한을 구조 검사에 포함하면 이 항목은 종결 가능하다.

## Databricks mapping 구현 가능성

설치된 Airflow 3.3.1 및 Databricks provider에서 메모리상의 DAG에 DatabricksSubmitRunOperator.partial(...).expand_kwargs([{json: A, idempotency_token: A}, {json: B, idempotency_token: B}])를 생성해 MappedOperator로 수용되는 것을 독립 확인했다. DAG 등록, 스케줄링, 외부 실행 및 코드 수정은 하지 않았다.

권장 구현 계약:

1. 각 staged 입력에서 완성된 json과 idempotency_token을 한 dict로 조립한다. 노트북 경로·READY key·landing·artifact·revision·attempt를 같은 입력에서 가져온다. mapped json 내부에 미렌더링 Jinja를 넣지 않는다.
2. partial에는 task_id, Connection ID, pool/concurrency/timeouts 등 공통 설정을 두고 expand_kwargs에는 한 입력에 대응하는 옵션 묶음을 넘긴다. json과 token을 별도 expand 축으로 주면 Cartesian product가 되므로 금지한다.
3. map index는 관측용 보조값이다. 동일 index라는 사실만으로 입력 일치를 보장하지 말고 ready key/raw_run_id/artifact/archive digest를 끝까지 전달·대사한다. 중간 단계에서 None 반환이나 필터링으로 목록을 축소하거나 재정렬하지 않는다.
4. provider 결과는 staged metadata 전체를 반환하지 않으므로 publish_exchange가 올바른 staged 입력과 원격 성공 의존성을 함께 갖도록 설계한다. 명시적 map_indexes 조회를 쓴다면 2개 입력의 완료 순서를 뒤집어도 자기 입력만 읽는지 검증한다. 기본 xcom_pull이 집계 목록을 반환하는 경우를 단일 dict로 간주하지 않는다.
5. mapped load 전체 성공 이후 unmapped ALL_SUCCESS barrier에서 기대 READY 집합과 load 결과 집합을 대사한 뒤 Cosmos를 한 번 실행하는 방식이 명료하다. 실패/skip/누락 결과를 성공으로 간주하지 않는다. 완료 결과는 개수뿐 아니라 identity 집합으로 확인한다.

[Airflow 3.3.1 dynamic mapping 공식 문서](https://airflow.apache.org/docs/apache-airflow/3.3.1/authoring-and-scheduling/dynamic-task-mapping.html)는 classic operator mapping, expand_kwargs, repeated mapping 및 여러 expand 인자의 Cartesian product를 설명한다. mapped template field 값은 자동 Jinja 렌더링을 기대하지 않아야 한다.

## 구현 검증 기준

- A/B 이벤트의 정렬·dedup 결과가 두 작업만 생성하고 토큰/노트북 인자가 서로 섞이지 않을 것.
- 실제 provider 생성 인자를 검사하고, 완료 순서 역전·한 index 실패·skip에서 잘못된 Exchange 게시 또는 dbt 진입이 없을 것.
- 모든 load 성공 시 Cosmos/PG는 한 번만 실행될 것. 일부 load가 이미 commit된 뒤 다른 입력이 실패하면 PG active는 유지하고 재실행 시 성공 load를 재사용할 것.
- resolve 이후 identify의 literal op_kwargs/graph identity 보존 및 모든 외부 쓰기 선행 조건을 유지할 것.
- manual 입력도 길이 1 목록으로 동일 경로를 사용하고, 복수 입력 초과 복구는 현재 manual 단일-key 인터페이스에 맞춰 key별 run으로 수행할 것.

동시성 정책을 위처럼 문서에 확정한 뒤 설계 승인이 가능하다. mapping 및 Asset 연결 구현/외부 실행은 이번 검토에서 하지 않았다.
