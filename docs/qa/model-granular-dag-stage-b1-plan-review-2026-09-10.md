# 단계 B1 durable control store / factory 계획 검토

판정: **CHANGES_REQUESTED**. 별도 control schema, relation 선봉인 후 result/outbox 원자 commit, partitioned cohort 소비 방향은 수용한다. 다음 네 계약을 구체화해야 실제 장애 복구를 보장할 수 있다. 계획과 업무 코드는 수정하지 않았고 외부 쓰기도 수행하지 않았다.

## 확인 범위

계획 전체, 단계 A 승인서, compose.yaml, create-airflow-database.sql, 실제 단계 A ManifestCatalog/provenance 및 registry 코드를 확인했다. compose는 서비스 PostgreSQL 인스턴스를 공유하지만 초기화 SQL은 별도 `airflow` database/owner를 만든다. 따라서 서비스 DB의 `dw_control`은 metadata DB와 논리적으로 분리할 수 있다. 동일 인스턴스의 가용성과 자원은 공유한다.

설치된 Airflow 컨테이너의 소스를 읽어 `PartitionedAssetTimetable(..., default_partition_mapper=IdentityMapper())`와 `OutletEventAccessor.add_partitions(self, keys: str | list[str])`를 확인했다. add_partitions는 concrete Asset에서 사용할 수 있고 alias에서는 거부된다. partition producer/consumer 설정과 실제 scheduler 동작은 synthetic integration에서 검증해야 한다. [Airflow Asset 문서](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html).

## 1. [P1] lease와 metadata stale commit 거부만으로 물리 결과를 보호하지 못함

근거: 계획 52·62행 claim_owner/expiry, 90–96행 relation 선봉인, 213행 stale worker 시나리오.

실패 시나리오: worker A가 Snowflake 쿼리를 실행하다 lease를 잃고 B가 같은 generation relation을 생성·봉인한 뒤 result를 commit한다. 뒤늦게 A의 원격 쿼리가 끝나 같은 relation을 변경하면 A의 PostgreSQL commit을 거부해도 이미 READY가 가리키는 데이터는 바뀐다. lease 원자 claim은 동시 소유권만 제한하며 기존 worker의 실행 자체를 중단하지 않는다.

수정: claim 때 단조 증가 fencing token/attempt를 발급하고 heartbeat·receipt·result commit 모두 해당 token의 현재 소유권과 expiry/state를 조건으로 검사한다. 물리 relation은 build+attempt별 격리 이름으로 만들고 봉인 후 불변 객체를 result가 가리키게 하거나, 모든 실제 쓰기가 fence를 강제하는 방식을 정한다. 단순 SQL 전 한 번 검사로 대체하지 않는다. generation용 공통 relation을 두 attempt가 덮어쓰지 않게 한다. response-loss retry는 기존 attempt/result를 조회하고, GC는 참조 중 relation 및 진행 중 attempt를 삭제하지 않는다.

필수 검증: A 만료→B 봉인/commit→A 원격 완료에도 B relation digest 불변, A receipt/commit 거부. worker 식별자 재사용 시에도 옛 token은 거부한다.

## 2. [P1] 하나의 ACK와 inbox upsert만으로 fan-out 및 ACK 이후 실패를 복구할 수 없음

근거: 계획 129–149행 단일 outbox ACKED, consumer_model별 inbox; 159–160행 resolver 뒤 claim; 144–146행 미ACK/outbox 부재 복구.

실패 시나리오 A: MODEL_READY를 dim/quality 등 여러 coordinator가 소비할 때 한 consumer가 ACK하면 outbox 전체가 ACKED가 된다. 다른 consumer가 알림을 잃었어도 재전송이 끝난다. 마지막 모델처럼 직접 consumer가 없는 이벤트는 반대로 영구 미ACK가 될 수 있다.

실패 시나리오 B: resolver가 inbox와 ACK를 저장한 뒤 claim 전에 실패하고 Airflow 재시도를 소진한다. outbox는 ACKED이며 존재하므로 계획의 두 reconciliation 대상에서 빠진다. inbox에 기록이 있다는 사실만으로 모델 실행이나 자식 cohort 등록이 지속된다고 볼 수 없다.

수정: event별 required recipient 집합을 release registry에 고정하고 `(event_id, consumer_id)` delivery 상태 또는 중앙 coordinator 하나로의 인계 모델 중 하나를 선택한다. 소비자 없는 이벤트의 완료 정책도 둔다. ACK는 단순 읽음이 아니라 해당 consumer의 durable pending work 등록과 같은 transaction으로 처리한다. recovery는 ACK 여부와 별개로 미완료 inbox/cohort, PLANNED slot, 만료 claim을 재구동한다. ACK 소유자·event ID·asset URI·partition·aggregate body의 일치를 검사한다. inbox dedup 시 본문 충돌을 no-op 처리하지 않는다.

필수 검증: 두 consumer 중 하나만 ACK, ACK 직후 claim 전 crash, lease 만료 후 retry 소진, no-consumer terminal event. Airflow 재전송 횟수 대신 실제 required work 완료를 대사한다.

## 3. [P2] search_path는 schema 밖 쓰기를 막는 권한 경계가 아님

근거: 계획 20–24행; compose.yaml의 db POSTGRES_USER와 서비스 접속 재사용, create-airflow-database.sql의 별도 metadata owner 구성.

서비스 DB의 별도 schema 선택은 적절하다. 그러나 `search_path=dw_control`은 이름 해석 범위를 정할 뿐, 권한이 있는 사용자의 `UPDATE dw_serving...` 같은 schema-qualified SQL을 금지하지 않는다. 현재 로컬 사용자를 계속 쓰면서 schema 외 쓰기를 권한 테스트로 금지한다는 약속은 실제 role 제한 없이는 성립하지 않는다. [PostgreSQL schema/search_path](https://www.postgresql.org/docs/17/ddl-schemas.html).

수정: B1부터 migration owner와 runtime role을 분리하고 runtime은 non-superuser/non-owner로 필요한 dw_control 권한만 받도록 한다. 같은 host/database를 유지하되 control 전용 credential 또는 안전하게 제한된 연결 경계를 명시한다. 서비스/Airflow metadata object 권한 및 role membership에 의한 우회를 확인한다. 권한 검증은 무해한 격리 sentinel 또는 privilege 조회로 수행하며 실제 서비스 데이터를 변경하지 않는다. schema 분리는 연결 DB 확인과 함께 검증한다.

## 4. [P2] receipt를 실행 attempt와 검증된 출력에 연결하는 필드가 부족함

근거: 계획 101–114행 receipt, 173–176행 exact test argv, 195–198행 run_results 대사.

정확한 test ID 집합 대사는 필요하지만 충분하지 않다. 다른 generation/attempt의 과거 run_results에 같은 test ID가 있고 현재 실행 결과 파일이 생성되지 않으면 잘못된 성공 receipt로 사용할 수 있다. 현재 receipt에는 cohort/deployment/parent vector, claim token, dbt invocation 및 run_results artifact identity가 명시돼 있지 않다. 모델 실행도 exact selector 주장과 실제 실행 unique ID를 연결해야 한다.

수정: attempt별 격리 target/log 경로를 사용하고 실행 전후 artifact identity를 구분한다. receipt에 build/cohort/deployment/parent vector/fence token, model 및 test invocation identity, run_results digest, relation 검증 query identity를 연결한다. subprocess 성공과 모든 예상 test의 허용 status를 검사하고 error/skip/누락을 거부한다. 소유 test가 원래 0개인 경우와 예상 test가 있는데 결과가 0개인 경우를 구분한다. 결과 등록 시 이 receipt가 현재 claim과 같은 봉인 relation을 증명하는지 대사한다.

현재 wrapper `scripts/dbt_cosmos_runner.py`는 기존 전체 test 태스크/모델 run 규칙을 사용한다. 계획대로 확장하되 신규 owned-test 경로가 기존 selector 보호를 느슨하게 하지 않도록 테스트한다. Cosmos TestBehavior.NONE + 명시 test 실행 자체는 실행 가능한 방향이다.

## FK·transaction 구체화 조건

계획의 개별 plan/cohort FK만으로 plan/model/build가 서로 같은 slot임을 보장하지는 않는다. migration에는 cohort→slot `(plan_id, model_unique_id, build_id)` 및 result→해당 cohort/slot의 composite identity 제약, receipt→build/attempt, outbox→권위 aggregate 연결을 명시한다. REBUILD/REUSE mode와 result NULL/state 조합도 CHECK/transaction validator로 강제한다. 순환 result FK는 초기 NULL slot→cohort→result→slot 완료 순서로 처리할 수 있다.

cohort catalog를 '같은 transaction snapshot'으로 읽는다는 문구는 실제 isolation 또는 일관된 조회 방식으로 구현한다. 전체 catalog 검증과 register는 단계 A의 논리 provenance를 유지하고, 원자 slot claim과 unique constraint가 concurrent conflict를 처리해야 한다. 같은 content ID 재시도는 본문까지 비교한다.

## B1 synthetic integration 범위

업무 SQL을 B2로 미루는 것은 수용한다. 단순 모델 2개의 직렬 성공만으로는 충분하지 않다. B1에서는 최소 fan-out→다중 부모 join을 가진 작은 graph(예: diamond)와 source/deployment/owned test gate를 포함한다. 실제 PostgreSQL concurrent transaction과 실제 Airflow partition dispatch, ACK 전후 crash를 검증하고 위 stale attempt·fan-out ACK·과거 receipt 반례를 포함한다. 모델 수보다 실패 경계를 검증하는 것이 중요하다.

Snowflake 물리 불변성은 실제 격리 candidate relation로 확인하거나, B1에서 모형만 사용했다면 실제 증명은 B2 이전 별도 통합 gate로 남긴다. current 7-model factory render만으로 generation-aware SQL, 실제 43 test 실행, production 전환을 검증했다고 표현하지 않는다.

위 네 수정과 FK/integration 조건을 계획에 반영한 뒤 재검토한다. 이번 검토에서 생성한 것은 이 보고서뿐이다.
