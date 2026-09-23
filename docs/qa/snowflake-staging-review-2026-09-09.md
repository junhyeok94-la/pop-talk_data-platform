# Exchange → Snowflake STAGING 검토

판정: 승인 보류. P1 1건, P2 1건 수정 필요.

검토자는 전체 단위 테스트 33개를 재실행해 통과를 확인했다. 현재 신규 테스트는 read_exchange 입력 검증 2개뿐이며 load_exchange의 transaction/rollback/replay SQL 동작은 검증하지 않는다. 원격 데이터 변경 없이 코드 분석과 가짜 연결 재현을 수행했다.

## P1 — 임시 테이블 DDL이 명시적 트랜잭션을 끊음

위치: `pipelines/transforms/snowflake_exchange_loader.py:120,135-144`.

BEGIN → ledger RUNNING MERGE → CREATE OR REPLACE TEMP TABLE 순서다. Snowflake에서는 임시 테이블 CREATE도 DDL이며 기존 트랜잭션을 암묵적으로 커밋한다. 따라서 ledger와 두 관측 MERGE 및 SUCCESS가 하나의 명시적 트랜잭션이라는 구현 설명은 성립하지 않는다. 기본 AUTOCOMMIT=true에서는 이후 각 DML도 별도 커밋되므로 movie 적재 뒤 boxoffice 실패 시 rollback으로 전체 원복할 수 없다. AUTOCOMMIT=false여도 이미 커밋된 RUNNING ledger는 롤백되지 않는다.

근거: https://docs.snowflake.com/en/sql-reference/transactions 및 https://docs.snowflake.com/en/sql-reference/sql/create-table — TEMPORARY TABLE 생성도 DDL implicit commit 대상.

권고: 영구/임시 테이블 DDL 및 임시 입력 준비를 BEGIN 전에 모두 완료한다. 이후 명시적 BEGIN 안에는 대상 conflict 검증·ledger DML·두 관측 MERGE·persisted 대사·SUCCESS DML만 배치하고 commit한다. 임시 입력 적재에서 connector가 내부 stage DDL을 실행할 수 있는 경로도 트랜잭션 밖으로 둔다.

검증: movie MERGE 뒤 boxoffice 단계에 실패를 주입하고 다른 연결에서 대상 테이블과 ledger가 이전 상태임을 확인한다. rollback 호출이 실행됐다는 사실과 실제 변경이 원복됐다는 사실을 구분한다. QA의 과거 SQL 오류 rollback 설명도 실제 확인 범위로 정정해야 한다.

## P2 — 성공 replay가 실제 저장 데이터 무결성을 확인하지 않음

위치: `snowflake_exchange_loader.py:130-134`.

SUCCESS ledger의 identity와 MOVIE_COUNT/BOXOFFICE_COUNT만 비교하고 바로 반환한다. 관측 테이블 누락 또는 같은 건수의 다른 hash가 있어도 성공한다. 가짜 연결에서 ledger SUCCESS/1/0을 반환하자 loader가 (1,0)을 반환했으며 관측 테이블 SELECT는 한 번도 실행하지 않았다. 현재 문서의 건수 재검증은 실제 저장 건수 대사가 아니다.

권고: 성공 replay도 저장된 업무 키 집합·건수·payload hash를 incoming과 대사한다. 일반 최초 실행은 기존 matched row의 hash와 count를 검사하지만 replay는 그 검사를 우회한다. 누락/변조 시 fail closed 또는 명시적 복구 경로로 처리하고 조용히 성공하지 않는다.

## 후속 계약 및 검증 범위

- revision은 observation MERGE 키에 없으므로 같은 raw/artifact를 높은 revision으로 재게시하면 observation의 PUBLICATION_REVISION과 EXCHANGE_READY_KEY는 최초 값으로 남는다. 이를 불변 관측+별도 publication ledger 설계로 유지하려면 dbt는 SOURCE_RUN_ID+ARTIFACT_VERSION으로 SUCCESS ledger의 publication을 결합해 우선순위를 계산해야 한다. 관측 테이블에 남은 최초 revision/READY로 최신을 고르면 안 된다. 다음 단계 전에 명시한다.
- 값은 바인딩 파라미터로 전달하고 동적 table/column은 코드 내 고정 목록이라 SQL 값 삽입 경로는 적절하다. 문자열 → PARSE_JSON 변환도 적절하다.
- 빈 입력은 executemany를 생략하고 0건 count로 처리하는 코드 경로가 있다. 실제 transaction/빈 입력/실패 테스트는 추가 필요.
- 승인된 단일 직렬 DAG 범위로 검토했다. 복수 독립 실행에서 조회 후 MERGE 경쟁을 막는 원자적 claim은 없으며, 병행 확대 전에 별도 설계가 필요하다.
- read_exchange의 observed_at은 비어 있지 않은지만 확인하고 날짜 해석은 Snowflake cast에 맡긴다. 바른 트랜잭션 경계가 확보되면 실패 자체는 막을 수 있으나 엄격한 시각/키 형식 검증은 SQL 이전에 수행하는 편이 좋다.

dbt Gold와 서비스 DB 반영은 이번 승인 범위 밖이다. 위 2개 수정 및 적재 동작 테스트 후 재검토한다.
