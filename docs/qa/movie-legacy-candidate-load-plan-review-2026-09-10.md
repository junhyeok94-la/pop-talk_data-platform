# Legacy candidate 적재 계획 선검토

최종 판정: APPROVED. 수정 계획에서 네 계약이 구체화됐다. Legacy candidate 격리 적재 범위의 구현을 진행할 수 있다. 아래 최초 지적은 이력으로 보존하며 최종 처리 결과는 마지막 절을 따른다. 업무 코드·데이터는 변경하지 않았다.

## 수용 사항

Legacy만 수동 candidate DAG에서 처리하고 Main Asset을 발행하지 않는 방식은 상위 승인 설계에 맞는다. Service snapshot/Gold/production STAGING/dbt/PG를 이번 적재 대상에서 제외한 것도 적절하다. Legacy 시간의 null/false/LEGACY_UNKNOWN, 원본 Bronze 한 객체와 Silver 5,985행의 구분, candidate boxoffice 테이블을 만들지 않는 선택을 수용한다.

## 1. [P2] Snowflake DDL과 DML 트랜잭션의 경계를 명시해야 함

계획은 rollback 테스트만 제시하고 실제 transaction 순서를 정하지 않는다. 기존 `snowflake_exchange_loader.load_exchange`는 DDL의 암시적 commit을 피하려고 테이블/임시 입력 준비를 BEGIN 전에 완료한다. candidate loader도 이 경계를 유지해야 한다.

필수 순서: candidate DDL/임시 입력 준비 → BEGIN → ledger identity 및 replay 검증 → 관측 conflict 검사/insert → 저장된 key/hash/count 대사 → SUCCESS ledger → COMMIT. 실패는 DML 전체 rollback하며 테이블 생성까지 rollback됐다고 주장하지 않는다. 성공 replay도 ledger 건수만 보지 말고 실제 관측 key/hash를 검증한다.

nullable/time-state는 적재 전 검증을 필수로 선택한다. `CHECK 또는 검증`처럼 구현 결정을 남기지 않는다. mock에서 BEGIN 이후 DDL이 없음을 확인하고 실제 candidate 실패 주입에서는 movie 삽입 후 예외가 나도 성공 ledger/부분 관측이 남지 않는지 확인한다.

## 2. [P2] artifact·revision·bundle의 불변 범위가 불명확함

§2의 ‘다른 artifact가 같은 publication revision을 차지하면 실패’는 source/manifest 범위가 없다. 관측 grain에는 artifact가 포함되므로 별도 artifact의 revision 1을 허용할지, manifest별 revision을 artifact 하나에 예약할지 결정해야 한다. 기존 방식에 맞출 경우 READY identity는 source × artifact × revision이고 서로 다른 artifact는 별도 namespace다. 다르게 설계할 경우 별도 reservation 및 충돌 검증이 필요하다.

다음을 계획에 명시한다.

- source_run_id 생성식과 manifest SHA/key의 대응.
- artifact digest에 실제 notebook, 순수 변환, v2 enrichment, 직렬화와 검증 코드를 어떤 형태로 포함하는지.
- bundle/JSONL의 정렬·UTF-8·개행·archive metadata 고정 규칙. ingested_at/processed_at 같은 실행시각은 결정적 JSONL/hash에서 제외하고 저장소 메타데이터로 둔다. 그렇지 않으면 로컬 계산/원격 재계산과 replay가 시각 차이로 충돌한다.
- DAG max_active_runs와 쓰기 태스크 직렬화, 원격 submit token의 입력과 재사용 규칙. 동일 run 재시도뿐 아니라 서로 다른 수동 run의 같은 입력 replay도 검증한다.

## 3. [P2] 읽기 금지 문구와 production 불변 검증이 충돌함

§1은 production 객체를 ‘읽거나 변경하지 않는다’고 하지만 §6은 실행 전후 production 건수·lineage 동일성을 요구한다. candidate 작업의 쓰기 대상과 검토용 read-only baseline 조회를 구분해야 한다.

실행 전후 검증에는 건수뿐 아니라 기존 API 관측 key/record hash, Gold 의미 데이터 digest, PG active publication ID/내용 digest를 사용한다. Databricks 운영 테이블과 기존 S3 Raw/Exchange도 해당 범위에 포함한다. 직접 조회가 제한된 대상은 사용 가능한 version/history 또는 실행 권한·쿼리 기록 등 대체 증거와 검증 한계를 명시한다. 건수만 같은 변경을 ‘불변’으로 승인하면 안 된다.

동시 운영 작업이 있다면 전후 차이를 candidate 탓으로 단정할 수 없으므로 실행 시 진행 중 작업 확인과 비교 기간을 기록한다. DAG 코드의 production 문자열 부재는 보조 검사일 뿐 실제 동적 경로 격리의 증거를 대신하지 않는다. 쓰기 대상 allowlist와 전송 전 검증을 adapter/notebook/loader에 둔다.

## 4. [P2] 0행 boxoffice와 READY 파일 경계를 명시적으로 닫아야 함

boxoffice=0은 ‘이 Legacy 원천에 박스오피스 관측이 없음’이다. 실제 관객/매출 0이나 기존 박스오피스 삭제를 의미하지 않는다. 문서에 이 의미를 명시한다.

READY에서 movie/boxoffice 두 파일의 정확한 집합, 동일 candidate publication prefix, bytes/hash/row_count를 검증하고, boxoffice는 bytes=0 및 빈 bytes의 SHA-256을 요구한다. row_count=0인데 공백/잘못된 행이 들어간 파일을 조용히 무시하지 않는다. 파일 key를 따라 GET하기 전에 candidate prefix와 source/artifact/revision 대응을 검증한다. 기존 v1 reader는 파일을 읽은 뒤 READY path를 검증하므로 이를 그대로 복제하지 않는다.

## 구현 가능한 범위와 재검토 조건

모듈 경계·candidate 저장 위치·Legacy 전용 실행 방향은 수용한다. 위 사항은 새 기능 확장이 아니라 승인된 격리·멱등성·byte 검증을 구현 가능한 규칙으로 고정하는 작업이다. 계획에 transaction 순서, 불변 identity, baseline 읽기 범위, 빈 파일 계약을 반영하면 구현 착수 판정을 다시 할 수 있다.

## 수정 계획 최종 확인

수정본 전체를 읽고 다음 반영을 확인했다.

- **트랜잭션:** DDL/임시 입력을 BEGIN 전에 준비하고 observation·대사·SUCCESS ledger를 한 DML transaction으로 처리한다. 실패 주입과 성공 replay의 실제 key/hash/count 검증을 명시했다.
- **불변 identity:** source_run_id 생성식, source×artifact×revision namespace, artifact 구성, canonical JSONL/ZIP, 처리시각 제외, 쓰기 직렬화와 cross-DagRun submit token 재사용을 명시했다.
- **격리 증거:** candidate 태스크의 운영 객체 접근 제한과 검증자의 read-only fingerprint를 분리했다. Delta/S3/Snowflake/Gold/PG의 내용·버전·계보 비교 및 제한된 조회의 증거 수준을 기록하도록 했다.
- **빈 boxoffice:** 관측 부재라는 의미, 실제 빈 bytes/건수/hash, 정확한 파일 집합과 GET 전 candidate path 검증을 명시했다.

이전 네 수정 요청은 모두 종료한다. 구현 검토에서는 artifact에 포함된 원격 재계산 코드가 실제 bundle 또는 고정 notebook 배포로 전달되는지 확인한다. 특히 v2 enrichment/직렬화의 실행 의존성이 bundle의 정확한 파일 목록과 일치해야 하며, digest에 이름만 들어 있고 실제로는 다른 코드를 실행하는 상태를 허용하지 않는다.

이번 승인은 계획에 정의된 Legacy candidate 구현에 한정한다. 테스트와 실제 격리 실행 결과, 전후 fingerprint는 구현 이후 확인한다. Service snapshot, candidate Gold, production STAGING/dbt/PG 변경은 승인 범위에 추가하지 않는다. 재검토에서는 문서만 확인했으며 새 외부 조회·적재 또는 업무 코드 변경은 수행하지 않았다.
