# B1-1 PostgreSQL control store 구현 검토

판정: **CHANGES_REQUESTED**.

구현 기록, SQL migration, migration script, store, DB tests, prepare-airflow.ps1 및 compose 변경을 확인했다. 영구 control runtime credential을 사용하는 새 Airflow 컨테이너에서 DB 테스트를 활성화해 전체 **126개가 통과**했다(0.867초). 추가 반례는 같은 runtime으로 외부 transaction 안에서 실행하고 rollback했다. control 업무 row는 남기지 않았으며 PostgreSQL sequence 값은 rollback되지 않아 fence 번호를 소비했다. Snowflake/업무 DAG/serving 데이터는 변경하지 않았다.

## 1. [P1] result commit이 event와 required recipients를 권위 계약에 연결하지 않음

근거: `postgres_control_store.py:417` commit_result, `:539` acknowledge_delivery, SQL asset_outbox 정의.

실제 DB 반례에서 정상 result/claim/receipt에 다음 event를 전달해 commit에 성공했다: event_id='not-a-content-id', aggregate_id='nonexistent-result', partition_key='gen-999999999999', event_body={'wrong': True}, recipient='invented-consumer'. 이후 해당 recipient의 ACK와 unrelated-build라는 work 등록도 성공했다.

동일 event ID replay 본문 비교는 있지만 최초 이벤트가 해당 result/model/generation을 가리키는지, event ID가 canonical body와 같은지, recipients가 승인 release registry에 속하는지 검증하지 않는다. consumer는 잘못된 outbox를 권위로 다시 확인하기 때문에 오류가 보존된다.

수정: commit에서 권위 result/plan/release로 event와 recipient 집합을 생성하거나 전달 객체를 완전 대사한다. ACK의 work_kind/work_identity도 consumer 계약 및 aggregate로부터 도출한다. aggregate_kind까지 포함해 대사한다. event→aggregate 및 consumer→release 관계를 DB 제약 또는 transaction validator로 강제한다. 위 잘못된 event/recipient/work는 result와 outbox 모두 rollback해야 한다.

## 2. [P1] inbox claim은 owner 이름만 검사해 이전 lease가 새 lease를 완료함

근거: `postgres_control_store.py:650` claim_work, `:676` complete_work, WorkClaim.

실제 DB에서 work를 same-worker로 claim하고 expiry를 과거로 만든 뒤 같은 이름으로 다시 claim했다. 이전 WorkClaim을 complete_work에 전달했는데 새 CLAIMED 행이 COMPLETED로 바뀌었다. WHERE는 owner와 현재 expiry만 보고 caller의 lease identity를 보지 않는다. build claim에 적용한 fencing이 inbox에는 빠졌다.

수정: inbox에도 매 claim마다 증가하는 token/attempt를 저장하고 WorkClaim에 포함한다. 완료·heartbeat·실패 전이는 현재 token과 미만료 lease를 모두 대사한다. 완료 응답 유실 replay는 같은 token/work의 이미 완료된 상태만 성공 처리한다. 현재 complete_work는 동일 완료 재호출도 실패하므로 이 경우도 추가 검증한다. publisher lease에도 이름 재사용 정책 또는 claim token을 적용해 전송 이력이 새 lease에 잘못 귀속되지 않게 한다.

## 3. [P2] migration ledger까지 runtime UPDATE 권한을 부여함

근거: `001_dw_control.sql:209` ALL TABLES grant, `scripts/migrate_dw_control.py`의 기존 version SHA 비교.

실제 runtime privilege 조회에서 `has_table_privilege(current_user,'dw_control.schema_migrations','UPDATE')=true`였다. 따라서 DAG runtime이 migration SHA/version 증거를 수정할 수 있어 migration owner와 runtime의 책임 분리가 불완전하다. public CREATE는 false였으며 기존 role 격리 테스트는 통과했다.

수정: schema_migrations는 owner만 INSERT/UPDATE하고 runtime은 필요하면 SELECT만 허용한다. immutable plan/cohort/result 본문 테이블 역시 UPDATE 필요성을 구분하고 상태 테이블과 권한을 나눈다. 기존 version을 이미 적용했으므로 SQL v1을 그대로 편집하지 말고 새 migration으로 권한을 보정한다. _ensure_roles는 기존 runtime에 SUPERUSER/CREATEDB/CREATEROLE 또는 타 role membership이 있어도 제거/거부하지 않으므로 재실행 시 기대 privilege를 검증하고 불일치하면 명시적으로 실패하도록 한다.

## 4. [P2] load_catalog가 DB 식별 열을 읽지 않아 body와 SQL identity 불일치를 검출하지 못함

근거: `postgres_control_store.py:173` load_catalog의 SELECT body 세 개, SQL plan/cohort/result body 정의.

catalog 복원은 body 내부 ID를 key로 재구성하므로 SQL PK/plan/model/build 열과 body의 identity가 다른 행을 발견하지 못한다. 단계 A validator는 본문 자체가 유효한지만 확인한다. SQL FK는 바깥 열끼리 확인하므로 두 검증이 다른 대상을 볼 수 있다. runtime에 immutable body UPDATE까지 허용된 현 상태에서 adapter가 보장한다고 설명한 read-back 경계가 닫혀 있지 않다.

수정: 모든 권위 식별 열과 body를 함께 읽어 plan/cohort/result 객체와 정확히 대사한다. 가능하면 CHECK/generated column 등으로 DB에서도 body/identity 불일치를 차단한다. 공개 load_catalog는 자체 일관된 transaction을 제공하거나 cursor/RR 전제를 강제한다. 현재 독립 호출은 여러 SELECT를 같은 snapshot으로 보장하지 않고 implicit transaction을 남길 수도 있다.

## 검증 범위와 후속 필수 사항

- composite slot/cohort/result/attempt FK 생성 순서와 정상 replay는 실제 DB 테스트에서 작동했다. 하지만 slot의 result FK는 result ID 단독이므로 SUCCEEDED→same-slot result, REUSED의 두 result ID 일치가 SQL 단독으로 보장되는 것은 아니다. state/identity 부정 fixture로 adapter와 제약을 함께 검증한다.
- 테스트 4개는 모두 단일 connection의 outer rollback 아래 실행된다. 실제 여러 connection의 concurrent claim, RR serialization/unique conflict 후 재조회, durable commit 뒤 연결 상실은 아직 검증하지 않는다. '실제 constraint/lock 사용'과 동시성 검증 완료를 구분한다. 마지막 테스트 이름의 conflicting inbox body 시나리오도 현재 본문에는 없다.
- record_receipt는 caller가 제공한 test ID 목록과 그 목록의 hash만 비교한다. registry의 expected test set 및 실제 invocation/status 증명은 B1-2로 남아 있다. 이 API 단독으로 소유 test 성공을 검증했다고 표현하지 말고, B1-2에서 권위 실행 receipt와 연결한다.
- register_cohort는 cohort만 등록하고 COHORT_READY outbox를 만들지 않는다. pending_work는 inbox만 조회한다. outbox 없는 READY aggregate, PLANNED slot 및 claim 복구 전체는 아직 구현되지 않았다. B1-3에서 단순 publisher 연결에 그치지 않고 cohort/outbox 원자 등록과 전체 recovery 경로를 완성해야 한다.
- compose는 startup에 migration을 추가했다. destructive SQL은 없지만 migration 실패가 Airflow init을 막는 의존성이 생겼다. version 불일치 시 schema를 수정하지 않고 중단하는 정책은 수용하며, migration role/DDL 동시 실행 및 재시작 멱등성은 별도 검증한다.
- prepare-airflow는 기존 비밀번호를 보존한다. password key만 있고 나머지 control 연결 key가 없는 부분 구성은 현재 보완하지 않으므로 설정 검증/누락 key 보완을 후속으로 다룬다. 비밀값은 검토 출력에 노출하지 않았다.

우선 네 지적을 수정한 뒤 재검토한다. 추가 DB 반례의 transaction은 rollback했으며 검토 보고서만 작성했다.
