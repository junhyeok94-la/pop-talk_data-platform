# 모델 단위 DAG 단계 B1-1 구현 기록 — PostgreSQL durable control store

## 결과

승인된 B1 설계 중 PostgreSQL 영속 저장소와 로컬 자격증명/migration 경계를 구현했다.
업무 dbt SQL, Snowflake candidate relation, Airflow model DAG factory는 아직 변경하지 않았다.

## 구현 파일

- `pipelines/orchestration/sql/001_dw_control.sql`
  - generation plan, model slot/cohort/attempt/receipt/result
  - Asset outbox, recipient별 delivery, durable inbox work
  - slot/cohort/result/attempt 복합 FK, state CHECK, recovery index
  - `dw_control_runtime`에는 SELECT/INSERT/UPDATE와 sequence 사용만 허용
- `pipelines/orchestration/sql/002_dw_control_authority_and_fencing.sql`
  - 이미 적용된 v1은 변경하지 않고 검토 보정을 별도 version으로 추가
  - 배포/model별 immutable `release_delivery_specs`
  - publisher/inbox claim token과 runtime 최소 권한 재부여
  - migration ledger와 plan/cohort/result/receipt에는 runtime UPDATE를 허용하지 않음
- `pipelines/orchestration/sql/003_dw_control_slot_result_integrity.sql`
  - SUCCEEDED slot이 같은 plan/model/build result만 가리키게 하는 복합 FK
  - REUSED slot의 `result_manifest_id = reuse_result_manifest_id` CHECK
  - slot runtime UPDATE를 state/claim/result 상태 열로 제한하고 reuse binding 변경 금지
- `pipelines/orchestration/sql/004_dw_control_reuse_and_delivery_registry.sql`
  - REBUILD일 때만 채워지는 generated result 열에 same-slot 복합 FK 적용
  - 정상 cross-generation REUSE는 과거 result FK를 그대로 사용
  - release registry 전체 본문/digest와 spec FK 추가
  - model runtime의 registry INSERT 회수, SELECT만 허용
- `scripts/migrate_dw_control.py`
  - `dw_control_owner` NOLOGIN과 `dw_control_runtime` LOGIN/NONINHERIT 분리
  - migration SHA-256과 version 기록, 같은 version의 변경된 파일 거부
  - runtime 비밀번호를 bind할 수 없는 role DDL에서는 psycopg `sql.Literal`을 사용하며 출력하지 않음
- `pipelines/orchestration/postgres_control_store.py`
  - plan/cohort 등록과 전체 단계 A provenance read-back 검증
  - `REPEATABLE READ` cohort snapshot
  - attempt마다 단조 fence token과 서로 다른 Snowflake candidate relation ID 발급
  - current lease/fence 기반 heartbeat와 receipt/result commit
  - 실제 `ModelDagRegistry` 전체에서 direct child와 terminal publication recipient를 계산하고
    deployment writer만 불변 등록
  - result + slot + registry에서 파생한 outbox + recipient delivery 원자 commit 및 응답 유실 replay
  - outbox publisher lease, 전송 이력, recipient ACK + inbox pending work 원자 인계
  - publisher/inbox claim token, 멱등 complete/전송 기록과 recovery scan
- `pipelines/orchestration/tests/test_postgres_control_store.py`
  - 실제 PostgreSQL 통합 테스트 9개, 순수 registry 변조 테스트 2개와 두 connection 동시 claim
- `scripts/prepare-airflow.ps1`
  - 기존 `.local/airflow.env` 비밀을 보존하며 control 전용 credential이 없을 때만 생성
- `compose.yaml`
  - Airflow init이 core metadata migration 전에 versioned `dw_control` migration 실행

## 구현된 장애 경계

1. worker A lease 만료 뒤 B가 claim하면 attempt와 fence가 증가하고 물리 relation ID가 달라진다.
   A는 heartbeat/receipt/result commit을 할 수 없다.
2. result commit 응답이 유실돼 같은 result/outbox/recipient 집합으로 재호출하면 중복 없이 성공을
   확정한다. 같은 ID의 다른 본문 또는 recipient 집합은 거부한다.
3. event는 required recipient별 delivery를 가진다. 한 recipient ACK만으로 outbox를 COMPLETE로
   만들지 않는다.
4. ACK와 durable pending work 등록은 같은 PostgreSQL transaction이다. claim 전 실패해도
   `pending_work()`가 다시 찾는다.
5. runtime role은 superuser/createdb/createrole/owner membership/schema CREATE 권한이 없다.
6. event ID/body/partition/aggregate/recipient/work는 권위 result와 release delivery registry에서
   파생된다. consumer는 실제 Airflow event body/partition만 제출하고 임의 work를 지정하지 못한다.
7. 같은 publisher/consumer 이름이 재사용돼도 이전 claim token으로 새 lease를 완료할 수 없다.
8. catalog 조회는 SQL 식별 열과 JSON body를 함께 대사하며 독립 호출도 REPEATABLE READ다.
9. g1 result를 무변경 g2 slot이 REUSE할 수 있으며, REBUILD slot만 same-slot result FK를
   적용받는다.
10. runtime은 release delivery registry를 최초 등록할 수 없다. deployment writer가 실제
    `ModelDagRegistry` 전체로 계산한 registry만 등록한다.
11. delivery child edge는 `ModelDagSpec`을 신뢰해 재계산하지 않고 권위
    `GraphSnapshot.dependencies`에서 계산한다. 각 spec의 parent vector도 graph와 정확히 같아야 한다.

## 실행 결과

초기 migration 과정에서 발견한 두 문제를 수정했다.

- PostgreSQL role password DDL은 bind parameter를 허용하지 않으므로 안전한 quoted literal 사용
- NOLOGIN owner에 대상 DB의 schema 생성용 CREATE 권한 명시

최종 검증:

```text
dw_control migration 적용 확인:
v1=30a7d3f57148bd7e9c649958491f64510d14463e87c20203af10190260c9a956
v2=1cfd5503fbfcee71f9c8d3b0672bad4735853a11639a234ef1e73c455acc2a0c
v3=694af2a30764e719b493437ad4f084b2c9131f483b04c563df8c1d76542330bb
v4=13663dbf74e56efbcdf0c0006c7409daf460231df280e7ea70368098ac143e2d

Ran 133 tests in 1.965s
OK
```

통합 테스트는 `.local/airflow.env`에 생성된 영구 `dw_control_runtime` credential을 사용한 새
Airflow 컨테이너에서 실행했다. 각 테스트는 실제 PostgreSQL constraint/lock을 사용하고 최종
rollback하여 업무 row를 남기지 않는다. migration schema와 version row는 의도대로 유지된다.
동시성/SQL-body 위조 테스트만 fixture를 잠시 commit하며, 별도 owner connection이 exact plan ID로
정리한다. PostgreSQL sequence 번호만 의도대로 소비될 수 있다.

## 다음 범위

B1-2에서 exact model/test invocation receipt와 dbt wrapper를 구현한다. B1-3에서 Airflow 3.3.1
partitioned Asset publisher/consumer와 model DAG factory를 synthetic diamond graph로 검증한다.
현재 단계만으로 Snowflake 물리 불변성, Asset scheduler dispatch, 업무 7-model 실행이 검증됐다고
간주하지 않는다.
