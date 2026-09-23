# B1-1 수정 구현 재검토

판정: **CHANGES_REQUESTED**. 이전 token/권한/body 대사 보완은 수용하지만 정상 REUSE를 차단하는 FK 회귀와 registry 최초 등록 권위 문제가 남아 있다.

## 독립 검증

최신 store, v2/v3 migration, migration runner와 테스트를 읽었다. 영구 runtime credential의 새 Airflow 컨테이너에서 DB 테스트를 활성화해 **129개 모두 통과**했다(1.565초). 전체 suite에는 두 connection 동시 claim, 이전 publisher/inbox token 거부, privilege 및 SQL/body 불일치 검사가 포함된다.

별도 정상 REUSE 등록 반례는 실제 control DB의 outer rollback transaction에서 실행했다. fixture row는 rollback했고 sequence 번호는 소비됐다. 전체 suite의 일시 commit fixture는 테스트의 exact-ID cleanup 경로를 사용했다. 코드나 migration은 수정하지 않았다.

## 1. [P1] v3 same-slot FK가 정상 cross-generation REUSE를 금지함

근거: `pipelines/orchestration/sql/003*`의 `model_build_slots_result_identity_fk`; `postgres_control_store.py:334` register_plan.

v3는 slot의 `(result_manifest_id, plan_id, model_unique_id, build_id)`가 result의 같은 네 필드와 일치하도록 모든 slot에 FK를 추가한다. 하지만 register_plan은 REUSED slot의 result_manifest_id에 과거 generation result ID를 저장한다. 그 result의 plan/build는 당연히 새 plan/build와 다르므로 정상 재사용도 FK에 실패한다.

실제 재현: synthetic g1을 claim→receipt→result commit까지 완료하고, 동일 graph/source의 다음 generation plan을 순수 API로 생성했다. 순수 slot.mode는 REUSE였으나 DB register_plan은 **ForeignKeyViolation / model_build_slots_result_identity_fk**로 실패했다. 따라서 반복 무변경 배치와 승인된 g2 dim+g1 fact 흐름을 durable store에 등록할 수 없다.

수정: same-slot 제약은 REBUILD 성공 결과에만 적용하고 REUSE는 별도 과거 result 참조를 유지한다. 예를 들어 rebuilt_result FK와 reuse_result FK를 분리하고 mode/state CHECK로 조합을 강제하거나 조건부 참조를 위한 generated column/검증 trigger를 사용한다. PostgreSQL FK가 mode 조건을 자동으로 해석한다고 가정하지 않는다. 적용된 v3는 보존하고 새 migration으로 보정한다.

필수 회귀: 실제 DB g1→g2→g3 무변경 REUSE 등록, g2 변경 branch와 g1 reused fact 결합, 반대로 REBUILD slot이 다른 slot result를 가리킬 때 거부.

## 2. [P1] delivery spec은 아직 runtime caller가 임의로 최초 등록할 수 있음

근거: `postgres_control_store.py:269` register_delivery_spec; v2 `release_delivery_specs`에 대한 runtime SELECT/INSERT grant.

commit_result가 event/recipient를 DB spec에서 파생하도록 바뀐 것은 맞다. 그러나 spec 등록 API는 형식과 동일 key replay만 검사한다. 실제 승인 deployment manifest/registry에 model과 required recipient가 속하는지 확인하지 않는다. runtime 계정에도 INSERT가 있어 새 release/model key에 빈 recipient 또는 임의 recipient를 먼저 등록할 수 있다. 이후 commit은 이 값을 권위로 사용한다. 빈 recipient이면 필요한 소비 작업이 있어도 terminal event로 COMPLETE 처리될 수 있다.

즉 임의 recipient 입력 지점이 commit_result에서 register_delivery_spec으로 이동했다. 이미 등록한 spec의 변경 거부만으로 최초 등록의 권위를 보장할 수 없다.

수정: spec 최초 등록은 승인 release descriptor/registry 전체를 검증하는 별도 deployment writer 경계로 제한하고 model worker runtime은 SELECT만 허용한다. 또는 명시적으로 신뢰할 release catalog를 조회해 exact model/Asset/recipient/work 집합을 전수 대사한 등록 API를 제공한다. synthetic fixture도 승인된 synthetic registry를 통해 등록한다. nonexistent model, 누락/추가 recipient, 임의 work kind와 runtime의 직접 INSERT를 거부하는 검증을 추가한다.

## 수용 사항과 후속 검증

- caller 임의 event 인자가 제거되고 result 기반 READY가 생성된다. ACK도 event와 partition을 대사하고 work를 spec에서 파생한다. registry 권위만 위와 같이 닫아야 한다.
- inbox/publisher token과 same-token 재시도 보완, migration ledger 및 immutable body UPDATE 제한, SQL identity/body read-back과 독립 RR 조회는 수용한다.
- v1 보존 후 v2/v3로 변경을 기록한 migration 방식은 적절하다. 현재 suite 통과는 동일 digest migration runner의 동시 실행 또는 모든 role privilege drift 복구까지 입증하지 않는다.
- publisher emit 뒤 마지막 recipient ACK가 먼저 도착하면 outbox가 COMPLETE여서 mark_outbox_sent의 SENDING 조건이 실패할 수 있다. 전달 정합성을 깨는 차단 지적으로 추가하지는 않지만, B1-3에서는 같은 token의 이미 ACK된 전송을 정상 종료할지 명시하고 retry 소진을 방지한다.
- receipt의 실제 dbt invocation/test 증명과 cohort outbox/recovery 전체는 이전에 구분한 B1-2/B1-3 범위로 유지한다.

위 두 항목 수정 후 재검토한다. 이번 결과는 검토 문서만 추가했으며 업무 구현은 변경하지 않았다.
