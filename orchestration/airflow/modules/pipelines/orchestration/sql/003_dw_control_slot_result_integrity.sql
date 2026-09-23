-- slot의 상태 변경 권한과 결과 identity를 DB에서도 최소 단위로 강제한다.
SET LOCAL search_path = dw_control, pg_catalog;

ALTER TABLE model_results
  ADD CONSTRAINT model_results_slot_identity_unique
  UNIQUE (result_manifest_id, plan_id, model_unique_id, build_id);

ALTER TABLE model_build_slots
  DROP CONSTRAINT IF EXISTS model_build_slots_result_fk;

ALTER TABLE model_build_slots
  ADD CONSTRAINT model_build_slots_result_identity_fk
  FOREIGN KEY (result_manifest_id, plan_id, model_unique_id, build_id)
  REFERENCES model_results(result_manifest_id, plan_id, model_unique_id, build_id)
  ON DELETE RESTRICT;

ALTER TABLE model_build_slots
  ADD CONSTRAINT model_build_slots_reused_result_match
  CHECK (state <> 'REUSED' OR result_manifest_id = reuse_result_manifest_id);

REVOKE UPDATE ON model_build_slots FROM dw_control_runtime;
GRANT UPDATE (
  state, current_attempt_no, current_fence_token, claim_owner,
  claim_expires_at, heartbeat_at, result_manifest_id
) ON model_build_slots TO dw_control_runtime;
