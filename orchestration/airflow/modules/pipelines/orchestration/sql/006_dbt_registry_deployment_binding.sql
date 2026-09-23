-- v5 invocation registry를 승인 deployment manifest/source digest에 직접 묶는다.
SET LOCAL search_path = dw_control, pg_catalog;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM dbt_invocation_registries)
     OR EXISTS (SELECT 1 FROM dbt_invocation_reservations) THEN
    RAISE EXCEPTION 'v6 requires no unbound v5 invocation registry/reservation rows';
  END IF;
END $$;

ALTER TABLE dbt_invocation_registries
  ADD COLUMN manifest_sha256 char(64) NOT NULL,
  ADD COLUMN model_version char(64) NOT NULL,
  ADD CONSTRAINT dbt_invocation_registry_deployment_identity_unique
    UNIQUE (deployment_id, manifest_sha256, model_version);

ALTER TABLE dbt_invocation_reservations
  ADD COLUMN manifest_sha256 char(64) NOT NULL,
  ADD COLUMN model_version char(64) NOT NULL,
  ADD CONSTRAINT dbt_invocation_reservation_deployment_fk
    FOREIGN KEY (deployment_id, manifest_sha256, model_version)
    REFERENCES dbt_invocation_registries(deployment_id, manifest_sha256, model_version)
    ON DELETE RESTRICT;

-- v5 권한을 다시 최소화해 새 identity 열은 runtime이 변경하지 못하게 한다.
REVOKE UPDATE ON dbt_invocation_reservations FROM dw_control_runtime;
GRANT UPDATE (
  state, dbt_native_invocation_id, artifact_path, heartbeat_at,
  started_at, completed_at, failure_reason
) ON dbt_invocation_reservations TO dw_control_runtime;
