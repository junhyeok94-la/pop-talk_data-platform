-- 정상 cross-generation REUSE와 승인 release delivery registry 경계를 보정한다.
SET LOCAL search_path = dw_control, pg_catalog;

ALTER TABLE model_build_slots
  DROP CONSTRAINT IF EXISTS model_build_slots_result_identity_fk;

ALTER TABLE model_build_slots
  ADD COLUMN rebuilt_result_manifest_id text
  GENERATED ALWAYS AS (
    CASE WHEN mode = 'REBUILD' THEN result_manifest_id ELSE NULL END
  ) STORED;

ALTER TABLE model_build_slots
  ADD CONSTRAINT model_build_slots_rebuilt_result_identity_fk
  FOREIGN KEY (rebuilt_result_manifest_id, plan_id, model_unique_id, build_id)
  REFERENCES model_results(result_manifest_id, plan_id, model_unique_id, build_id)
  ON DELETE RESTRICT;

CREATE TABLE release_delivery_registries (
    deployment_id char(64) PRIMARY KEY,
    graph_digest char(64) NOT NULL,
    registry_digest char(64) NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (deployment_id, registry_digest)
);

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM release_delivery_specs) THEN
    RAISE EXCEPTION 'v4 requires no unregistered legacy delivery specs';
  END IF;
END $$;

ALTER TABLE release_delivery_specs
  ADD COLUMN registry_digest char(64) NOT NULL;
ALTER TABLE release_delivery_specs
  ADD CONSTRAINT release_delivery_specs_registry_fk
  FOREIGN KEY (deployment_id, registry_digest)
  REFERENCES release_delivery_registries(deployment_id, registry_digest)
  ON DELETE RESTRICT;

REVOKE INSERT ON release_delivery_specs FROM dw_control_runtime;
GRANT SELECT ON release_delivery_registries, release_delivery_specs TO dw_control_runtime;
