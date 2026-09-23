-- v1 검토 보정: 권위 delivery registry, publisher/inbox fence, 최소 runtime 권한.
SET LOCAL search_path = dw_control, pg_catalog;

CREATE SEQUENCE IF NOT EXISTS publisher_claim_token_seq AS bigint START WITH 1;
CREATE SEQUENCE IF NOT EXISTS work_claim_token_seq AS bigint START WITH 1;

CREATE TABLE IF NOT EXISTS release_delivery_specs (
    deployment_id char(64) NOT NULL,
    model_unique_id text NOT NULL,
    asset_uri text NOT NULL,
    recipient_work jsonb NOT NULL,
    body_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id, model_unique_id),
    UNIQUE (deployment_id, asset_uri)
);

ALTER TABLE asset_outbox
  ADD COLUMN IF NOT EXISTS publisher_claim_token bigint;
ALTER TABLE asset_outbox
  ADD COLUMN IF NOT EXISTS last_sent_claim_token bigint;
ALTER TABLE asset_inbox
  ADD COLUMN IF NOT EXISTS work_claim_token bigint;

DO $$ BEGIN
  ALTER TABLE asset_outbox ADD CONSTRAINT asset_outbox_claim_token_positive
    CHECK (publisher_claim_token IS NULL OR publisher_claim_token > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE asset_inbox ADD CONSTRAINT asset_inbox_claim_token_positive
    CHECK (work_claim_token IS NULL OR work_claim_token > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

REVOKE ALL ON ALL TABLES IN SCHEMA dw_control FROM dw_control_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA dw_control FROM dw_control_runtime;

GRANT SELECT ON schema_migrations TO dw_control_runtime;
GRANT SELECT, INSERT ON generation_plans, model_cohorts, model_results,
  model_execution_receipts, release_delivery_specs TO dw_control_runtime;
GRANT SELECT, INSERT, UPDATE ON model_build_slots, model_build_attempts,
  asset_outbox, asset_deliveries, asset_inbox TO dw_control_runtime;
GRANT USAGE, SELECT ON fence_token_seq, publisher_claim_token_seq,
  work_claim_token_seq TO dw_control_runtime;

ALTER DEFAULT PRIVILEGES FOR ROLE dw_control_owner IN SCHEMA dw_control
  REVOKE ALL ON TABLES FROM dw_control_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE dw_control_owner IN SCHEMA dw_control
  REVOKE ALL ON SEQUENCES FROM dw_control_runtime;
