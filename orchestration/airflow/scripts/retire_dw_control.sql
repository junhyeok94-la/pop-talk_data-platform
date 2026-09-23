-- Run explicitly after pg_dump --schema=dw_control. Never run during startup.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $$
DECLARE t text; n bigint;
BEGIN
  IF (SELECT count(*) FROM pg_tables WHERE schemaname='dw_control') <> 17 THEN
    RAISE EXCEPTION 'Unexpected legacy table inventory';
  END IF;
  FOR t IN SELECT tablename FROM pg_tables WHERE schemaname='dw_control' LOOP
    EXECUTE format('LOCK TABLE dw_control.%I IN ACCESS EXCLUSIVE MODE', t);
    EXECUTE format('SELECT count(*) FROM dw_control.%I', t) INTO n;
    IF n <> (CASE WHEN t='schema_migrations' THEN 7 ELSE 0 END) THEN
      RAISE EXCEPTION 'Legacy table % has unexpected data', t;
    END IF;
  END LOOP;
END $$;
DROP TABLE dw_control.asset_deliveries, dw_control.asset_inbox, dw_control.asset_outbox,
  dw_control.dbt_invocation_artifacts, dw_control.dbt_invocation_registries,
  dw_control.dbt_invocation_reservations, dw_control.dbt_model_invocation_specs,
  dw_control.generation_gate_receipts, dw_control.generation_plans,
  dw_control.model_build_attempts, dw_control.model_build_slots, dw_control.model_cohorts,
  dw_control.model_execution_receipts, dw_control.model_results,
  dw_control.release_delivery_registries, dw_control.release_delivery_specs,
  dw_control.schema_migrations RESTRICT;
DROP SEQUENCE dw_control.fence_token_seq, dw_control.publisher_claim_token_seq,
  dw_control.work_claim_token_seq RESTRICT;
DROP SCHEMA dw_control RESTRICT;
COMMIT;
