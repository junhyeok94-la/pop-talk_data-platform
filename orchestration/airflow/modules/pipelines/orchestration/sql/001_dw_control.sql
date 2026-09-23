-- Pop Talk DW 모델 단위 DAG의 영속 control plane 최초 스키마.
-- 이 파일은 dw_control_owner role로만 실행하며 DAG runtime은 DDL 권한을 갖지 않는다.

CREATE SCHEMA IF NOT EXISTS dw_control AUTHORIZATION dw_control_owner;
SET LOCAL search_path = dw_control, pg_catalog;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    sha256 char(64) NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE SEQUENCE IF NOT EXISTS fence_token_seq AS bigint START WITH 1;

CREATE TABLE IF NOT EXISTS generation_plans (
    plan_id text PRIMARY KEY,
    generation_id text NOT NULL UNIQUE,
    generation_sequence bigint NOT NULL UNIQUE CHECK (generation_sequence > 0),
    deployment_id char(64) NOT NULL,
    graph_digest char(64) NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS model_build_slots (
    plan_id text NOT NULL REFERENCES generation_plans(plan_id) ON DELETE RESTRICT,
    model_unique_id text NOT NULL,
    build_id text NOT NULL UNIQUE,
    mode text NOT NULL CHECK (mode IN ('REBUILD', 'REUSE')),
    state text NOT NULL CHECK (state IN ('PLANNED', 'CLAIMED', 'SUCCEEDED', 'REUSED', 'FAILED')),
    reuse_result_manifest_id text,
    current_attempt_no integer CHECK (current_attempt_no IS NULL OR current_attempt_no > 0),
    current_fence_token bigint CHECK (current_fence_token IS NULL OR current_fence_token > 0),
    claim_owner text,
    claim_expires_at timestamptz,
    heartbeat_at timestamptz,
    result_manifest_id text,
    PRIMARY KEY (plan_id, model_unique_id),
    UNIQUE (plan_id, model_unique_id, build_id),
    CHECK (
      (mode = 'REBUILD' AND reuse_result_manifest_id IS NULL)
      OR (mode = 'REUSE' AND reuse_result_manifest_id IS NOT NULL)
    ),
    CHECK (
      (state = 'PLANNED' AND mode = 'REBUILD' AND result_manifest_id IS NULL)
      OR (state = 'CLAIMED' AND mode = 'REBUILD' AND result_manifest_id IS NULL
          AND current_attempt_no IS NOT NULL AND current_fence_token IS NOT NULL
          AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
      OR (state = 'FAILED' AND mode = 'REBUILD' AND result_manifest_id IS NULL)
      OR (state = 'SUCCEEDED' AND mode = 'REBUILD' AND result_manifest_id IS NOT NULL)
      OR (state = 'REUSED' AND mode = 'REUSE' AND result_manifest_id IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS model_cohorts (
    cohort_manifest_id text PRIMARY KEY,
    plan_id text NOT NULL,
    model_unique_id text NOT NULL,
    build_id text NOT NULL UNIQUE,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cohort_manifest_id, plan_id, model_unique_id, build_id),
    FOREIGN KEY (plan_id, model_unique_id, build_id)
      REFERENCES model_build_slots(plan_id, model_unique_id, build_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS model_build_attempts (
    build_id text NOT NULL REFERENCES model_build_slots(build_id) ON DELETE RESTRICT,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    fence_token bigint NOT NULL UNIQUE CHECK (fence_token > 0),
    claim_owner text NOT NULL,
    state text NOT NULL CHECK (state IN ('CLAIMED', 'SUCCEEDED', 'FAILED', 'EXPIRED')),
    lease_expires_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    relation_id text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (build_id, attempt_no),
    UNIQUE (build_id, attempt_no, fence_token)
);

CREATE TABLE IF NOT EXISTS model_execution_receipts (
    build_id text NOT NULL,
    attempt_no integer NOT NULL,
    fence_token bigint NOT NULL,
    cohort_manifest_id text NOT NULL REFERENCES model_cohorts(cohort_manifest_id) ON DELETE RESTRICT,
    deployment_id char(64) NOT NULL,
    parent_vector_sha256 char(64) NOT NULL,
    claim_owner text NOT NULL,
    relation_id text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    content_sha256 char(64) NOT NULL,
    model_invocation_id text NOT NULL,
    model_unique_id text NOT NULL,
    model_run_results_sha256 char(64) NOT NULL,
    test_invocation_id text,
    test_run_results_sha256 char(64),
    executed_test_ids jsonb NOT NULL,
    tests_sha256 char(64) NOT NULL,
    relation_validation_query_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED', 'EMPTY_TEST_SET')),
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (build_id, attempt_no),
    FOREIGN KEY (build_id, attempt_no, fence_token)
      REFERENCES model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT,
    CHECK (
      (status = 'EMPTY_TEST_SET' AND test_invocation_id IS NULL
       AND test_run_results_sha256 IS NULL AND executed_test_ids = '[]'::jsonb)
      OR (status IN ('SUCCEEDED', 'FAILED') AND test_invocation_id IS NOT NULL
          AND test_run_results_sha256 IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS model_results (
    result_manifest_id text PRIMARY KEY,
    plan_id text NOT NULL,
    cohort_manifest_id text NOT NULL UNIQUE,
    model_unique_id text NOT NULL,
    build_id text NOT NULL UNIQUE,
    attempt_no integer NOT NULL,
    fence_token bigint NOT NULL,
    relation_id text NOT NULL UNIQUE,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    content_sha256 char(64) NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (plan_id, model_unique_id, build_id)
      REFERENCES model_build_slots(plan_id, model_unique_id, build_id) ON DELETE RESTRICT,
    FOREIGN KEY (cohort_manifest_id, plan_id, model_unique_id, build_id)
      REFERENCES model_cohorts(cohort_manifest_id, plan_id, model_unique_id, build_id) ON DELETE RESTRICT,
    FOREIGN KEY (build_id, attempt_no, fence_token)
      REFERENCES model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT,
    FOREIGN KEY (build_id, attempt_no)
      REFERENCES model_execution_receipts(build_id, attempt_no) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS asset_outbox (
    event_id text PRIMARY KEY,
    aggregate_kind text NOT NULL CHECK (aggregate_kind IN ('COHORT_READY', 'MODEL_READY')),
    aggregate_id text NOT NULL,
    asset_uri text NOT NULL,
    partition_key text NOT NULL,
    event_body jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('PENDING', 'SENDING', 'COMPLETE')),
    lease_owner text,
    lease_expires_at timestamptz,
    send_attempts integer NOT NULL DEFAULT 0 CHECK (send_attempts >= 0),
    last_sent_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (aggregate_kind, aggregate_id, asset_uri, partition_key)
);

CREATE TABLE IF NOT EXISTS asset_deliveries (
    event_id text NOT NULL REFERENCES asset_outbox(event_id) ON DELETE RESTRICT,
    consumer_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('PENDING', 'SENDING', 'ACKED', 'COMPLETED')),
    lease_owner text,
    lease_expires_at timestamptz,
    acknowledged_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (event_id, consumer_id)
);

CREATE TABLE IF NOT EXISTS asset_inbox (
    consumer_id text NOT NULL,
    event_id text NOT NULL,
    asset_uri text NOT NULL,
    partition_key text NOT NULL,
    aggregate_id text NOT NULL,
    event_body jsonb NOT NULL,
    work_kind text NOT NULL,
    work_identity text NOT NULL,
    state text NOT NULL CHECK (state IN ('PENDING', 'CLAIMED', 'COMPLETED', 'FAILED')),
    claim_owner text,
    claim_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (consumer_id, event_id),
    UNIQUE (consumer_id, work_kind, work_identity),
    FOREIGN KEY (event_id, consumer_id)
      REFERENCES asset_deliveries(event_id, consumer_id) ON DELETE RESTRICT
);

-- 순환 참조는 모든 표를 만든 뒤 추가한다. NOT VALID로 미루지 않고 즉시 기존 행까지 검증한다.
DO $$ BEGIN
  ALTER TABLE model_build_slots ADD CONSTRAINT model_build_slots_reuse_result_fk
    FOREIGN KEY (reuse_result_manifest_id) REFERENCES model_results(result_manifest_id) ON DELETE RESTRICT;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE model_build_slots ADD CONSTRAINT model_build_slots_result_fk
    FOREIGN KEY (result_manifest_id) REFERENCES model_results(result_manifest_id) ON DELETE RESTRICT;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS model_slots_recovery_idx
  ON model_build_slots(state, claim_expires_at);
CREATE INDEX IF NOT EXISTS outbox_recovery_idx
  ON asset_outbox(state, lease_expires_at);
CREATE INDEX IF NOT EXISTS inbox_recovery_idx
  ON asset_inbox(state, claim_expires_at);

REVOKE ALL ON SCHEMA dw_control FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA dw_control FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA dw_control FROM PUBLIC;
GRANT USAGE ON SCHEMA dw_control TO dw_control_runtime;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA dw_control TO dw_control_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dw_control TO dw_control_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE dw_control_owner IN SCHEMA dw_control
  GRANT SELECT, INSERT, UPDATE ON TABLES TO dw_control_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE dw_control_owner IN SCHEMA dw_control
  GRANT USAGE, SELECT ON SEQUENCES TO dw_control_runtime;
