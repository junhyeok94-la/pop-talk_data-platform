-- exact dbt model/test 실행을 SQL 시작 전에 예약하고 실행 증거를 불변 보존한다.
SET LOCAL search_path = dw_control, pg_catalog;

CREATE TABLE dbt_invocation_registries (
    deployment_id char(64) PRIMARY KEY,
    graph_digest char(64) NOT NULL,
    invocation_registry_digest char(64) NOT NULL,
    release_registry_digest char(64) NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (deployment_id, invocation_registry_digest),
    FOREIGN KEY (deployment_id, release_registry_digest)
      REFERENCES release_delivery_registries(deployment_id, registry_digest)
      ON DELETE RESTRICT
);

CREATE TABLE dbt_model_invocation_specs (
    deployment_id char(64) NOT NULL,
    model_unique_id text NOT NULL,
    model_selector text NOT NULL,
    owned_test_unique_ids jsonb NOT NULL,
    owned_test_selectors jsonb NOT NULL,
    body_sha256 char(64) NOT NULL,
    invocation_registry_digest char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id, model_unique_id),
    FOREIGN KEY (deployment_id, invocation_registry_digest)
      REFERENCES dbt_invocation_registries(deployment_id, invocation_registry_digest)
      ON DELETE RESTRICT,
    CHECK (jsonb_typeof(owned_test_unique_ids) = 'array'),
    CHECK (jsonb_typeof(owned_test_selectors) = 'array')
);

CREATE TABLE dbt_invocation_reservations (
    orchestration_invocation_id text PRIMARY KEY,
    build_id text NOT NULL,
    attempt_no integer NOT NULL,
    fence_token bigint NOT NULL,
    cohort_manifest_id text NOT NULL,
    plan_id text NOT NULL,
    deployment_id char(64) NOT NULL,
    model_unique_id text NOT NULL,
    invocation_kind text NOT NULL
      CHECK (invocation_kind IN ('MODEL', 'OWNED_TESTS', 'EMPTY_TEST_SET')),
    contract_sha256 char(64) NOT NULL,
    contract_body jsonb NOT NULL,
    claim_owner text NOT NULL,
    state text NOT NULL
      CHECK (state IN ('RESERVED','RUNNING','FILE_SEALED','COMPLETED','FAILED','ABANDONED')),
    dbt_native_invocation_id uuid,
    artifact_path text,
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (build_id, attempt_no, fence_token, invocation_kind),
    FOREIGN KEY (build_id, attempt_no, fence_token)
      REFERENCES model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT,
    FOREIGN KEY (cohort_manifest_id, plan_id, model_unique_id, build_id)
      REFERENCES model_cohorts(cohort_manifest_id, plan_id, model_unique_id, build_id)
      ON DELETE RESTRICT,
    FOREIGN KEY (deployment_id, model_unique_id)
      REFERENCES dbt_model_invocation_specs(deployment_id, model_unique_id) ON DELETE RESTRICT,
    CHECK ((invocation_kind = 'EMPTY_TEST_SET' AND dbt_native_invocation_id IS NULL)
           OR invocation_kind <> 'EMPTY_TEST_SET')
);

CREATE TABLE dbt_invocation_artifacts (
    orchestration_invocation_id text PRIMARY KEY
      REFERENCES dbt_invocation_reservations(orchestration_invocation_id) ON DELETE RESTRICT,
    dbt_native_invocation_id uuid,
    build_id text NOT NULL,
    attempt_no integer NOT NULL,
    fence_token bigint NOT NULL,
    cohort_manifest_id text NOT NULL,
    deployment_id char(64) NOT NULL,
    model_unique_id text NOT NULL,
    invocation_kind text NOT NULL,
    expected_unique_ids jsonb NOT NULL,
    executed_unique_ids jsonb NOT NULL,
    run_results_sha256 char(64),
    argv_sha256 char(64) NOT NULL,
    environment_sha256 char(64) NOT NULL,
    artifact_path text NOT NULL,
    status text NOT NULL CHECK (status IN ('SUCCEEDED','EMPTY_TEST_SET')),
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (build_id, attempt_no, fence_token, invocation_kind),
    FOREIGN KEY (build_id, attempt_no, fence_token)
      REFERENCES model_build_attempts(build_id, attempt_no, fence_token) ON DELETE RESTRICT,
    CHECK (jsonb_typeof(expected_unique_ids) = 'array'),
    CHECK (jsonb_typeof(executed_unique_ids) = 'array'),
    CHECK (
      (invocation_kind = 'EMPTY_TEST_SET' AND status = 'EMPTY_TEST_SET'
       AND dbt_native_invocation_id IS NULL AND run_results_sha256 IS NULL
       AND expected_unique_ids = '[]'::jsonb AND executed_unique_ids = '[]'::jsonb)
      OR
      (invocation_kind IN ('MODEL','OWNED_TESTS') AND status = 'SUCCEEDED'
       AND dbt_native_invocation_id IS NOT NULL AND run_results_sha256 IS NOT NULL)
    )
);

CREATE INDEX dbt_invocation_recovery_idx
  ON dbt_invocation_reservations(state, heartbeat_at);

REVOKE ALL ON dbt_invocation_registries, dbt_model_invocation_specs,
  dbt_invocation_reservations, dbt_invocation_artifacts FROM dw_control_runtime;
GRANT SELECT ON dbt_invocation_registries, dbt_model_invocation_specs TO dw_control_runtime;
GRANT SELECT, INSERT ON dbt_invocation_artifacts TO dw_control_runtime;
GRANT SELECT, INSERT ON dbt_invocation_reservations TO dw_control_runtime;
GRANT UPDATE (
  state, dbt_native_invocation_id, artifact_path, heartbeat_at,
  started_at, completed_at, failure_reason
) ON dbt_invocation_reservations TO dw_control_runtime;
