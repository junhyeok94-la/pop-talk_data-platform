-- source/deployment 검증 증거가 없는 generation의 모델 실행을 차단한다.
SET LOCAL search_path = dw_control, pg_catalog;

CREATE TABLE generation_gate_receipts (
    gate_receipt_id text PRIMARY KEY,
    gate_kind text NOT NULL CHECK (gate_kind IN ('SOURCE_GATE','DEPLOYMENT_GATE')),
    generation_id text NOT NULL,
    plan_id text NOT NULL REFERENCES generation_plans(plan_id) ON DELETE RESTRICT,
    deployment_id char(64) NOT NULL,
    owner_unique_id text NOT NULL,
    source_snapshot_id text,
    evidence_sha256 char(64) NOT NULL,
    body jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (plan_id, gate_kind, owner_unique_id),
    CHECK (
      (gate_kind='SOURCE_GATE' AND source_snapshot_id IS NOT NULL)
      OR (gate_kind='DEPLOYMENT_GATE' AND source_snapshot_id IS NULL)
    )
);

REVOKE ALL ON generation_gate_receipts FROM dw_control_runtime;
GRANT SELECT, INSERT ON generation_gate_receipts TO dw_control_runtime;
