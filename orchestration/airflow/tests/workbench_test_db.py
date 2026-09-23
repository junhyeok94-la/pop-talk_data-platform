"""Each test receives a disposable schema in the real PostgreSQL engine."""

import os
import uuid
from unittest.mock import patch
from sqlalchemy import text
from airflow_workbench import metadata


def isolated_database(case):
    schema = "wb_test_" + uuid.uuid4().hex
    metadata.migrate(schema)

    def cleanup():
        # Only the randomly generated, validated test namespace can be dropped.
        assert schema.startswith("wb_test_")
        metadata.schema_name(schema)
        with metadata.engine().begin() as db:
            db.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))

    case.addCleanup(cleanup)
    env = patch.dict(os.environ, {"AIRFLOW_WORKBENCH_DB_SCHEMA": schema})
    env.start()
    case.addCleanup(env.stop)
    # Functional suites may issue many sequential UI calls. Dedicated protection
    # tests restore the production policy to exercise admission and pressure.
    from dataclasses import replace
    from airflow_workbench.dashboard import protection

    policy_patch = patch.object(
        protection,
        "policy",
        replace(protection.policy, rate=50, requests=16, compute_gap=0, lag_ms=1000),
    )
    policy_patch.start()
    case.addCleanup(policy_patch.stop)
    protection.guard.reset()
    case.addCleanup(protection.guard.reset)
