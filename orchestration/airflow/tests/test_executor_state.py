"""Real PostgreSQL tests for the executor-only API and cross-environment bounds."""

import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from fastapi.testclient import TestClient
from airflow_workbench import executor_state as state, environments
from workbench_test_db import isolated_database


class ExecutorStateTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        for name in ("alpha", "beta"):
            environments.save(
                environments.Environment(
                    id=name,
                    name=name,
                    backend="http",
                    connection_id=name,
                    dag_prefix=name,
                    pool=name,
                )
            )
        self.connection = patch.object(
            state.worker_client,
            "connection",
            lambda name: ("http://executor", name * 40),
        )
        self.connection.start()
        self.addCleanup(self.connection.stop)
        self.client = TestClient(state.app)
        self.addCleanup(self.client.close)

    def headers(self, env="alpha"):
        return {"Authorization": "Bearer " + env * 40, "X-Workbench-Executor": env}

    def create(self, key="key", env="alpha", **extra):
        request = json.dumps({"key": key, **extra})
        return self.client.post(
            "/jobs", json={"key": key, "request": request}, headers=self.headers(env)
        )

    def test_auth_rejects_missing_spoofed_and_unknown_environment(self):
        self.assertEqual(self.client.get("/jobs").status_code, 401)
        headers = self.headers()
        headers["X-Workbench-Executor"] = "beta"
        self.assertEqual(self.client.get("/jobs", headers=headers).status_code, 401)
        self.assertEqual(
            self.client.get("/jobs", headers=self.headers("missing")).status_code, 401
        )

    def test_job_ownership_and_release_are_scoped(self):
        job = self.create().json()["job"]
        self.assertEqual(
            self.client.get(
                "/jobs/" + job["id"], headers=self.headers("beta")
            ).status_code,
            404,
        )
        self.assertEqual(
            self.client.get("/jobs", headers=self.headers("beta")).json(), []
        )
        reservation = self.client.post(
            "/reservations", headers=self.headers("beta")
        ).json()
        self.client.post(
            "/reservations/" + reservation["id"] + "/release", headers=self.headers()
        )
        self.assertEqual(self.create("next", "beta").status_code, 409)

    def test_idempotency_conflict_and_terminal_result(self):
        first = self.create().json()
        self.assertEqual(self.create().json()["job"]["id"], first["job"]["id"])
        self.assertEqual(self.create(changed=True).status_code, 409)
        route = "/jobs/" + first["job"]["id"] + "/transition"
        for value in ("running", "cancelling", "cancelled"):
            self.assertTrue(
                self.client.post(
                    route, headers=self.headers(), json={"status": value}
                ).json()["changed"]
            )
        late = self.client.post(
            route,
            headers=self.headers(),
            json={"status": "success", "result": {"late": True}},
        ).json()
        self.assertFalse(late["changed"])
        self.assertEqual(late["job"]["status"], "cancelled")

    def test_concurrent_admission_and_environment_independence(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            codes = list(
                pool.map(lambda key: self.create(key).status_code, ("one", "two"))
            )
        self.assertEqual(sorted(codes), [200, 409])
        self.assertEqual(self.create("other", "beta").status_code, 200)
        self.client.post("/recover", headers=self.headers())
        self.assertEqual(
            self.client.get("/jobs/other", headers=self.headers("beta")).json()[
                "status"
            ],
            "queued",
        )

    def test_no_generic_sql_or_extra_job_fields(self):
        self.assertEqual(
            self.client.post(
                "/sql", json={"sql": "SELECT 1"}, headers=self.headers()
            ).status_code,
            404,
        )
        self.assertEqual(
            self.client.post(
                "/jobs",
                json={"key": "key", "request": "{}", "environment_id": "beta"},
                headers=self.headers(),
            ).status_code,
            422,
        )

    def test_mounted_api_has_own_auth_without_granting_user_access(self):
        from airflow_workbench.app import app, access
        from fastapi import HTTPException

        async def deny():
            raise HTTPException(401, "No human session")

        app.dependency_overrides[access] = deny
        self.addCleanup(app.dependency_overrides.clear)
        with TestClient(app) as client:
            self.assertEqual(
                client.get("/executor-state/jobs", headers=self.headers()).status_code,
                200,
            )
            self.assertEqual(
                client.get("/api/config", headers=self.headers()).status_code, 401
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
