"""Model Lab invariants against isolated PostgreSQL and deterministic providers."""

import asyncio
import json
import os
import sys
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, "/opt/airflow/plugins")
from fastapi import HTTPException
from fastapi.testclient import TestClient
from airflow_workbench.app import app, access
from airflow_workbench.lab_contracts import Dataset, Endpoint, digest
from airflow_workbench.lab_store import LabStore
from airflow_workbench import lab_evaluation as evaluation
from airflow_workbench.store import Conflict
from workbench_test_db import isolated_database


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, {"AIRFLOW_WORKBENCH_ARTIFACT_DIR": self.temp.name})
        env.start()
        self.addCleanup(env.stop)
        self.user = SimpleNamespace(get_id=lambda: "alice", is_admin=True)
        app.dependency_overrides[access] = lambda: self.user
        self.addCleanup(app.dependency_overrides.clear)
        admin = patch(
            "airflow_workbench.lab_api.admin", side_effect=lambda u: u.is_admin
        )
        admin.start()
        self.addCleanup(admin.stop)
        trigger = patch(
            "airflow_workbench.lab_api.can_trigger",
            new_callable=AsyncMock,
            return_value=True,
        )
        self.can_trigger = trigger.start()
        self.addCleanup(trigger.stop)
        self.client = TestClient(app)
        self.headers = {"X-Workbench-Request": "1"}
        self.client.headers.update(self.headers)
        self.store = LabStore()
        self.project("alpha")
        self.endpoint = Endpoint(id="default-ollama", name="Test", version=1)
        self.store.put(
            "_global",
            "endpoint",
            {"name": "Test", "spec": self.endpoint.model_dump()},
            key="default-ollama",
        )
        self.experiment = self.post("/projects/alpha/experiment", {"name": "Baseline"})
        self.dataset = self.post(
            "/projects/alpha/dataset",
            {
                "name": "QA",
                "kind": "generation",
                "cases": [
                    {
                        "id": "one",
                        "input": "hello",
                        "review_status": "reviewed",
                        "required_text": ["hello"],
                    }
                ],
            },
        )

    def project(self, key):
        r = self.client.put("/api/lab/projects/" + key, json={"id": key, "name": key})
        self.assertEqual(r.status_code, 200, r.text)

    def test_connection_status_separates_pause_from_settings_and_never_writes(self):
        manager = SimpleNamespace(is_authorized_dag=lambda **kwargs: True)
        async def paused(*args, **kwargs):
            value = await self.native(*args, **kwargs)
            if args[2].endswith("/details"):
                value["is_paused"] = True
            return value
        with patch("airflow_workbench.lab_api.auth.get_auth_manager", return_value=manager), patch("airflow_workbench.lab_api.airflow_api", side_effect=paused) as native:
            result = self.client.get("/api/lab/endpoints/default-ollama/status")
        self.assertEqual(result.status_code, 200, result.text)
        self.assertTrue(result.json()["dag"]["is_paused"])
        self.assertEqual(result.json()["settings"]["state"], "ready")
        self.assertTrue(result.json()["can_change_state"])
        self.assertTrue(all(c.args[1] == "GET" for c in native.call_args_list))

    def test_active_dag_with_wrong_pool_is_not_ready_and_cannot_submit(self):
        manager = SimpleNamespace(is_authorized_dag=lambda **kwargs: True)
        async def wrong_pool(*args, **kwargs):
            value = await self.native(*args, **kwargs)
            if args[2].startswith("/pools/"):
                value["slots"] = 9
            return value
        with patch("airflow_workbench.lab_api.auth.get_auth_manager", return_value=manager), patch("airflow_workbench.lab_api.airflow_api", side_effect=wrong_pool) as native:
            report = self.client.get("/api/lab/endpoints/default-ollama/status").json()
            response = self.client.post("/api/lab/projects/alpha/evaluate", json={"request_id": str(uuid.uuid4()), "experiment_id": self.experiment["id"], "dataset_id": self.dataset["id"], "endpoint_id": "default-ollama", "model": "example:latest"})
        self.assertFalse(report["dag"]["is_paused"])
        self.assertEqual(report["settings"]["state"], "blocked")
        self.assertEqual(response.status_code, 409)
        self.assertTrue(all(c.args[1] == "GET" for c in native.call_args_list))

    def test_connection_catalog_requires_admin_and_redacts_values(self):
        self.user.is_admin = False
        self.assertEqual(self.client.get("/api/lab/connection-options").status_code, 403)
        self.user.is_admin = True
        manager = SimpleNamespace(is_authorized_connection=lambda **kwargs: kwargs["details"].conn_id != "hidden")
        metadata = {"connection_id": "visible", "source": "environment", "source_label": "환경변수", "resolved": True, "valid": True}
        with patch("airflow_workbench.lab_api.auth.get_auth_manager", return_value=manager), patch("airflow_workbench.lab_api.airflow_api", AsyncMock(return_value={"connections": [{"connection_id": "visible", "host": "https://private.internal", "password": "secret-never-return"}]})), patch("airflow_workbench.lab_api.connections.environment_ids", return_value=["hidden", "visible"]), patch("airflow_workbench.lab_api.connections.describe", return_value=metadata) as describe:
            response = self.client.get("/api/lab/connection-options")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([r["connection_id"] for r in response.json()["connections"]], ["visible"])
        self.assertNotIn("private.internal", response.text)
        self.assertNotIn("secret-never-return", response.text)
        describe.assert_called_once_with("visible")

    def post(self, path, body):
        r = self.client.post("/api/lab" + path, json=body)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    async def native(self, request, method, path, **kwargs):
        if path.endswith("/details"):
            return {
                "tags": [
                    "model-lab",
                    "contract:lab-v1",
                    "config:" + self.endpoint.fingerprint(),
                ],
                "is_paused": False,
            }
        if path.endswith("/tasks"):
            return {
                "tasks": [
                    {
                        "operator_name": "RemoteEvaluationOperator",
                        "pool": "model_lab_gpu",
                    }
                ]
            }
        if path.startswith("/pools/"):
            return {"slots": 1, "include_deferred": True}
        if method == "POST":
            return {"state": "queued", **kwargs["payload"]}
        return {"state": "success"}

    def launch(self, **changes):
        value = {
            "request_id": str(uuid.uuid4()),
            "experiment_id": self.experiment["id"],
            "dataset_id": self.dataset["id"],
            "endpoint_id": "default-ollama",
            "model": "example:latest",
        }
        value.update(changes)
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            return self.post("/projects/alpha/evaluate", value)

    def finish(self, run, rows=None):
        rows = rows or [
            {
                "id": "one",
                "input": "hello",
                "category": "general",
                "review_status": "reviewed",
                "output": "hello",
                "score": 1,
                "latency_ms": 1,
            }
        ]
        evaluation.write_artifact(
            run["id"],
            {
                "signature": digest(
                    {
                        "endpoint": run["endpoint"],
                        "config": run["config"],
                        "dataset": run["dag_conf"]["model_lab"]["dataset"],
                    }
                ),
                "status": "success",
                "rows": rows,
                "summary": evaluation.summarize(rows, len(rows)),
                "model_revision": "sha-model-v1",
            },
        )

    def test_project_scope_and_roles(self):
        self.user = SimpleNamespace(get_id=lambda: "bob", is_admin=False)
        self.assertEqual(
            self.client.get("/api/lab/projects/alpha/dataset").status_code, 403
        )
        self.project("beta")
        self.assertEqual(self.client.get("/api/lab/projects/beta/dataset").json(), [])
        self.assertEqual(
            [p["id"] for p in self.client.get("/api/lab/bootstrap").json()["projects"]],
            ["beta"],
        )
        p = self.store.get("_global", "project", "alpha")
        self.store.patch("_global", "project", "alpha", {"members": {"bob": "viewer"}})
        self.assertEqual(
            self.client.get("/api/lab/projects/alpha/dataset").status_code, 200
        )
        self.assertEqual(
            self.client.post(
                "/api/lab/projects/alpha/dataset", json=self.dataset["spec"]
            ).status_code,
            403,
        )

    def test_dataset_immutable_and_validation(self):
        newer = self.post(
            "/projects/alpha/dataset", {**self.dataset["spec"], "description": "new"}
        )
        self.assertNotEqual(newer["id"], self.dataset["id"])
        self.assertNotEqual(newer["sha256"], self.dataset["sha256"])
        bad = {
            "name": "Invalid",
            "kind": "embedding",
            "cases": [{"id": "a", "input": "q", "relevance": {"missing": 2}}],
            "corpus": [{"id": "doc", "text": "text"}],
        }
        self.assertEqual(
            self.client.post("/api/lab/projects/alpha/dataset", json=bad).status_code,
            422,
        )

    def test_project_owner_without_airflow_trigger_permission(self):
        self.user = SimpleNamespace(get_id=lambda: "alice", is_admin=False)
        self.can_trigger.return_value = False
        permissions = self.client.get("/api/lab/bootstrap").json()[
            "evaluation_permissions"
        ]
        self.assertFalse(permissions[self.endpoint.id]["can_trigger"])
        self.assertEqual(permissions[self.endpoint.id]["dag_id"], self.endpoint.dag_id())
        with patch(
            "airflow_workbench.lab_api.airflow_api", new_callable=AsyncMock
        ) as native:
            response = self.client.post(
                "/api/lab/projects/alpha/evaluate",
                json={
                    "request_id": str(uuid.uuid4()),
                    "experiment_id": self.experiment["id"],
                    "dataset_id": self.dataset["id"],
                    "endpoint_id": self.endpoint.id,
                    "model": "example:latest",
                },
            )
        self.assertEqual(response.status_code, 403, response.text)
        self.assertIn("Viewer", response.json()["detail"])
        native.assert_not_awaited()
        self.assertEqual(self.store.list("alpha", "run"), [])

    def test_submission_refusal_and_transport_failure_have_distinct_states(self):
        from airflow_workbench.lab_api import submit

        for code, initial, expected in [
            (401, "submitting", "submission_rejected"),
            (403, "submitting", "submission_rejected"),
            (502, "submitting", "submission_unknown"),
            (403, "submission_unknown", "submission_unknown"),
        ]:
            with self.subTest(code=code, initial=initial):
                run = self.launch()
                run = self.store.patch("alpha", "run", run["id"], {"status": initial})
                with patch(
                    "airflow_workbench.lab_api.airflow_api",
                    new_callable=AsyncMock,
                    side_effect=HTTPException(code, "native rejection"),
                ):
                    with self.assertRaises(HTTPException):
                        asyncio.run(submit(None, "alpha", run, run["dag_conf"]))
                self.assertEqual(
                    self.store.get("alpha", "run", run["id"])["status"], expected
                )
                if expected == "submission_rejected":
                    with patch(
                        "airflow_workbench.lab_api.airflow_api", new_callable=AsyncMock
                    ) as native:
                        detail = self.client.get(
                            f"/api/lab/projects/alpha/run/{run['id']}"
                        )
                    self.assertEqual(detail.status_code, 200)
                    self.assertEqual(detail.json()["status"], expected)
                    self.assertNotIn("dag_conf", detail.json())
                    native.assert_not_awaited()
                    recovered = self.launch(**run["dag_conf"]["model_lab"]["config"])
                    self.assertEqual(recovered["id"], run["id"])
                    self.assertEqual(recovered["status"], "queued")

    def test_cancel_rejected_submission_finishes_without_external_acknowledgement(self):
        for kind in ["evaluation", "training"]:
            with self.subTest(kind=kind):
                run = self.launch()
                self.store.patch("alpha", "run", run["id"], {
                    "kind": kind, "status": "submission_rejected"
                })
                path = f"/api/lab/projects/alpha/run/{run['id']}"
                with patch(
                    "airflow_workbench.lab_api.airflow_api", new_callable=AsyncMock
                ) as native, patch(
                    "airflow_workbench.lab_api.worker_client.call", new_callable=AsyncMock
                ) as worker:
                    cancelled = self.client.post(path + "/cancel")
                    self.assertEqual(cancelled.status_code, 200, cancelled.text)
                    self.assertEqual(cancelled.json()["status"], "cancelled")
                    self.assertFalse(cancelled.json()["dag_run_created"])
                    self.assertNotIn("dag_conf", cancelled.json())
                    self.assertEqual(self.client.get(path).json()["status"], "cancelled")
                    repeated = self.client.post(path + "/cancel").json()
                    self.assertEqual(repeated["version"], cancelled.json()["version"])
                    native.assert_not_awaited()
                    worker.assert_not_awaited()
                self.assertFalse(evaluation.artifact_path(run["id"]).with_suffix(".cancel").exists())

    def test_cancel_pending_submission_does_not_resubmit_or_claim_missing_run_stopped(self):
        from airflow_workbench.lab_api import submit

        run = self.launch()
        self.store.patch("alpha", "run", run["id"], {"status": "submission_unknown"})
        path = f"/api/lab/projects/alpha/run/{run['id']}"
        cancelled = self.client.post(path + "/cancel").json()
        self.assertTrue(cancelled["cancel_requested"])
        self.assertEqual(cancelled["status"], "submission_unknown")
        with patch(
            "airflow_workbench.lab_api.airflow_api", new_callable=AsyncMock,
            side_effect=HTTPException(404, "not found"),
        ):
            detail = self.client.get(path).json()
        self.assertEqual(detail["status"], "submission_unknown")
        self.assertIn("DAG 생성 여부", detail["message"])
        self.assertTrue(evaluation.artifact_path(run["id"]).with_suffix(".cancel").exists())
        with patch(
            "airflow_workbench.lab_api.airflow_api", new_callable=AsyncMock
        ) as native:
            body = run["dag_conf"]["model_lab"]["config"]
            repeated = self.post("/projects/alpha/evaluate", body)
            self.assertTrue(repeated["cancel_requested"])
            # Even a caller holding a stale pre-cancel snapshot must not submit.
            asyncio.run(submit(None, "alpha", run, run["dag_conf"]))
            native.assert_not_awaited()

    def test_cancel_arriving_before_native_refusal_is_reconciled(self):
        run = self.launch()
        self.store.patch("alpha", "run", run["id"], {
            "status": "submission_rejected", "cancel_requested": True,
        })
        with patch(
            "airflow_workbench.lab_api.airflow_api", new_callable=AsyncMock
        ) as native:
            detail = self.client.get(f"/api/lab/projects/alpha/run/{run['id']}").json()
            native.assert_not_awaited()
        self.assertEqual(detail["status"], "cancelled")
        self.assertFalse(detail["dag_run_created"])

    def test_cancel_during_rejected_request_retry_waits_for_actual_dag(self):
        from airflow_workbench.lab_api import submit

        run = self.launch()
        self.store.patch("alpha", "run", run["id"], {"status": "submission_rejected"})
        path = f"/api/lab/projects/alpha/run/{run['id']}"

        async def native(request, method, native_path, **kwargs):
            current = self.store.get("alpha", "run", run["id"])
            self.assertEqual(current["status"], "submitting")
            cancelled = self.client.post(path + "/cancel").json()
            self.assertTrue(cancelled["cancel_requested"])
            self.assertNotEqual(cancelled["status"], "cancelled")
            return {"state": "queued"}

        with patch("airflow_workbench.lab_api.airflow_api", side_effect=native):
            result = asyncio.run(submit(None, "alpha", run, run["dag_conf"]))
        self.assertEqual(result["status"], "queued")
        self.assertTrue(result["cancel_requested"])
        self.assertTrue(evaluation.artifact_path(run["id"]).with_suffix(".cancel").exists())

    def test_cancel_finished_run_preserves_result(self):
        run = self.launch()
        self.store.patch("alpha", "run", run["id"], {"status": "success"})
        response = self.client.post(f"/api/lab/projects/alpha/run/{run['id']}/cancel")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "success")
        self.assertFalse(response.json().get("cancel_requested", False))
        self.assertFalse(evaluation.artifact_path(run["id"]).with_suffix(".cancel").exists())

    def test_request_id_conflicts_and_recovery(self):
        key = str(uuid.uuid4())
        first = self.launch(request_id=key)
        again = self.launch(request_id=key)
        self.assertEqual(first["id"], again["id"])
        value = {
            "request_id": key,
            "experiment_id": self.experiment["id"],
            "dataset_id": self.dataset["id"],
            "endpoint_id": "default-ollama",
            "model": "changed",
        }
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            response = self.client.post("/api/lab/projects/alpha/evaluate", json=value)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(len(self.store.list("alpha", "run")), 1)

    def test_model_review_and_release(self):
        run = self.launch()
        self.finish(run)
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            self.post(f"/projects/alpha/run/{run['id']}/baseline", {})
            model = self.post(
                "/projects/alpha/model",
                {"name": "candidate", "evaluation_run_id": run["id"]},
            )
        self.assertEqual(
            self.client.post(
                "/api/lab/projects/alpha/release",
                json={"model_version_id": model["id"], "target": "test"},
            ).status_code,
            409,
        )
        self.post(
            f"/projects/alpha/model/{model['id']}/review",
            {"decision": "approved", "reason": "Cases reviewed"},
        )
        release = self.post(
            "/projects/alpha/release",
            {"model_version_id": model["id"], "target": "test"},
        )
        self.assertEqual(release["status"], "configuration_ready")
        self.assertEqual(release["manifest"]["model_revision"], "sha-model-v1")
        self.assertNotIn("password", json.dumps(release))

    def test_compare_rejects_different_dataset(self):
        baseline = self.launch()
        self.finish(baseline)
        another = self.post(
            "/projects/alpha/dataset",
            {**self.dataset["spec"], "description": "different"},
        )
        candidate = self.launch(dataset_id=another["id"])
        self.finish(candidate)
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            self.post(f"/projects/alpha/run/{baseline['id']}/baseline", {})
            response = self.client.get(
                f"/api/lab/projects/alpha/compare/{candidate['id']}"
            )
        self.assertEqual(response.status_code, 409)

    def test_drafts_are_personal(self):
        r = self.client.put(
            "/api/lab/projects/alpha/draft/experiment",
            json={"values": {"system": "alice draft"}},
        )
        self.assertEqual(r.status_code, 200)
        self.user = SimpleNamespace(get_id=lambda: "bob", is_admin=True)
        self.assertEqual(
            self.client.get("/api/lab/projects/alpha/draft/experiment").json()[
                "values"
            ],
            {},
        )

    def test_unreviewed_and_json_subset_scores(self):
        case = {"review_status": "draft", "required_text": ["x"], "expected_json": None}
        self.assertIsNone(evaluation.generation_score(case, "x"))
        case = {
            "review_status": "reviewed",
            "required_text": [],
            "expected_json": {"intent": "search"},
        }
        self.assertEqual(
            evaluation.generation_score(case, '{"intent":"search","limit":5}'), 1
        )
        self.assertEqual(evaluation.generation_score(case, '{"intent":"other"}'), 0)
        self.assertIsNone(evaluation.summarize([], 3)["pass_rate"])

    def test_retrieval_graded_unknown_and_empty(self):
        c = {"review_status": "reviewed", "relevance": {"a": 2, "b": 1}}
        metrics = evaluation.retrieval_score(c, ["b", "a"], 2)
        self.assertEqual(metrics["recall"], 1)
        self.assertLess(metrics["ndcg"], 1)
        self.assertEqual(
            evaluation.retrieval_score({**c, "relevance": None}, ["a"], 1), {}
        )
        self.assertNotIn(
            "recall", evaluation.retrieval_score({**c, "relevance": {}}, ["a"], 1)
        )

    def test_remote_evaluation_restart_and_cancel(self):
        config = {
            "model": "test",
            "system": "",
            "temperature": 0.2,
            "max_tokens": 16,
            "k": 1,
        }
        dataset = Dataset(
            name="Tests",
            kind="generation",
            cases=[
                {
                    "id": "a",
                    "input": "q",
                    "review_status": "reviewed",
                    "required_text": ["hello"],
                },
                {"id": "b", "input": "q2"},
            ],
        ).model_dump()

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        generated = AsyncMock(return_value={"output": "hello"})
        with patch.object(
            evaluation, "client_for", return_value=Client()
        ), patch.object(
            evaluation, "request_json", AsyncMock(return_value={"models": []})
        ), patch.object(
            evaluation, "generate", generated
        ):
            rid = uuid.uuid4().hex
            result = asyncio.run(
                evaluation.evaluate(rid, self.endpoint.model_dump(), config, dataset)
            )
            self.assertEqual(result["summary"]["scored"], 1)
            self.assertEqual(result["summary"]["total"], 2)
            again = asyncio.run(
                evaluation.evaluate(rid, self.endpoint.model_dump(), config, dataset)
            )
            self.assertEqual(generated.await_count, 2)
            self.assertEqual(again["status"], "success")
            cancelled = uuid.uuid4().hex
            evaluation.artifact_path(cancelled).with_suffix(".cancel").write_text(
                "cancel"
            )
            result = asyncio.run(
                evaluation.evaluate(
                    cancelled, self.endpoint.model_dump(), config, dataset
                )
            )
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(generated.await_count, 2)

    def test_foreign_artifact_is_not_exposed(self):
        run = self.launch()
        evaluation.write_artifact(
            run["id"],
            {
                "signature": "wrong",
                "status": "success",
                "rows": [{"output": "foreign"}],
            },
        )
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            response = self.client.get("/api/lab/projects/alpha/run/" + run["id"])
        self.assertEqual(response.json()["status"], "result_missing")
        self.assertIsNone(response.json()["result"])
        self.assertNotIn("foreign", response.text)

    def test_unknown_retrieval_labels_not_treated_as_wrong(self):
        result = evaluation.retrieval_score(
            {"review_status": "reviewed", "relevance": {"a": 2}}, ["unknown", "a"], 2
        )
        self.assertEqual(result["label_coverage"], 0.5)
        self.assertNotIn("recall", result)

    def test_recover_submission_uses_saved_inputs(self):
        run = self.launch()
        self.store.patch("alpha", "run", run["id"], {"status": "submission_unknown"})
        with patch(
            "airflow_workbench.lab_api.evaluation_ready",
            side_effect=AssertionError("must not rebuild accepted request"),
        ):
            again = self.launch(request_id=run["request_id"])
        self.assertEqual(again["id"], run["id"])

    def test_upstream_provider_payloads(self):
        import httpx

        async def check(provider, response_body):
            sent = []

            def handle(request):
                sent.append((str(request.url), json.loads(request.content)))
                return httpx.Response(200, json=response_body)

            spec = Endpoint(
                id="remote",
                name="Remote",
                provider=provider,
                connection_id="test",
                capabilities=["generation"],
            )
            config = {
                "model": "model-v1",
                "system": "System",
                "temperature": 0.2,
                "max_tokens": 32,
                "json_mode": True,
                "options": {},
            }
            async with httpx.AsyncClient(
                base_url="https://test.invalid/v1/",
                transport=httpx.MockTransport(handle),
            ) as c:
                result = await evaluation.generate(
                    c,
                    spec,
                    config,
                    {"input": "Question", "reference": {"secret": "never send"}},
                )
            self.assertEqual(result["output"], "ok")
            self.assertNotIn("never send", json.dumps(sent))
            return sent[0]

        url, payload = asyncio.run(
            check(
                "openai",
                {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]},
            )
        )
        self.assertTrue(url.endswith("/v1/chat/completions"))
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        url, payload = asyncio.run(
            check("gemini", {"candidates": [{"content": {"parts": [{"text": "ok"}]}}]})
        )
        self.assertTrue(url.endswith("/models/model-v1:generateContent"))
        self.assertEqual(
            payload["generationConfig"]["responseMimeType"], "application/json"
        )

    def test_case_review_project_access_and_storage(self):
        run = self.launch()
        self.finish(run)
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            self.post(
                f"/projects/alpha/run/{run['id']}/cases/one/review",
                {"score": 0.5, "reason": "Partially correct"},
            )
        self.assertEqual(
            self.store.get("alpha", "run", run["id"])["case_reviews"]["one"]["score"],
            0.5,
        )

    def test_reservation_renewal_checks_expiry_and_environment(self):
        from airflow_workbench.executor_state import renew
        from fastapi import HTTPException
        import time

        with self.store.connection() as db:
            db.execute(
                "INSERT INTO executor_reservations VALUES (?,?,?)",
                ("alpha", "reservation", time.time() + 60),
            )
        self.assertEqual(renew("reservation", env="alpha"), {"renewed": True})
        with self.assertRaises(HTTPException):
            renew("reservation", env="beta")
        with self.store.connection() as db:
            db.execute(
                "UPDATE executor_reservations SET expires=0 WHERE id=?",
                ("reservation",),
            )
        with self.assertRaises(HTTPException):
            renew("reservation", env="alpha")

    def test_native_success_without_artifact_not_registerable(self):
        run = self.launch()
        with patch("airflow_workbench.lab_api.airflow_api", side_effect=self.native):
            response = self.client.post(
                "/api/lab/projects/alpha/model",
                json={"name": "missing", "evaluation_run_id": run["id"]},
            )
        self.assertEqual(response.status_code, 409)


if __name__ == "__main__":
    unittest.main()
