"""Real plugin DB tests; native side effects are mocked and asserted explicitly."""

import asyncio
import copy
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.requests import Request

from airflow_workbench.app import app
from airflow_workbench.dashboard import actions, work
from airflow_workbench.monitoring import api as monitoring
from airflow_workbench.shared import auth
from airflow_workbench.shared.database import Store, Conflict
from workbench_test_db import isolated_database


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        self.target = actions.Target(dag_id="example", run_id="scheduled__2026-09-10")
        self.tasks = [dict(task_id="load", map_index=3, state="failed", try_number=1, dag_version={"version_number": 1}, dag_id=self.target.dag_id, dag_run_id=self.target.run_id), dict(task_id="publish", map_index=-1, state="upstream_failed", try_number=0, dag_version={"version_number": 1}, dag_id=self.target.dag_id, dag_run_id=self.target.run_id)]
        self.run = {"state": "failed", "start_date": "2026-09-10T00:00:00Z", "end_date": "2026-09-10T00:01:00Z", "bundle_version": "v1"}
        self.dag = {"is_paused": False, "tags": []}
        self.live = []
        self.manager = Mock()
        manager_patch = patch.object(auth, "get_auth_manager", return_value=self.manager)
        manager_patch.start()
        self.addCleanup(manager_patch.stop)
        self.user = SimpleNamespace(get_id=lambda: "alice")

    async def native(self, request, method, path, params=None, payload=None):
        if path.endswith("clearTaskInstances"):
            if payload["dry_run"] is False:
                self.live.append(copy.deepcopy(payload))
            return {"task_instances": copy.deepcopy(self.tasks), "total_entries": len(self.tasks)}
        return copy.deepcopy(self.run if "/dagRuns/" in path else self.dag)

    def test_profile_ownership_conflict_and_dag_scope(self):
        original = work.Profile(dag_ids=["example"], tags=["team:etl"])
        saved = work.save_profile("alice", original, {"example"})
        self.assertEqual(saved.version, 1)
        self.assertEqual(work.load_profile("alice").dag_ids, ["example"])
        self.assertEqual(work.load_profile("bob").dag_ids, [])
        with self.assertRaises(Conflict):
            work.save_profile("alice", original, {"example"})
        with self.assertRaises(HTTPException):
            work.save_profile("alice", work.Profile(dag_ids=["secret"]), {"example"})

    def test_monitoring_url_and_credential_validation(self):
        for url in ("javascript:alert(1)", "data:text/html,test", "file:///etc/passwd", "https://u:p@example.org", "https://example.org/?token=secret", "https://example.org/workbench/monitoring"):
            with self.subTest(url=url), self.assertRaises(ValidationError):
                monitoring.Screen(id="test", name="test", url=url)
        self.assertEqual(monitoring.Screen(id="safe", name="safe", url="https://metrics.example.org:8443/d/a?var-team=etl").origin(), "https://metrics.example.org:8443")

    def test_monitoring_admin_writes_viewer_reads_and_page_frame_policy(self):
        app.dependency_overrides[auth.access] = lambda: self.user
        self.addCleanup(app.dependency_overrides.clear)
        spec = {"version": 0, "screens": [{"id": "ops", "name": "운영", "url": "https://metrics.example.org/d/test"}, {"id": "disabled", "name": "비활성", "url": "https://hidden.example.org/", "enabled": False}]}
        with TestClient(app) as client, patch.object(monitoring, "can_edit", return_value=False):
            self.assertEqual(client.put("/api/monitoring", json=spec).status_code, 403)
        with TestClient(app) as client, patch.object(monitoring, "can_edit", return_value=True):
            self.assertEqual(client.put("/api/monitoring", json=spec).status_code, 200)
            self.assertEqual(client.put("/api/monitoring", json=spec).status_code, 409)
        with TestClient(app) as client, patch.object(monitoring, "can_edit", return_value=False):
            result = client.get("/api/monitoring").json()
            self.assertEqual([s["id"] for s in result["screens"]], ["ops"])
            policy = client.get("/monitoring").headers["content-security-policy"]
            self.assertIn("frame-src 'self' https://metrics.example.org", policy)
            self.assertNotIn("hidden.example.org", policy)
            self.assertNotIn("metrics.example.org", client.get("/dashboard").headers["content-security-policy"])

    def test_profile_and_retry_routes_do_not_require_global_admin(self):
        self.manager.is_authorized_view.return_value = True
        self.manager.is_authorized_configuration.return_value = False
        self.manager.is_authorized_custom_view.return_value = False
        for method, route in (("PUT", "work/profile"), ("POST", "work/retry-preview"), ("POST", "work/retry"), ("PUT", "work/dag-state")):
            req = Request({"type": "http", "method": method, "path": "/workbench/api/" + route, "scheme": "http", "server": ("localhost", 8080), "headers": [(b"x-workbench-request", b"1"), (b"origin", b"http://localhost:8080")]})
            asyncio.run(auth.access(req, self.user))
        self.manager.is_authorized_configuration.assert_not_called()
        bad = Request({"type": "http", "method": "POST", "path": "/workbench/api/work/retry", "scheme": "http", "server": ("localhost", 8080), "headers": [(b"x-workbench-request", b"1"), (b"origin", b"https://other.example")]})
        with self.assertRaises(HTTPException):
            asyncio.run(auth.access(bad, self.user))

    def test_dag_pause_and_resume_use_native_caller_api(self):
        native=AsyncMock(side_effect=[{"is_paused":True,"tags":[{"name":"model-lab"}]},{"is_paused":False},{"is_paused":False,"tags":[{"name":"model-lab"}]},{"is_paused":True}])
        self.manager.is_authorized_dag.return_value=True
        with patch.object(actions,"airflow_api",native):
            for desired in (False,True):
                result=asyncio.run(actions.dag_state(actions.DagState(dag_id="example",is_paused=desired,expected_is_paused=not desired),object(),self.user))
                self.assertEqual(result["is_paused"],desired)
        writes=[c for c in native.call_args_list if c.args[1]=="PATCH"]
        self.assertEqual([c.kwargs["payload"] for c in writes],[{"is_paused":False},{"is_paused":True}])
        self.assertTrue(all(c.args[2]=="/dags/example" for c in writes))

    def test_dag_state_denied_or_stale_never_patches(self):
        cases=[(False,{"is_paused":True,"tags":[{"name":"model-lab"}]}), (True,{"is_paused":True,"tags":[],"is_stale":True}), (True,{"is_paused":True,"tags":[],"has_import_errors":True}), (True,{"is_paused":False,"tags":[]})]
        for permission,current in cases:
            with self.subTest(current=current,permission=permission):
                native=AsyncMock(return_value=current)
                self.manager.is_authorized_dag.return_value=permission
                with patch.object(actions,"airflow_api",native),self.assertRaises(HTTPException):
                    asyncio.run(actions.dag_state(actions.DagState(dag_id="example",is_paused=False,expected_is_paused=True),object(),self.user))
                self.assertEqual(native.call_count,1)

    def test_native_permission_recheck_can_reject_pause_change(self):
        self.manager.is_authorized_dag.return_value=True
        native=AsyncMock(side_effect=[{"is_paused":True,"tags":[]},HTTPException(403,"permission revoked")])
        with patch.object(actions,"airflow_api",native),self.assertRaises(HTTPException) as caught:
            asyncio.run(actions.dag_state(actions.DagState(dag_id="example",is_paused=False,expected_is_paused=True),object(),self.user))
        self.assertEqual(caught.exception.status_code,403)

    def test_native_permission_denial_never_submits(self):
        with patch.object(actions, "airflow_api", AsyncMock(side_effect=HTTPException(403))):
            with self.assertRaises(HTTPException):
                asyncio.run(actions.preview(self.target, object(), self.user))
        with Store().connection() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM dashboard_actions").fetchone()[0], 0)

    def test_preview_then_mapped_retry_and_same_receipt_is_idempotent(self):
        async def run():
            p = await actions.preview(self.target, object(), self.user)
            self.assertFalse(self.live)
            request = actions.Confirmation(preview_id=p["preview_id"])
            self.assertEqual((await actions.retry(request, object(), self.user))["status"], "submitted")
            await actions.retry(request, object(), self.user)
        with patch.object(actions, "airflow_api", side_effect=self.native):
            asyncio.run(run())
        self.assertEqual(len(self.live), 1)
        body = self.live[0]
        self.assertEqual(body["task_ids"], [["load", 3], ["publish", -1]])
        for key in ("include_past", "include_future", "include_upstream", "include_downstream", "run_on_latest_version"):
            self.assertFalse(body[key])
        self.assertTrue(body["only_failed"])
        self.assertTrue(body["prevent_running_task"])

    def test_other_user_cannot_confirm_preview(self):
        async def run():
            p = await actions.preview(self.target, object(), self.user)
            with self.assertRaises(HTTPException) as caught:
                await actions.retry(actions.Confirmation(preview_id=p["preview_id"]), object(), SimpleNamespace(get_id=lambda: "bob"))
            self.assertEqual(caught.exception.status_code, 404)
        with patch.object(actions, "airflow_api", side_effect=self.native):
            asyncio.run(run())
        self.assertFalse(self.live)

    def test_state_change_between_preview_and_confirm_blocks_action(self):
        async def run():
            p = await actions.preview(self.target, object(), self.user)
            self.tasks[0]["try_number"] += 1
            with self.assertRaises(HTTPException) as caught:
                await actions.retry(actions.Confirmation(preview_id=p["preview_id"]), object(), self.user)
            self.assertEqual(caught.exception.status_code, 409)
        with patch.object(actions, "airflow_api", side_effect=self.native):
            asyncio.run(run())
        self.assertFalse(self.live)

    def test_expired_and_parallel_preview_claims(self):
        value = {"dag_id": "example", "run_id": "run"}
        a, b = actions.remember("a", value), actions.remember("b", value)
        actions.claim("a", a["preview_id"])
        with self.assertRaises(HTTPException):
            actions.claim("b", b["preview_id"])
        expired = actions.remember("a", {**value, "run_id": "expired"})
        with Store().connection() as db:
            db.execute("UPDATE dashboard_actions SET expires=0 WHERE id=?", (expired["preview_id"],))
        with self.assertRaises(HTTPException):
            actions.claim("a", expired["preview_id"])

    def test_ambiguous_native_failure_cannot_repeat_same_receipt(self):
        original = self.native
        async def failing(*args, **kwargs):
            payload = kwargs.get("payload") or {}
            if payload.get("dry_run") is False:
                self.live.append(payload)
                raise HTTPException(502, "unknown delivery")
            return await original(*args, **kwargs)
        async def run():
            p = await actions.preview(self.target, object(), self.user)
            request = actions.Confirmation(preview_id=p["preview_id"])
            with self.assertRaises(HTTPException):
                await actions.retry(request, object(), self.user)
            with self.assertRaises(HTTPException):
                await actions.retry(request, object(), self.user)
        with patch.object(actions, "airflow_api", side_effect=failing):
            asyncio.run(run())
        self.assertEqual(len(self.live), 1)

    def test_paused_running_and_model_lab_runs_are_not_cleared(self):
        for kind in ("paused", "running", "model-lab"):
            self.dag = {"is_paused": kind == "paused", "tags": [{"name": "model-lab"}] if kind == "model-lab" else []}
            self.run["state"] = "running" if kind == "running" else "failed"
            with patch.object(actions, "airflow_api", side_effect=self.native), self.assertRaises(HTTPException):
                asyncio.run(actions.preview(self.target, object(), self.user))
        self.assertFalse(self.live)

    def test_preview_does_not_accept_other_run_or_successful_task(self):
        for field, value in (("dag_run_id", "other"), ("state", "success")):
            original = self.tasks[0][field]
            self.tasks[0][field] = value
            with patch.object(actions, "airflow_api", side_effect=self.native), self.assertRaises(HTTPException):
                asyncio.run(actions.preview(self.target, object(), self.user))
            self.tasks[0][field] = original


if __name__ == "__main__":
    unittest.main(verbosity=2)
