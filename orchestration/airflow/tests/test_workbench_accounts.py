"""Account-scoped dashboard API tests; no real accounts or database grants are created."""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, "/opt/airflow/plugins")

from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from airflow_workbench import studio
from airflow_workbench.app import app, dashboard_owner, get_user
from airflow_workbench.store import Conflict, Store
from workbench_test_db import isolated_database


class TestAuthManager:
    def is_authorized_view(self, *, user, **kw):
        return user.role != "blocked"

    def is_authorized_configuration(self, *, user, **kw):
        return user.role == "admin"

    def is_authorized_custom_view(self, **kw):
        return False


class AccountTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        env = patch.dict(os.environ, {"AIRFLOW_WORKBENCH_DATA_DIR": temp.name})
        env.start()
        self.addCleanup(env.stop)
        manager = patch(
            "airflow_workbench.shared.auth.get_auth_manager", return_value=TestAuthManager()
        )
        manager.start()
        self.addCleanup(manager.stop)
        self.users = {
            name: SimpleNamespace(get_id=lambda n=name: n, username=name, role=role)
            for name, role in [
                ("alice", "viewer"),
                ("bob", "viewer"),
                ("admin", "admin"),
                ("blocked", "blocked"),
            ]
        }

        def authenticated_user(request: Request):
            user = self.users.get(
                request.headers.get("authorization", "").removeprefix("Bearer ")
            )
            if user is None:
                raise HTTPException(401)
            return user

        # Replace identity verification only. Production access/ownership checks remain active.
        app.dependency_overrides[get_user] = authenticated_user
        self.addCleanup(app.dependency_overrides.clear)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def call(self, who, method, path, **kw):
        headers = {
            "Authorization": "Bearer " + who,
            "X-Workbench-Request": "1",
            "Origin": "http://testserver",
            **kw.pop("headers", {}),
        }
        return self.client.request(method, "/api/" + path, headers=headers, **kw)

    def boards(self, who):
        response = self.call(who, "GET", "studio/boards")
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def save(self, who, board, status=200, **kw):
        response = self.call(
            who, "PUT", "studio/boards/" + board["id"], json=board, **kw
        )
        self.assertEqual(response.status_code, status, response.text)
        return response.json()

    def test_defaults_and_same_id_edits_are_independent(self):
        alice, bob = self.boards("alice"), self.boards("bob")
        self.assertEqual({b["id"] for b in alice}, {"operations", "model-lab"})
        a, b = alice[0], bob[0]
        a.update(title="Alice private queries", variables={"team": "alice-secret"})
        b.update(title="Bob private board")
        self.save("alice", a)
        self.save("bob", b)
        for who, forbidden in [
            ("alice", "Bob private"),
            ("bob", "alice-secret"),
            ("admin", "alice-secret"),
        ]:
            self.assertNotIn(forbidden, json.dumps(self.boards(who)))
        self.assertIn("Alice private queries", json.dumps(self.boards("alice")))

    def test_guessed_id_and_spoofed_owner_do_not_access_another_account(self):
        board = studio.Board(id="alice_only", title="private").model_dump()
        saved = self.save("alice", board)
        self.save("bob", {**saved, "title": "tampered"}, status=404)
        for endpoint in [
            "studio/boards/alice_only/revisions",
            "studio/boards/alice_only/revisions?owner=alice",
        ]:
            self.assertEqual(self.call("bob", "GET", endpoint).status_code, 404)
        self.assertEqual(
            self.call(
                "bob", "PUT", "studio/preferences", json={"last_board_id": "alice_only"}
            ).status_code,
            404,
        )
        self.save("bob", {**saved, "owner": "alice"}, status=422)
        self.assertNotIn(
            "private",
            self.call(
                "bob",
                "GET",
                "studio/boards?owner=alice",
                headers={"X-Workbench-Owner": "alice"},
            ).text,
        )
        # Version zero may create an independent board with the same ID, never overwrite Alice's.
        self.save(
            "bob",
            {**board, "title": "bob copy"},
            headers={"X-Workbench-Owner": "alice"},
        )
        self.assertEqual(
            next(b for b in self.boards("alice") if b["id"] == "alice_only")["title"],
            "private",
        )

    def test_templates_are_private_even_with_identical_ids(self):
        for who in ("alice", "bob"):
            response = self.call(
                who, "POST", "studio/templates", json={"id": "same_id", "title": who}
            )
            self.assertEqual(response.status_code, 200, response.text)
        for who in ("alice", "bob"):
            self.assertEqual(
                [p["title"] for p in self.call(who, "GET", "studio/templates").json()],
                [who],
            )
        self.assertEqual(self.call("admin", "GET", "studio/templates").json(), [])

    def test_history_retention_and_conflicts_are_scoped(self):
        a, b = self.boards("alice")[0], self.boards("bob")[0]
        for version in range(24):
            a = self.save("alice", {**a, "title": f"alice-{version}"})
        b = self.save("bob", {**b, "title": "bob"})
        history = self.call("alice", "GET", f"studio/boards/{a['id']}/revisions").json()
        self.assertEqual(len(history), 20)
        self.assertEqual([h["version"] for h in history], list(range(24, 4, -1)))
        bob_history = self.call(
            "bob", "GET", f"studio/boards/{b['id']}/revisions"
        ).json()
        self.assertEqual(len(bob_history), 1)
        self.assertNotIn("alice-", json.dumps(bob_history))
        self.save("alice", {**history[0], "version": a["version"]})
        self.save("alice", a, status=409)

    def test_last_board_persists_per_account_and_rename_uses_same_id(self):
        self.boards("alice")
        self.boards("bob")
        self.assertEqual(
            self.call(
                "alice",
                "PUT",
                "studio/preferences",
                json={"last_board_id": "model-lab"},
            ).status_code,
            200,
        )
        self.assertEqual(
            self.call("alice", "GET", "studio/preferences").json()["last_board_id"],
            "model-lab",
        )
        self.assertEqual(
            self.call("bob", "GET", "studio/preferences").json()["last_board_id"],
            "operations",
        )
        owner_before = asyncio.run(dashboard_owner(self.users["alice"]))
        self.users["alice"].username = "renamed"
        self.assertEqual(
            asyncio.run(dashboard_owner(self.users["alice"])), owner_before
        )
        saved = self.save("alice", studio.Board(title="new").model_dump())
        self.assertEqual(
            self.call("alice", "GET", "studio/preferences").json()["last_board_id"],
            saved["id"],
        )

    def test_personal_edit_does_not_grant_operational_write_access(self):
        result = self.call("alice", "GET", "config")
        self.assertEqual(result.status_code, 200, result.text)
        self.assertFalse(result.json()["can_edit"])
        self.assertTrue(result.json()["can_edit_dashboard"])
        self.assertEqual(result.json()["dashboard_scope"], "personal")
        self.assertEqual(
            self.call("alice", "POST", "studio/validate", json={}).status_code, 200
        )
        for method, path in [
            ("POST", "datasources"),
            ("POST", "training/launch"),
            ("POST", "experiments/generation"),
            ("PUT", "dashboard"),
            ("POST", "studio/legacy/import"),
            ("GET", "studio/legacy"),
            ("GET", "boards"),
            ("GET", "dashboard"),
        ]:
            self.assertEqual(
                self.call("alice", method, path, json={}).status_code, 403, path
            )

    def test_csrf_authentication_and_plugins_permission_still_required(self):
        board = studio.Board().model_dump()
        for headers in [{"Origin": "https://evil.test"}, {"X-Workbench-Request": ""}]:
            self.save("alice", board, status=403, headers=headers)
        for who, status in [("unknown", 401), ("blocked", 403)]:
            self.assertEqual(self.call(who, "GET", "studio/boards").status_code, status)
            self.save(who, board, status=status)

    def test_parallel_saves_do_not_silently_overwrite(self):
        board = studio.Board.model_validate(self.boards("alice")[0])
        owner = asyncio.run(dashboard_owner(self.users["alice"]))

        def attempt():
            try:
                studio.save_board(owner, board)
                return "saved"
            except Conflict:
                return "conflict"

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: attempt(), range(2)))
        self.assertCountEqual(results, ["saved", "conflict"])

    def test_legacy_import_copies_history_and_templates_once_without_claiming_originals(
        self,
    ):
        old = studio.Board(
            id="operations", title="Previously shared", version=2
        ).model_dump()
        previous = {**old, "version": 1, "title": "Previous title"}
        panel = studio.Panel(id="template", title="Shared template").model_dump()
        with Store().connection() as db:
            db.executemany(
                "INSERT INTO documents VALUES (?,?,?)",
                [
                    ("studio:board:operations", json.dumps(old), 2),
                    ("studio:revision:operations:000001", json.dumps(previous), 1),
                    ("studio:template:template", json.dumps(panel), 1),
                ],
            )
        archive = self.call("admin", "GET", "studio/legacy").json()
        self.assertNotIn("Previously shared", json.dumps(self.boards("alice")))
        result = self.call("admin", "POST", "studio/legacy/import")
        self.assertEqual(result.status_code, 200, result.text)
        imported = result.json()["boards"][0]
        self.assertNotEqual(imported["id"], old["id"])
        self.assertEqual(imported["version"], 2)
        self.assertEqual(result.json()["templates"], 1)
        history = self.call(
            "admin", "GET", f"studio/boards/{imported['id']}/revisions"
        ).json()
        self.assertEqual(history[0], {**previous, "id": imported["id"]})
        self.assertEqual(len(self.boards("admin")), 3)
        self.assertEqual(
            self.call("admin", "POST", "studio/legacy/import").status_code, 409
        )
        self.assertEqual(self.call("admin", "GET", "studio/legacy").json(), archive)
        self.assertNotIn("Previously shared", json.dumps(self.boards("bob")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
