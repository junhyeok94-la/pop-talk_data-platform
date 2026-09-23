"""Bounded real PostgreSQL contention; never mutates business DAGs or DB roles."""

import asyncio
import concurrent.futures
import threading
import time
import unittest
from dataclasses import replace
from unittest.mock import Mock, patch

import sqlalchemy as sa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from airflow_workbench.dashboard import protection, cache, performance, studio
from airflow_workbench.dashboard.database import Store
from airflow_workbench.shared import metadata
from workbench_test_db import isolated_database

PRODUCTION = protection.policy


class ProtectionTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        self.set_policy(compute_gap=0)

    def set_policy(self, **values):
        p = patch.object(protection, "policy", replace(PRODUCTION, **values))
        p.start()
        self.addCleanup(p.stop)
        protection.guard.reset()

    def seed(self, key="old", age=40):
        with Store().connection() as db:
            db.execute(
                "INSERT INTO dashboard_cache(key,value,sampled,expires) VALUES (?,?,?,?)",
                (key, '{"value":7}', time.time() - age, time.time() - 10),
            )

    def test_pool_exhaustion_does_not_borrow_native_connections(self):
        self.set_policy(connections=1)
        native = metadata.engine()
        self.assertIsNot(protection.engine(), native)
        with protection.engine().connect():
            started = time.monotonic()
            with self.assertRaises(protection.Deferred):
                with protection.connection():
                    self.fail("The dedicated pool must have no overflow")
            self.assertLess(time.monotonic() - started, 0.3)
            with native.connect() as db:
                self.assertEqual(db.execute(sa.text("SELECT 1")).scalar(), 1)
        self.assertEqual(protection.engine().pool.size(), 1)

    def test_query_cancels_and_native_transaction_still_succeeds(self):
        self.set_policy(query_ms=100)
        started = time.monotonic()
        with self.assertRaises(protection.Deferred):
            with protection.connection(readonly=True) as db:
                db.execute(sa.text("SELECT pg_sleep(0.5)"))
        self.assertLess(time.monotonic() - started, 0.7)
        with metadata.engine().begin() as db:
            self.assertEqual(db.execute(sa.text("SELECT 42")).scalar(), 42)
        self.assertEqual(protection.guard.blocked(), "database_wait")

    def test_session_limits_do_not_change_airflow_pool_defaults(self):
        with metadata.engine().connect() as db:
            before = db.execute(sa.text("SHOW statement_timeout")).scalar()
        with protection.connection(readonly=True) as db:
            self.assertEqual(
                db.execute(sa.text("SHOW transaction_read_only")).scalar(), "on"
            )
            self.assertEqual(
                db.execute(sa.text("SHOW max_parallel_workers_per_gather")).scalar(),
                "0",
            )
            self.assertEqual(db.execute(sa.text("SHOW lock_timeout")).scalar(), "100ms")
        with metadata.engine().connect() as db:
            self.assertEqual(
                db.execute(sa.text("SHOW statement_timeout")).scalar(), before
            )

    def test_busy_slot_returns_stale_without_polling_or_recomputing(self):
        self.seed()
        with Store().connection() as db:
            db.execute(
                "INSERT INTO leases VALUES ('dashboard-aggregate:0','busy',?)",
                (time.time() + 10,),
            )
        compute = Mock(side_effect=AssertionError("must not compute"))
        with patch.object(
            protection, "database_pressure", return_value=None
        ), patch.object(cache, "probe", wraps=cache.probe) as probe:
            value, info = asyncio.run(cache.get("old", compute))
            self.assertEqual(value, {"value": 7})
            self.assertTrue(info["stale"])
            self.assertEqual(info["deferred_reason"], "aggregate_busy")
            self.assertEqual(probe.call_count, 1)
        compute.assert_not_called()

    def test_new_scope_or_too_old_result_never_reuses_another_scope(self):
        self.seed(age=PRODUCTION.stale_seconds + 5)
        with Store().connection() as db:
            db.execute(
                "INSERT INTO leases VALUES ('dashboard-aggregate:0','busy',?)",
                (time.time() + 10,),
            )
        for key in ("old", "different-permissions"):
            with patch.object(
                protection, "database_pressure", return_value=None
            ), self.assertRaises(protection.Deferred):
                asyncio.run(cache.get(key, Mock()))

    def test_distinct_cold_requests_are_shed_and_only_one_computes(self):
        calls = []

        def compute():
            calls.append(1)
            time.sleep(0.1)
            return {"value": 1}

        async def run():
            return await asyncio.gather(
                *(cache.get(str(i), compute) for i in range(60)), return_exceptions=True
            )

        with patch.object(protection, "database_pressure", return_value=None):
            result = asyncio.run(run())
        self.assertEqual(len(calls), 1)
        self.assertTrue(
            all(isinstance(r, (tuple, protection.Deferred)) for r in result)
        )
        self.assertGreaterEqual(
            sum(isinstance(r, protection.Deferred) for r in result), 59
        )

    def test_shared_refresh_budget_spans_distinct_keys(self):
        self.set_policy(compute_gap=10)
        with patch.object(protection, "database_pressure", return_value=None):
            first = cache.probe("one", "a")
            self.assertEqual(first[0], "compute")
            cache.finish("one", "a", first[3], {"v": 1}, first[2])
            self.assertEqual(cache.probe("two", "b")[3], "aggregate_budget")
            self.assertEqual(cache.probe("one", "c")[0], "hit")

    def test_real_database_activity_preempts_dashboard(self):
        self.set_policy(db_active=1)
        entered = threading.Event()

        def pipeline():
            with metadata.engine().begin() as db:
                entered.set()
                db.execute(sa.text("SELECT pg_sleep(0.6)"))
                return db.execute(sa.text("SELECT 9")).scalar()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(pipeline)
            self.assertTrue(entered.wait(1))
            time.sleep(0.03)
            compute = Mock(side_effect=AssertionError("pipeline has priority"))
            with self.assertRaises(protection.Deferred):
                asyncio.run(cache.get("cold", compute))
            self.assertEqual(protection.guard.blocked(), "database_active")
            self.assertEqual(future.result(timeout=2), 9)
            compute.assert_not_called()

    def test_recovery_requires_two_healthy_samples(self):
        protection.guard.trip("database_active")
        protection.guard.until = 0
        result = Mock()
        result.mappings.return_value.one.return_value = dict(
            connections=2, active=0, waiting=0, maximum=100
        )
        db = Mock()
        db.execute.return_value = result
        self.assertEqual(protection.database_pressure(db), "database_active")
        self.assertEqual(protection.database_pressure(db), "database_active")
        self.assertEqual(db.execute.call_count, 1)
        protection.guard.probe_due = 0
        self.assertIsNone(protection.database_pressure(db))
        self.assertIsNone(protection.guard.blocked())

    def test_admission_precedes_application_and_preserves_native_routes(self):
        app = FastAPI()
        seen = []

        @app.get("/{path:path}")
        async def route(path):
            seen.append(path)
            return {"ok": True}

        app.add_middleware(protection.Middleware)
        protection.guard.trip("database_latency")
        with TestClient(app) as client:
            denied = client.get("/api/work/summary")
            self.assertEqual(denied.status_code, 503)
            self.assertEqual(denied.headers["retry-after"], str(PRODUCTION.cooldown))
            for path in (
                "execution/heartbeat",
                "api/lab/runs",
                "executor-state/jobs",
                "api/work/retry",
                "api/work/dag-state",
            ):
                self.assertEqual(client.get("/" + path).status_code, 200)
        self.assertNotIn("api/work/summary", seen)

    def test_api_lag_opens_circuit(self):
        self.set_policy(lag_ms=30)

        async def run():
            task = asyncio.create_task(protection._watch_loop())
            try:
                await asyncio.sleep(0)
                time.sleep(0.35)
                await asyncio.sleep(0.02)
                self.assertEqual(protection.guard.blocked(), "api_latency")
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        asyncio.run(run())

    def test_slow_database_probe_never_locks_the_api_admission_loop(self):
        entered, release = threading.Event(), threading.Event()
        db = Mock()

        def slow(*args):
            entered.set()
            release.wait(2)
            result = Mock()
            result.mappings.return_value.one.return_value = dict(
                connections=2, active=0, waiting=0, maximum=100
            )
            return result

        db.execute.side_effect = slow
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(protection.database_pressure, db)
            try:
                self.assertTrue(entered.wait(1))
                started = time.monotonic()
                protection.guard.enter()
                protection.guard.leave()
                self.assertLess(time.monotonic() - started, 0.05)
            finally:
                release.set()
                future.result(timeout=2)

    def test_cancelled_request_does_not_release_running_thread(self):
        release = threading.Event()
        started = [threading.Event() for _ in range(4)]

        def job(i):
            started[i].set()
            release.wait(2)

        async def run():
            tasks = [asyncio.create_task(protection.run(job, i)) for i in range(4)]
            try:
                for _ in range(100):
                    if all(e.is_set() for e in started):
                        break
                    await asyncio.sleep(0.01)
                self.assertTrue(all(e.is_set() for e in started))
                tasks[0].cancel()
                await asyncio.gather(tasks[0], return_exceptions=True)
                with self.assertRaises(protection.Deferred):
                    await protection.run(lambda: None)
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(run())

    def test_independent_sql_uses_same_budget_and_authorization(self):
        spec = studio.Board(
            follow_work_scope=False,
            panels=[
                studio.Panel(
                    queries=[studio.Query(datasource="pg_test", sql="SELECT 1")]
                )
            ],
        )
        with patch.object(
            performance, "scope", return_value=({"allowed"}, "test")
        ), patch(
            "airflow_workbench.shared.identity.owner_id", return_value="alice"
        ), patch.object(
            studio.postgres, "configured", return_value=[]
        ), patch.object(
            studio.postgres,
            "execute",
            return_value={"rows": [{"v": 1}], "columns": ["v"]},
        ) as sql, patch.object(
            protection, "database_pressure", return_value=None
        ):
            result = asyncio.run(
                performance.board(spec, Mock(), object(), allow_postgres=True)
            )
            self.assertEqual(result[spec.panels[0].id][0]["rows"], [{"v": 1}])
            protection.guard.trip("database_active")
            with self.assertRaises(protection.Deferred):
                asyncio.run(
                    performance.board(spec, Mock(), object(), allow_postgres=True)
                )
            self.assertEqual(sql.call_count, 1)


if __name__ == "__main__":
    unittest.main()
