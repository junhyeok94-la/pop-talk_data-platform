"""Real PostgreSQL: aggregate correctness, permission isolation and cache concurrency."""

import asyncio
import json
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch
from types import SimpleNamespace

import sqlalchemy as sa

from airflow_workbench.dashboard import aggregate, cache, performance, studio, work
from airflow_workbench.dashboard import protection
from airflow_workbench.shared import metadata
from airflow_workbench.shared.database import Store
from workbench_test_db import isolated_database


class PerformanceTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        self.engine = metadata.engine()
        engine_patch = patch.object(protection, "engine", return_value=self.engine)
        engine_patch.start()
        self.addCleanup(engine_patch.stop)
        pressure_patch = patch.object(
            protection, "database_pressure", return_value=None
        )
        pressure_patch.start()
        self.addCleanup(pressure_patch.stop)
        self.table = sa.Table(
            "fixture_runs",
            sa.MetaData(),
            sa.Column("dag_id", sa.Text),
            sa.Column("run_id", sa.Text),
            sa.Column("state", sa.Text),
            sa.Column("run_after", sa.DateTime(timezone=True)),
            sa.Column("start_date", sa.DateTime(timezone=True)),
            sa.Column("end_date", sa.DateTime(timezone=True)),
            schema=Store().schema,
        )
        self.table.create(self.engine)
        self.context = studio.Context(
            from_ts=1, to_ts=10000, bucket_seconds=3600, variables={"dag": "train"}
        )
        self.records = [
            dict(
                dag_id="train",
                run_id="a",
                state="success",
                run_ts=3660,
                start_ts=3660,
                end_ts=3670,
                duration_seconds=10,
                environment_id="gpu",
                workload="training",
            ),
            dict(
                dag_id="train",
                run_id="b",
                state="failed",
                run_ts=3690,
                start_ts=3690,
                end_ts=3710,
                duration_seconds=20,
                environment_id="gpu",
                workload="training",
            ),
            dict(
                dag_id="other",
                run_id="c",
                state="running",
                run_ts=7300,
                start_ts=7300,
                end_ts=None,
                duration_seconds=None,
                environment_id=None,
                workload=None,
            ),
        ]

        def stamp(v):
            return datetime.fromtimestamp(v, timezone.utc) if v is not None else None

        with self.engine.begin() as db:
            db.execute(
                self.table.insert(),
                [
                    dict(
                        dag_id=r["dag_id"],
                        run_id=r["run_id"],
                        state=r["state"],
                        run_after=stamp(r["run_ts"]),
                        start_date=stamp(r["start_ts"]),
                        end_date=stamp(r["end_ts"]),
                    )
                    for r in self.records
                ],
            )

    def execute(self, builders, allowed=None):
        return aggregate.execute(
            {aggregate.identity(b): b for b in builders},
            self.context,
            allowed if allowed is not None else {"train", "other"},
            table=self.table,
            engine=self.engine,
            environments={"train": ("gpu", "training")},
        )

    def test_sql_matches_python_builder_for_all_aggregates_filters_and_buckets(self):
        specs = [
            {},
            {
                "columns": ["dag_id", "duration_seconds"],
                "sort": "duration_seconds",
                "descending": True,
                "limit": 2,
            },
            {"group_by": ["state"]},
            {
                "group_by": ["run_ts", "state"],
                "bucket": "run_ts",
                "measures": [{"op": "count"}],
                "sort": "run_ts",
            },
            {
                "filters": [{"field": "dag_id", "op": "contains", "value": ":dag"}],
                "measures": [{"op": "percent", "field": "state"}],
            },
            {
                "filters": [{"field": "state", "op": "in", "value": "success,failed"}],
                "measures": [{"op": "count"}],
            },
            {
                "filters": [{"field": "state", "op": "ne", "value": "failed"}],
                "measures": [{"op": "count"}],
            },
            {
                "filters": [{"field": "duration_seconds", "op": "gte", "value": "11"}],
                "measures": [{"op": "count"}],
            },
            {
                "filters": [{"field": "duration_seconds", "op": "lte", "value": "11"}],
                "measures": [{"op": "count"}],
            },
            {
                "filters": [{"field": "duration_seconds", "op": "not_null"}],
                "measures": [{"op": "count"}],
            },
            {
                "filters": [{"field": "state", "value": "missing"}],
                "measures": [{"op": "avg", "field": "duration_seconds"}],
            },
            {
                "group_by": ["environment_id"],
                "measures": [{"op": "count"}],
                "sort": "environment_id",
            },
        ] + [
            {"measures": [{"op": op, "field": "duration_seconds"}]}
            for op in ("sum", "avg", "min", "max", "p95")
        ]
        builders = [studio.Builder(**s) for s in specs]
        actual = self.execute(builders)
        for b in builders:
            with self.subTest(spec=b.model_dump()):
                expected = studio.build_result(
                    b, {"dag_runs": self.records}, self.context
                )
                result = actual[aggregate.identity(b)]
                self.assertEqual(result["columns"], expected["columns"])
                self.assertEqual(result["limited"], expected["limited"])
                self.assertEqual(
                    sorted(result["rows"], key=lambda x: json.dumps(x, sort_keys=True)),
                    sorted(
                        expected["rows"], key=lambda x: json.dumps(x, sort_keys=True)
                    ),
                )

    def test_scope_filters_before_count_and_empty_scope_leaks_nothing(self):
        b = studio.Builder(measures=[dict(op="count")])
        self.assertEqual(
            self.execute([b], {"train"})[aggregate.identity(b)]["rows"], [{"value": 2}]
        )
        self.assertEqual(
            self.execute([b], set())[aggregate.identity(b)]["rows"], [{"value": 0}]
        )

    def test_over_5000_records_are_aggregated_without_sampling(self):
        with self.engine.begin() as db:
            db.execute(
                sa.text(
                    f"INSERT INTO \"{Store().schema}\".fixture_runs SELECT 'train','bulk-'||n,'success',to_timestamp(4000),to_timestamp(4000),to_timestamp(4001) FROM generate_series(1,6000) n"
                )
            )
        b = studio.Builder(measures=[dict(op="count")])
        r = self.execute([b], {"train"})[aggregate.identity(b)]
        self.assertEqual(r["rows"], [{"value": 6002}])
        self.assertFalse(r["truncated"])

    def test_filter_values_are_bound_and_invalid_columns_rejected(self):
        b = studio.Builder(
            filters=[dict(field="dag_id", value="train' OR 1=1 --")],
            measures=[dict(op="count")],
        )
        self.assertEqual(
            self.execute([b])[aggregate.identity(b)]["rows"], [{"value": 0}]
        )
        with self.assertRaises(ValueError):
            self.execute([studio.Builder(columns=["conf"])])

    def test_one_materialized_source_query_for_many_panels(self):
        b = [
            studio.Builder(measures=[dict(op="count")]),
            studio.Builder(group_by=["state"], measures=[dict(op="count")]),
        ]
        statements = []

        def before(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("WITH wb_source"):
                statements.append(statement)

        sa.event.listen(self.engine, "before_cursor_execute", before)
        try:
            self.execute(b)
        finally:
            sa.event.remove(self.engine, "before_cursor_execute", before)
        self.assertEqual(len(statements), 1)
        self.assertIn("AS MATERIALIZED", statements[0])

    def test_personal_work_includes_old_active_but_not_old_failures_or_other_dags(self):
        def stamp(seconds):
            return datetime.fromtimestamp(seconds, timezone.utc)

        extra = [
            {
                "dag_id": "train",
                "run_id": "old-active",
                "state": "running",
                "run_after": stamp(10),
                "start_date": stamp(10),
                "end_date": None,
            },
            {
                "dag_id": "train",
                "run_id": "old-failed",
                "state": "failed",
                "run_after": stamp(10),
                "start_date": stamp(10),
                "end_date": stamp(20),
            },
            {
                "dag_id": "secret",
                "run_id": "hidden",
                "state": "failed",
                "run_after": stamp(3990),
                "start_date": stamp(3990),
                "end_date": stamp(3995),
            },
            {
                "dag_id": "train",
                "run_id": "waiting",
                "state": "queued",
                "run_after": stamp(3900),
                "start_date": None,
                "end_date": None,
            },
        ]
        with self.engine.begin() as db:
            db.execute(self.table.insert(), extra)
        statements = []

        def before(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("WITH work_runs"):
                statements.append(statement)

        sa.event.listen(self.engine, "before_cursor_execute", before)
        try:
            with patch.object(work, "DagRun", SimpleNamespace(__table__=self.table)):
                result = work.summary(
                    [{"dag_id": "train"}],
                    work.Profile(hours=1, long_running_minutes=5),
                    4000,
                )
        finally:
            sa.event.remove(self.engine, "before_cursor_execute", before)
        self.assertEqual(
            result["counts"], {"success": 1, "failed": 1, "running": 1, "queued": 1}
        )
        self.assertEqual(
            {r["run_id"] for r in result["attention"]}, {"b", "old-active"}
        )
        self.assertEqual(result["dags"][0]["latest"]["run_id"], "waiting")
        self.assertIsNotNone(result["dags"][0]["last_success"])
        self.assertEqual(len(statements), 1)
        self.assertIn("AS MATERIALIZED", statements[0])

    def test_cross_process_claim_and_abandoned_lease_recovery(self):
        # Independent connections exercise the shared DB protocol, not the local task map.
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            claims = list(
                pool.map(lambda i: cache.probe("distributed", str(i)), range(6))
            )
        self.assertEqual(sum(r[0] == "compute" for r in claims), 1)
        with Store().connection() as db:
            db.execute(
                "UPDATE dashboard_cache SET lease_until=0 WHERE key='distributed'"
            )
            db.execute(
                "UPDATE leases SET expires=0 WHERE name LIKE 'dashboard-aggregate:%'"
            )
        retry = cache.probe("distributed", "recovery")
        self.assertEqual(retry[0], "compute")
        cache.finish("distributed", "recovery", retry[3], {"recovered": True}, retry[2])
        # An old compute cannot replace the newer lease owner's result.
        cache.finish(
            "distributed", "0", claims[0][3], {"recovered": False}, claims[0][2] or 0
        )
        self.assertEqual(cache.probe("distributed", "reader")[1], {"recovered": True})

    def test_cache_storage_and_payload_budgets(self):
        with patch.object(cache, "MAX_ENTRIES", 2):
            for key in ("a", "b", "c"):
                r = cache.probe(key, "owner")
                cache.finish(key, "owner", r[3], {"key": key}, r[2])
            with Store().connection() as db:
                self.assertEqual(
                    db.execute("SELECT count(*) FROM dashboard_cache").fetchone()[0], 2
                )
        r = cache.probe("oversize", "owner")
        with patch.object(cache, "MAX_BYTES", 8), self.assertRaisesRegex(
            ValueError, "2MB"
        ):
            cache.finish("oversize", "owner", r[3], {"large": "payload"}, r[2])
        self.assertEqual(cache.probe("oversize", "retry")[0], "compute")

    def test_concurrent_cache_misses_compute_once_and_then_hit(self):
        calls = []

        def compute():
            calls.append(1)
            time.sleep(0.05)
            return {"value": 123}

        async def run():
            results = await asyncio.gather(
                *[cache.get("same", compute) for _ in range(20)]
            )
            self.assertEqual(sum(not meta["cache_hit"] for _, meta in results), 1)
            self.assertEqual((await cache.get("same", compute))[0], {"value": 123})

        asyncio.run(run())
        self.assertEqual(len(calls), 1)

    def test_expiry_failure_and_slot_budget(self):
        action, _, sampled, slot = cache.probe("first", "a")
        self.assertEqual(action, "compute")
        self.assertEqual(cache.probe("first", "b")[0], "wait")
        other = cache.probe("second", "b")
        self.assertEqual(other[0], "wait")
        self.assertEqual(cache.probe("third", "c")[0], "wait")
        cache.finish("first", "a", slot, {"n": 1}, sampled)
        self.assertEqual(cache.probe("first", "z")[0], "hit")
        with Store().connection() as db:
            db.execute("UPDATE dashboard_cache SET expires=0 WHERE key='first'")
        third = cache.probe("first", "z")
        self.assertEqual(third[0], "compute")
        cache.finish("first", "z", third[3])
        self.assertEqual(cache.probe("first", "new")[0], "compute")

    def test_permission_change_is_checked_before_cache_lookup(self):
        manager = Mock()
        manager.is_authorized_dag.return_value = True
        manager.get_authorized_dag_ids.return_value = {"train"}
        spec = studio.Board(
            follow_work_scope=False,
            from_ts=1,
            to_ts=10000,
            panels=[
                studio.Panel(
                    queries=[
                        studio.Query(
                            builder=studio.Builder(measures=[dict(op="count")])
                        )
                    ]
                )
            ],
        )
        original = aggregate.execute

        def compute(builders, context, allowed):
            return original(
                builders,
                context,
                allowed,
                table=self.table,
                engine=self.engine,
                environments={},
            )

        async def run():
            a = await performance.board(spec, AsyncMock(), object())
            manager.get_authorized_dag_ids.return_value = set()
            b = await performance.board(spec, AsyncMock(), object())
            self.assertEqual(a[spec.panels[0].id][0]["rows"], [{"value": 2}])
            self.assertEqual(b[spec.panels[0].id][0]["rows"], [{"value": 0}])
            manager.is_authorized_dag.return_value = False
            with self.assertRaisesRegex(Exception, "권한"):
                await performance.board(spec, AsyncMock(), object())

        with patch(
            "airflow_workbench.shared.auth.get_auth_manager", return_value=manager
        ), patch.object(aggregate, "execute", side_effect=compute):
            asyncio.run(run())

    def test_shared_workspace_matches_work_counts_and_blocks_unscoped_sources(self):
        self.install_scope_fixture()
        profile = work.Profile(
            dag_ids=["train", "other"],
            hours=1,
            search="etl trainer",
            paused="paused",
            run_state="failed",
        )
        work.save_profile("scope-user", profile, {"train", "other"})
        q = studio.Query(builder=studio.Builder(measures=[dict(op="count")]))
        panel = studio.Panel(
            queries=[q, studio.Query(ref="B", datasource="pg_airflow", sql="SELECT 1")]
        )
        spec = studio.Board(hours=720, from_ts=1, to_ts=10000, panels=[panel])
        execute = aggregate.execute

        def compute(builders, context, allowed):
            self.assertEqual(allowed, {"train"})
            self.assertEqual(context.hours, 1)
            self.assertEqual(context.run_state, "failed")
            return execute(
                builders,
                context,
                allowed,
                table=self.table,
                engine=self.engine,
                environments={},
            )

        async def run():
            summary = await work.work_summary(object())
            result = await performance.board(
                spec, AsyncMock(), object(), allow_postgres=True
            )
            self.assertEqual(summary["counts"]["failed"], 1)
            self.assertIsNotNone(summary["dags"][0]["last_success"])
            self.assertEqual(result[panel.id][0]["rows"], [{"value": 1}])
            self.assertEqual(
                summary["profile"]["version"],
                result[panel.id][0]["work_scope"]["version"],
            )
            self.assertIn("별도 분석", result[panel.id][1]["error"])

        with patch.object(work, "owner_id", return_value="scope-user"), patch.object(
            performance, "scope", return_value=({"train", "other"}, "test")
        ), patch.object(aggregate, "execute", side_effect=compute), patch.object(
            studio.postgres, "execute"
        ) as direct, patch(
            "time.time", return_value=4000
        ):
            asyncio.run(run())
            direct.assert_not_called()

    def install_scope_fixture(self):
        dags = sa.Table(
            "fixture_dags",
            sa.MetaData(),
            sa.Column("dag_id", sa.Text),
            sa.Column("dag_display_name", sa.Text),
            sa.Column("owners", sa.Text),
            sa.Column("is_paused", sa.Boolean),
            schema=Store().schema,
        )
        tags = sa.Table(
            "fixture_tags",
            sa.MetaData(),
            sa.Column("dag_id", sa.Text),
            sa.Column("name", sa.Text),
            schema=Store().schema,
        )
        dags.create(self.engine)
        tags.create(self.engine)
        with self.engine.begin() as db:
            db.execute(
                dags.insert(),
                [
                    dict(
                        dag_id="train",
                        dag_display_name="Trainer",
                        owners="alice",
                        is_paused=True,
                    ),
                    dict(
                        dag_id="other",
                        dag_display_name="Other",
                        owners="bob",
                        is_paused=False,
                    ),
                    dict(
                        dag_id="secret",
                        dag_display_name="Hidden",
                        owners="alice",
                        is_paused=False,
                    ),
                ],
            )
            db.execute(
                tags.insert(),
                [dict(dag_id=d, name="team:etl") for d in ("train", "other", "secret")],
            )
        for name, table in [
            ("DagModel", dags),
            ("DagTag", tags),
            ("DagRun", self.table),
        ]:
            mock = patch.object(work, name, SimpleNamespace(__table__=table))
            mock.start()
            self.addCleanup(mock.stop)

    def test_saved_tags_search_and_operational_state_never_expand_dag_permissions(self):
        self.install_scope_fixture()
        profile = work.Profile(tags=["team:etl"], search="etl ALICE", paused="paused")
        selected = work.selected({"train", "other"}, profile)
        self.assertEqual({d["dag_id"] for d in selected}, {"train", "other"})
        self.assertEqual(
            [d["dag_id"] for d in work.narrow(selected, profile)], ["train"]
        )
        self.assertEqual(
            work.narrow(selected, profile.model_copy(update={"search": "no-match"})), []
        )
        self.assertEqual(work.selected(set(), profile), [])
        self.assertEqual(work.selected({"train", "other"}, work.Profile()), [])

    def test_shared_scope_change_invalidates_cache_and_empty_selection_stays_empty(
        self,
    ):
        self.install_scope_fixture()
        profile = work.save_profile(
            "scope-user", work.Profile(dag_ids=["train"], hours=1), {"train"}
        )
        spec = studio.Board(
            panels=[
                studio.Panel(
                    queries=[
                        studio.Query(
                            builder=studio.Builder(measures=[dict(op="count")])
                        )
                    ]
                )
            ]
        )
        execute = aggregate.execute

        def compute(builders, context, allowed):
            return execute(
                builders,
                context,
                allowed,
                table=self.table,
                engine=self.engine,
                environments={},
            )

        async def run():
            result = await performance.board(spec, AsyncMock(), object())
            self.assertEqual(result[spec.panels[0].id][0]["rows"], [{"value": 2}])
            work.save_profile(
                "scope-user",
                profile.model_copy(update={"search": "no-match"}),
                {"train"},
            )
            result = await performance.board(spec, AsyncMock(), object())
            self.assertEqual(result[spec.panels[0].id][0]["rows"], [{"value": 0}])

        with patch.object(work, "owner_id", return_value="scope-user"), patch.object(
            performance, "scope", return_value=({"train", "other"}, "test")
        ), patch.object(aggregate, "execute", side_effect=compute), patch(
            "time.time", return_value=4000
        ):
            asyncio.run(run())


if __name__ == "__main__":
    unittest.main(verbosity=2)
