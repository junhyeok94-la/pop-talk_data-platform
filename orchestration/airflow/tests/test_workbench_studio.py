"""Dashboard Studio query, saved-layout, and access boundaries without network or GPU."""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, "/opt/airflow/plugins")
from pydantic import ValidationError
from starlette.requests import Request
from airflow_workbench import studio
from airflow_workbench.app import access
from airflow_workbench.store import Conflict
from workbench_test_db import isolated_database


class StudioTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        temp = tempfile.TemporaryDirectory()
        env = patch.dict(os.environ, {"AIRFLOW_WORKBENCH_DATA_DIR": temp.name})
        env.start()
        self.addCleanup(temp.cleanup)
        self.addCleanup(env.stop)
        self.rows = [
            dict(dag_id="train", state="success", run_ts=3660, duration_seconds=10),
            dict(dag_id="train", state="failed", run_ts=3690, duration_seconds=20),
            dict(dag_id="other", state="running", run_ts=7300, duration_seconds=None),
        ]

    def build(self, **kw):
        return studio.build_result(
            studio.Builder(**kw),
            {"dag_runs": self.rows},
            studio.Context(variables={"dag": "train"}),
        )

    def test_builder_filters_variables_and_completed_success_rate(self):
        result = self.build(
            filters=[
                dict(field="dag_id", op="contains", value=":dag"),
                dict(field="state", op="in", value="success,failed"),
            ],
            measures=[dict(op="percent", field="state")],
        )
        self.assertEqual(result["rows"], [{"value": 50.0}])

    def test_group_multiple_measures_and_alias_projection(self):
        result = self.build(
            group_by=["dag_id"],
            measures=[
                dict(op="count", alias="n"),
                dict(op="avg", field="duration_seconds", alias="seconds"),
            ],
            columns=["dag_id", "seconds"],
            sort="n",
            descending=True,
        )
        self.assertEqual(
            result["rows"],
            [dict(dag_id="train", seconds=15), dict(dag_id="other", seconds=None)],
        )

    def test_time_buckets_utc_and_sorted(self):
        r = self.build(
            group_by=["run_ts", "state"],
            bucket="run_ts",
            measures=[dict(op="count")],
            sort="run_ts",
        )
        self.assertEqual(r["rows"][0]["run_ts"], "1970-01-01T01:00:00+00:00")
        self.assertEqual(r["rows"][-1]["run_ts"], "1970-01-01T02:00:00+00:00")

    def test_empty_aggregate_nulls_and_limit_visible(self):
        self.assertEqual(
            self.build(measures=[dict(op="p95", field="duration_seconds")])["rows"],
            [{"value": 20}],
        )
        r = self.build(
            filters=[dict(field="state", value="missing")],
            measures=[dict(op="avg", field="duration_seconds")],
        )
        self.assertEqual(r["rows"], [dict(value=None)])
        self.assertTrue(self.build(limit=1)["limited"])
        self.assertEqual(
            self.build(sort="duration_seconds", descending=True)["rows"][-1]["state"],
            "running",
        )

    def test_invalid_fields_aliases_and_variable_rejected(self):
        for args in [
            dict(group_by=["conf"]),
            dict(columns=["password"]),
            dict(filters=[dict(field="state", value=":missing")]),
            dict(group_by=["state"], measures=[dict(alias="state")]),
            dict(bucket="run_ts"),
        ]:
            with self.assertRaises(ValueError):
                self.build(**args)

    def test_no_memory_sql_and_one_snapshot_per_board_source(self):
        fetch = AsyncMock(
            return_value={
                "total_entries": 1,
                "dag_runs": [
                    dict(
                        dag_id="one",
                        dag_run_id="r",
                        state="success",
                        run_after="2026-09-10T00:00:00Z",
                        start_date="2026-09-10T00:00:00Z",
                        end_date="2026-09-10T00:00:10Z",
                    )
                ],
            }
        )
        b = studio.initial_boards()[0]
        with patch(
            "airflow_workbench.query.execute_sql",
            side_effect=AssertionError("Legacy SQL called"),
        ):
            result = asyncio.run(studio.run_board(b, fetch))
        self.assertEqual(fetch.await_count, 1)
        self.assertEqual(len(result), 7)
        self.assertFalse(
            any(r.get("error") for frames in result.values() for r in frames)
        )

    def test_multiquery_error_is_visible_and_other_query_survives(self):
        b = studio.Board(
            panels=[
                studio.Panel(
                    queries=[
                        studio.Query(),
                        studio.Query(ref="B", datasource="missing"),
                    ]
                )
            ]
        )
        fetch = AsyncMock(return_value={"dag_runs": [], "total_entries": 0})
        result = asyncio.run(studio.run_board(b, fetch))[b.panels[0].id]
        self.assertNotIn("error", result[0])
        self.assertIn("error", result[1])

    def test_postgres_does_not_inherit_viewer_access(self):
        q = studio.Query(datasource="pg_airflow", sql="SELECT 1")
        with patch("airflow_workbench.postgres.execute") as execute:
            with self.assertRaisesRegex(ValueError, "권한"):
                asyncio.run(studio.run_query(q, studio.Context(), AsyncMock()))
            execute.assert_not_called()

    def test_timed_out_source_is_not_retried_for_every_panel(self):
        board = studio.initial_boards()[0]
        with patch(
            "airflow_workbench.studio.records", AsyncMock(side_effect=TimeoutError)
        ) as read:
            result = asyncio.run(studio.run_board(board, AsyncMock()))
        self.assertEqual(read.await_count, 1)
        self.assertTrue(all("15초" in frames[0]["error"] for frames in result.values()))

    def test_layout_version_conflict_and_restore(self):
        original = studio.list_boards("tester")[0]
        b = studio.Board.model_validate(original)
        b.panels[0].x = 2
        b.panels[0].w = 8
        b.panels[0].h = 9
        saved = studio.save_board("tester", b)
        self.assertEqual(saved["version"], 2)
        with self.assertRaises(Conflict):
            studio.save_board("tester", b)
        history = studio.revisions("tester", b.id)
        self.assertEqual(history[0], original)
        old = studio.Board.model_validate({**history[0], "version": 2})
        restored = studio.save_board("tester", old)
        self.assertEqual(restored["version"], 3)
        self.assertEqual(restored["panels"], original["panels"])

    def test_layout_and_advanced_contract(self):
        for args in [
            dict(x=10, w=6),
            dict(queries=[dict(ref="A"), dict(ref="A")]),
            dict(visual={"advanced": {"tooltip": {"formatter": "<script>"}}}),
        ]:
            with self.assertRaises(ValidationError):
                studio.Panel(**args)
        with self.assertRaises(ValidationError):
            studio.Context(from_ts=100, to_ts=90)
        with self.assertRaises(ValidationError):
            studio.Context(variables={"from_ts": "bad"})

    def test_api_builder_uses_read_permission_with_csrf_protection(self):
        manager = Mock()
        manager.is_authorized_view.return_value = True
        manager.is_authorized_configuration.return_value = False
        manager.is_authorized_custom_view.return_value = False
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/workbench/api/studio/query",
                "scheme": "http",
                "server": ("localhost", 8080),
                "headers": [
                    (b"x-workbench-request", b"1"),
                    (b"origin", b"http://localhost:8080"),
                ],
            }
        )
        with patch("airflow_workbench.shared.auth.get_auth_manager", return_value=manager):
            asyncio.run(access(request, user=object()))
        manager.is_authorized_view.assert_called_once()


if __name__ == "__main__":
    unittest.main()
