"""SQL sandbox, 입력 계약, 보드 충돌, RBAC 및 Model Lab DAG 계약 회귀 검사."""

import asyncio
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, "/opt/airflow/plugins")
from fastapi import HTTPException
from pydantic import ValidationError
from starlette.requests import Request
from airflow_workbench import boards, query, postgres
from airflow_workbench.app import access
from airflow_workbench.mlops import validate_dag_metadata
from airflow_workbench.store import Conflict


class QueryTest(unittest.TestCase):
    def setUp(self):
        from workbench_test_db import isolated_database

        isolated_database(self)

    def test_retired_sqlite_executor_cannot_run_even_a_select(self):
        with self.assertRaises(query.QueryError):
            query.execute_sql("SELECT 1", {}, {})

    def test_board_categories_persist_and_detect_conflict(self):
        values = boards.list_boards()
        self.assertEqual(len({b["category"] for b in values}), 2)
        b = boards.Board.model_validate(values[0])
        b.title = "SQL 운영 보드"
        saved = boards.save_board(b)
        with self.assertRaises(Conflict):
            boards.save_board(b)
        loaded = next(v for v in boards.list_boards() if v["id"] == saved["id"])
        self.assertEqual(loaded, saved)

    def test_import_contract_rejects_style_injection_and_bad_variables(self):
        for changes in [
            {"color": "red;position:fixed"},
            {"height": 100000},
            {"maximum": 0},
            {"width": 99},
        ]:
            with self.assertRaises(ValidationError):
                boards.QueryPanel(id="p", title="p", sql="SELECT 1", **changes)
        for variables in [{"from_ts": "0"}, {"x;drop": "x"}, {"x": 3}]:
            with self.assertRaises(ValidationError):
                boards.QuerySpec(sql="SELECT 1", variables=variables)

    def test_readonly_user_can_query_but_needs_origin_header(self):
        manager = Mock()
        manager.is_authorized_view.return_value = True
        manager.is_authorized_configuration.return_value = False
        manager.is_authorized_custom_view.return_value = False

        def request(headers):
            return Request(
                {
                    "type": "http",
                    "method": "POST",
                    "scheme": "http",
                    "path": "/workbench/api/query",
                    "server": ("localhost", 8080),
                    "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
                }
            )

        with patch("airflow_workbench.shared.auth.get_auth_manager", return_value=manager):
            asyncio.run(
                access(
                    request(
                        {"x-workbench-request": "1", "origin": "http://localhost:8080"}
                    ),
                    SimpleNamespace(),
                )
            )
            for h in [{}, {"x-workbench-request": "1", "origin": "https://other.test"}]:
                with self.assertRaises(HTTPException):
                    asyncio.run(access(request(h), SimpleNamespace()))

    def test_postgres_parameter_translation_preserves_literals(self):
        actual = postgres.prepare_sql(
            "SELECT count(*) AS value FROM dw_serving.publish_attempts_v3 WHERE status=:status AND status LIKE '%ok%'",
            {"status": "ok"},
        )
        self.assertIn("%(status)s", actual)
        self.assertIn("'%%ok%%'", actual)
        for sql in [
            "DELETE FROM x",
            "SELECT 1; SELECT 2",
            "SELECT pg_sleep(8)",
            "SELECT public.count(*) FROM x",
            "WITH x AS (DELETE FROM foo RETURNING *) SELECT * FROM x",
            "SELECT 1 INTO temp_t",
        ]:
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                postgres.prepare_sql(sql, {})

    def test_model_lab_contract_rejects_cpu_training_and_unbounded_runs(self):
        valid = {
            "tags": ["mlops", "model-lab", "external-executor", "contract:v2"],
            "max_active_runs": 1,
            "max_active_tasks": 1,
            "catchup": False,
            "timetable_summary": None,
            "timetable_periodic": False,
            "owners": ["mlops"],
            "dag_run_timeout": "PT6M",
        }
        tasks = [
            {
                "operator_name": "ExternalModelJobOperator",
                "pool": "model_lab_gpu",
                "pool_slots": 1,
                "retries": 0,
                "execution_timeout": 240,
            }
        ]
        self.assertEqual(validate_dag_metadata(valid, tasks), [])
        for key, value in [
            ("operator_name", "PythonOperator"),
            ("pool", "default_pool"),
            ("retries", 3),
            ("execution_timeout", None),
        ]:
            self.assertTrue(validate_dag_metadata(valid, [{**tasks[0], key: value}]))
        self.assertTrue(
            validate_dag_metadata({**valid, "timetable_periodic": True}, tasks)
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
