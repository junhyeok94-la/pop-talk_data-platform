"""간결한 DAG 그래프에서도 동일 run의 고정 입력과 mapped 항목을 그대로 전달한다."""
import unittest
from unittest.mock import MagicMock, patch

from airflow.dag_processing.dagbag import DagBag


class InputHandoffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dags = DagBag("/opt/airflow/dags").dags

    def test_dw_stage_reads_fixed_prepare_input(self):
        function = self.dags["pop_talk_dw_serving"].get_task("dw.models.dw.dim_movie_run").python_callable
        ti = MagicMock()
        ti.xcom_pull.return_value = {"token": "fixed-run"}
        with patch.dict(function.__globals__, get_current_context=lambda: {"ti": ti}), \
             patch("pipelines.orchestration.dw_pipeline.run_dbt") as run:
            function("dim_movie")
        ti.xcom_pull.assert_called_once_with(task_ids="prepare")
        run.assert_called_once_with({"token": "fixed-run"}, "dim_movie")

    def test_raw_details_reuse_plan_without_direct_graph_edge(self):
        for dag_id, factory in (("pop_talk_movie_raw_daily", "_collector"), ("pop_talk_movie_raw_initial", "_runtime")):
            function = self.dags[dag_id].get_task("collect_details").python_callable
            ti, collector, make_collector = MagicMock(), MagicMock(), MagicMock()
            ti.xcom_pull.return_value = {"run_id": "fixed-run"}
            make_collector.return_value = collector
            with self.subTest(dag=dag_id), patch.dict(function.__globals__,
                    {"get_current_context": lambda: {"ti": ti}, factory: make_collector}):
                function("completed-manifest")
            make_collector.assert_called_once_with({"run_id": "fixed-run"})
            collector.details.assert_called_once_with()

    def test_deployment_preserves_multiple_ready_inputs_in_mapping_order(self):
        function = self.dags["pop_talk_movie_databricks_daily"].get_task("deploy_notebook").python_callable
        ti, runtime = MagicMock(), MagicMock()
        ready = [{"raw_run_id": "one"}, {"raw_run_id": "two"}]
        ti.xcom_pull.return_value = ready
        runtime.deploy_notebook.return_value = {"notebook_path": "/fixed"}
        with patch.dict(function.__globals__, {
            "get_current_context": lambda: {"ti": ti},
            "_runtime": lambda: ("/project", runtime),
            "_databricks_credentials": lambda: ("host", "token"),
        }):
            result = function()
        self.assertEqual(result, [{"deployed": {"notebook_path": "/fixed"}, "ready_input": item} for item in ready])
        ti.xcom_push.assert_called_once_with(key="notebook", value={"notebook_path": "/fixed"})

    def test_confirmation_reads_all_mapped_stages(self):
        function = self.dags["pop_talk_movie_databricks_daily"].get_task("confirm_loaded_batch").python_callable
        ti = MagicMock()
        ti.xcom_pull.side_effect = [[{"raw_run_id": "one"}], iter([{"stage": "one"}])]
        loaded = [{"loaded": "one"}]
        with patch.dict(function.__globals__, get_current_context=lambda: {"ti": ti, "params": {"publication_revision": 2}}), \
             patch("pipelines.orchestration.asset_contract.confirm_loaded_batch") as confirm:
            function(loaded)
        confirm.assert_called_once_with(ready_inputs=[{"raw_run_id": "one"}],
            staged_results=[{"stage": "one"}], loaded_results=loaded, publication_revision=2)
        ti.xcom_pull.assert_any_call(task_ids="stage_bundle", map_indexes=[0])


if __name__ == "__main__":
    unittest.main()
