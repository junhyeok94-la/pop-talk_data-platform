"""PostgreSQL environments and portable orchestration, with no external compute launch."""

import asyncio
import json
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, "/opt/airflow/plugins")
from fastapi.testclient import TestClient
from airflow.exceptions import TaskDeferred
from airflow_workbench import environments, portable_training
from airflow_workbench.app import app, access
from airflow_workbench.dag_factory import build_dags
from airflow_workbench.mlops import ModelJobTrigger, validate_dag_metadata
from airflow_workbench.schemas import TrainingRecipe
from airflow_workbench.store import Conflict
from workbench_test_db import isolated_database


class PortableTest(unittest.TestCase):
    def setUp(self):
        isolated_database(self)
        self.target = environments.save(
            environments.Environment(
                id="remote",
                name="Remote training",
                connection_id="remote_executor",
                dag_prefix="team_model_lab",
                pool="team_gpu",
            )
        )

    def recipe(self, **kw):
        return TrainingRecipe(
            base_model="example/base-model",
            environment_id="remote",
            revision="a" * 40,
            train_dataset="s3://data/train.jsonl",
            validation_dataset="s3://data/validation.jsonl",
            train_sha256="b" * 64,
            validation_sha256="c" * 64,
            artifact_uri="s3://models/run-001/",
            **kw
        )

    def test_environment_versions_and_prefix_conflicts(self):
        with self.assertRaises(Conflict):
            environments.save(self.target.model_copy(update={"version": 0}))
        with self.assertRaises(Conflict):
            environments.save(
                self.target.model_copy(update={"id": "other", "version": 0})
            )
        updated = environments.save(self.target.model_copy(update={"capacity": 2}))
        self.assertNotEqual(updated.fingerprint(), self.target.fingerprint())

    def test_dag_bundle_parsing_has_no_database_or_network_side_effect(self):
        source = environments.dag_source(self.target)
        self.assertNotIn("password", source)
        with patch(
            "airflow_workbench.metadata.engine",
            side_effect=AssertionError("DB during parsing"),
        ), patch(
            "airflow_workbench.worker_client.connection",
            side_effect=AssertionError("Secrets during parsing"),
        ):
            namespace = {}
            exec(compile(source, "environment.py", "exec"), namespace)
        for kind, dag_id in self.target.dag_ids().items():
            dag = namespace[dag_id]
            task = dag.get_task("run_external_job")
            self.assertEqual(task.environment["connection_id"], "remote_executor")
            self.assertIn("config:" + self.target.fingerprint(), dag.tags)
            self.assertEqual(task.pool, "team_gpu")
            self.assertFalse(dag.catchup)

    def test_two_environments_keep_connections_and_dags_separate(self):
        other = self.target.model_copy(
            update={
                "id": "second",
                "connection_id": "other_executor",
                "dag_prefix": "other_model_lab",
                "pool": "other_gpu",
            }
        )
        a, b = (
            build_dags(self.target.model_dump())[0],
            build_dags(other.model_dump())[0],
        )
        self.assertNotEqual(a.dag_id, b.dag_id)
        self.assertNotEqual(
            a.tasks[0].environment["connection_id"],
            b.tasks[0].environment["connection_id"],
        )

    def test_native_kubernetes_operator_uses_declared_resources(self):
        from airflow.providers.cncf.kubernetes.operators.pod import (
            KubernetesPodOperator,
        )

        target = self.target.model_copy(
            update={
                "backend": "kubernetes",
                "image": "registry.example/trainer:1.0",
                "namespace": "training",
                "cpu": "4",
                "memory": "32Gi",
                "gpu": 2,
                "workloads": ["llm_sft"],
            }
        )
        task = build_dags(target.model_dump())[0].tasks[0]
        self.assertIsInstance(task, KubernetesPodOperator)
        self.assertTrue(task.deferrable)
        self.assertEqual(
            task.container_resources.limits,
            {"cpu": "4", "memory": "32Gi", "nvidia.com/gpu": "2"},
        )
        self.assertEqual(task.kubernetes_conn_id, "remote_executor")
        self.assertEqual(task.namespace, "training")
        self.assertTrue(task.do_xcom_push)
        self.assertEqual(task.active_deadline_seconds, 21600)

    def test_remote_datasets_and_artifacts_require_immutable_manifest(self):
        self.assertEqual(
            portable_training.validate_remote_recipe(
                self.recipe().model_dump(), "llm_sft"
            ).train_sha256,
            "b" * 64,
        )
        for change in [
            {"train_sha256": ""},
            {"artifact_uri": ""},
            {"validation_sha256": "b" * 64},
            {"train_dataset": "train.jsonl"},
        ]:
            with self.assertRaises(ValueError):
                portable_training.validate_remote_recipe(
                    self.recipe().model_copy(update=change).model_dump(), "llm_sft"
                )
        for uri in [
            "http://host/data.jsonl",
            "s3://user:secret@bucket/data.jsonl",
            "s3://bucket/../data.jsonl",
        ]:
            with self.assertRaises(ValueError):
                TrainingRecipe.model_validate(
                    {**self.recipe().model_dump(), "train_dataset": uri}
                )

    def test_kubernetes_completion_requires_artifact_and_training_receipt(self):
        from airflow.providers.cncf.kubernetes.operators.pod import (
            KubernetesPodOperator,
        )
        from airflow.exceptions import AirflowException

        target = self.target.model_copy(
            update={
                "backend": "kubernetes",
                "image": "registry.example/trainer:1.0",
                "workloads": ["llm_sft"],
            }
        )
        task = build_dags(target.model_dump())[0].tasks[0]
        for result in [
            None,
            {},
            {"training_performed": False, "artifact_uri": "s3://models/a"},
        ]:
            with patch.object(
                KubernetesPodOperator, "trigger_reentry", return_value=result
            ), self.assertRaises(AirflowException):
                task.trigger_reentry({}, {})
        receipt = {"training_performed": True, "artifact_uri": "s3://models/a"}
        with patch.object(
            KubernetesPodOperator, "trigger_reentry", return_value=receipt
        ):
            self.assertEqual(task.trigger_reentry({}, {}), receipt)

    def test_activation_uses_selected_environment_dag(self):
        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "admin"
        )
        self.addCleanup(app.dependency_overrides.clear)
        native = AsyncMock(return_value={"is_paused": False})
        with patch(
            "airflow_workbench.model_lab.operations_api.dag_contract",
            AsyncMock(return_value={"contract_valid": True}),
        ), patch("airflow_workbench.model_lab.operations_api.pool_contract", AsyncMock()), patch(
            "airflow_workbench.model_lab.operations_api.airflow_api", native
        ), TestClient(
            app
        ) as client:
            response = client.post(
                "/api/mlops/dags/llm_sft/activate?environment_id=remote"
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(native.await_args.args[2], "/dags/team_model_lab_llm_train")

    def test_trigger_keeps_connection_id_and_cancels_at_deadline(self):
        trigger = ModelJobTrigger(
            "abc", 0, connection_id="remote_executor", poll_seconds=7
        )
        self.assertEqual(trigger.serialize()[1]["connection_id"], "remote_executor")
        call = AsyncMock()

        async def consume():
            return [event async for event in trigger.run()]

        with patch("airflow_workbench.worker_client.call", call):
            events = asyncio.run(consume())
        call.assert_awaited_once_with(
            "POST", "/jobs/abc/cancel", connection_id="remote_executor"
        )
        self.assertEqual(events[0].payload["status"], "timeout")

    def test_submit_defers_to_the_selected_executor(self):
        task = build_dags(self.target.model_dump())[1].tasks[0]
        report = portable_training.recipe_report(self.recipe(), self.target)
        report["datasets"] = [{"sha256": "b" * 64}, {"sha256": "c" * 64}]
        context = {
            "dag": task.dag,
            "dag_run": SimpleNamespace(
                conf={"workbench": report}, run_id="manual__one"
            ),
        }
        submit = AsyncMock(return_value={"id": "job123"})
        with patch("airflow_workbench.worker_client.call", submit):
            with self.assertRaises(TaskDeferred) as deferred:
                task.execute(context)
        self.assertEqual(submit.await_args.kwargs["connection_id"], "remote_executor")
        self.assertEqual(deferred.exception.trigger.connection_id, "remote_executor")
        self.assertEqual(
            submit.await_args.args[2]["payload"]["datasets"], report["datasets"]
        )

    def test_remote_preflight_does_not_require_airflow_local_files(self):
        app.dependency_overrides[access] = lambda: SimpleNamespace(
            get_id=lambda: "admin"
        )
        self.addCleanup(app.dependency_overrides.clear)
        remote = AsyncMock(
            return_value={
                "ready": True,
                "blockers": [],
                "datasets": [
                    {"uri": "s3://data/train.jsonl", "sha256": "b" * 64},
                    {"uri": "s3://data/validation.jsonl", "sha256": "c" * 64},
                ],
            }
        )
        with patch(
            "airflow_workbench.model_lab.operations_api.dag_contract",
            AsyncMock(return_value={"is_paused": False, "blockers": []}),
        ), patch(
            "airflow_workbench.model_lab.operations_api.pool_contract", AsyncMock(return_value={})
        ), patch(
            "airflow_workbench.worker_client.call", remote
        ), patch(
            "airflow_workbench.training.validate_dataset",
            side_effect=AssertionError("local data read"),
        ), TestClient(
            app
        ) as client:
            response = client.post(
                "/api/training/validate", json=self.recipe().model_dump()
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["ready"])
        self.assertEqual(response.json()["dag_id"], "team_model_lab_llm_train")
        self.assertEqual(remote.await_args.kwargs["connection_id"], "remote_executor")


if __name__ == "__main__":
    unittest.main(verbosity=2)
