"""리팩터링 전 승인된 Airflow DAG 구조 계약을 검사한다.

실제 Airflow 이미지 안에서 실행한다. 데이터나 Connection에 접근하지 않고 DAG를 parse해
schedule, Params, task graph, 재시도·timeout과 핵심 Jinja/XCom 참조만 비교한다.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.serialization.serialized_objects import DagSerialization

PROJECT_ROOT = Path(os.environ.get("POP_TALK_PROJECT_ROOT", "/opt/airflow/modules"))
# Direct script execution puts tests/ first; select the runtime pipelines package.
sys.path.insert(0, str(PROJECT_ROOT))

from dbt_cosmos_runner import prepare_invocation
from pipelines.paths import DBT_DEPLOYMENT_ROOT
from pipelines.orchestration.dbt_deployment import validate_deployment

DAG_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")

EXPECTED = {
    "pop_talk_environment_check": {
        "schedule": "None",
        "start_date": "2026-09-09T00:00:00+00:00",
        "paused": None,
        "max_active_runs": 16,
        "max_active_tasks": 4,
        "params": {},
        "tasks": {"check_project_mount", "check_serving_postgres"},
        "edges": {("check_project_mount", "check_serving_postgres")},
        "retries": 0,
        "timeout": None,
    },
    "pop_talk_movie_databricks_daily": {
        "schedule_asset_uri": (
            "s3://amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an/"
            "manifests/movie_api_daily/v1/DAILY_READY"
        ),
        "start_date": "2026-09-08T15:00:00+00:00",
        "timezone": "Asia/Seoul",
        "paused": True,
        "max_active_runs": 1,
        "max_active_tasks": 4,
        "params": {
            "ready_manifest_key": {
                "value": (
                    "manifests/movie_api_daily/v1/collection_date=2026-09-09/"
                    "run_id=4834bb8200e5a76f2cf8b408/DAILY_READY.json"
                ),
                "type": "string",
                "pattern": r"^manifests/movie_api_daily/.+/DAILY_READY[.]json$",
            },
            "publication_revision": {"value": 1, "type": "integer", "minimum": 1},
            "processing_attempt": {"value": 1, "type": "integer", "minimum": 1},
            "snowflake_fail_after_movie_merge": {"value": False, "type": "boolean"},
        },
        "tasks": {
            "resolve_ready_inputs", "deploy_notebook", "stage_bundle", "prepare_transform_requests",
            "transform_bronze_silver", "publish_exchange", "load_snowflake", "confirm_loaded_batch",
            "start_model_generation",
        },
        "edges": {
            ("resolve_ready_inputs", "deploy_notebook"),
            ("deploy_notebook", "stage_bundle"),
            ("stage_bundle", "prepare_transform_requests"), ("prepare_transform_requests", "transform_bronze_silver"),
            ("stage_bundle", "publish_exchange"), ("transform_bronze_silver", "publish_exchange"),
            ("publish_exchange", "load_snowflake"),
            ("load_snowflake", "confirm_loaded_batch"),
            ("confirm_loaded_batch", "start_model_generation"),
        },
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "timeout": timedelta(hours=2),
        "dagrun_timeout": timedelta(hours=4),
    },
    "pop_talk_movie_raw_daily": {
        "schedule": "0 3 * * *",
        "start_date": "2026-09-08T15:00:00+00:00",
        "timezone": "Asia/Seoul",
        "paused": True,
        "max_active_runs": 1,
        "max_active_tasks": 1,
        "params": {
            "days_back": {"value": 7, "type": "integer", "minimum": 1, "maximum": 31},
            "max_movies": {
                "value": 0,
                "type": "integer",
                "minimum": 0,
                "maximum": 1000,
            },
            "kmdb_max_pages": {
                "value": 3,
                "type": "integer",
                "minimum": 1,
                "maximum": 10,
            },
        },
        "tasks": {
            "plan_collection",
            "collect_candidates", "collect_boxoffice",
            "collect_details",
            "collect_kmdb",
            "validate_daily",
        },
        "edges": {
            ("plan_collection", "collect_boxoffice"),
            ("collect_boxoffice", "collect_candidates"),
            ("collect_candidates", "collect_details"),
            ("collect_details", "collect_kmdb"),
            ("collect_kmdb", "validate_daily"),
        },
        "retries": 2,
        "timeout": timedelta(hours=8),
    },
    "pop_talk_movie_raw_initial": {
        "schedule": "None",
        "start_date": "2026-09-09T00:00:00+00:00",
        "paused": True,
        "max_active_runs": 1,
        "max_active_tasks": 1,
        "params": {
            "start_year": {
                "value": 2026,
                "type": "integer",
                "minimum": 2020,
                "maximum": 2200,
            },
            "end_year": {
                "value": 2026,
                "type": "integer",
                "minimum": 2020,
                "maximum": 2200,
            },
            "max_movies_per_year": {
                "value": 10,
                "type": "integer",
                "minimum": 0,
                "maximum": 10000,
            },
            "max_pages_per_year": {
                "value": 100,
                "type": "integer",
                "minimum": 1,
                "maximum": 200,
            },
            "kmdb_max_pages": {
                "value": 3,
                "type": "integer",
                "minimum": 1,
                "maximum": 10,
            },
            "boxoffice_start": {
                "value": "",
                "type": "string",
                "pattern": r"^(|\d{4}-\d{2}-\d{2})$",
            },
            "boxoffice_end": {
                "value": "",
                "type": "string",
                "pattern": r"^(|\d{4}-\d{2}-\d{2})$",
            },
        },
        "tasks": {
            "plan_collection",
            "collect_movie_list",
            "collect_details",
            "collect_kmdb_candidates",
            "collect_boxoffice",
            "validate_raw_run",
        },
        "edges": {
            ("plan_collection", "collect_movie_list"),
            ("plan_collection", "collect_boxoffice"),
            ("collect_movie_list", "collect_details"),
            ("collect_details", "collect_kmdb_candidates"),
            ("collect_kmdb_candidates", "validate_raw_run"),
            ("collect_boxoffice", "validate_raw_run"),
        },
        "retries": 2,
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "timeout": timedelta(hours=8),
    },
    "pop_talk_s3_canary": {
        "schedule": "None",
        "start_date": "2026-09-09T00:00:00+00:00",
        "paused": None,
        "max_active_runs": 16,
        "max_active_tasks": 4,
        "params": {},
        "tasks": {"check_bucket", "inspect_prefixes"},
        "edges": {("check_bucket", "inspect_prefixes")},
        "retries": 0,
        "timeout": None,
    },
    "pop_talk_snowflake_transaction_probe": {
        "schedule": "None",
        "start_date": "2026-09-09T00:00:00+00:00",
        "paused": True,
        "max_active_runs": 16,
        "max_active_tasks": 4,
        "params": {},
        "tasks": {"prove_rollback"},
        "edges": set(),
        "retries": 0,
        "timeout": None,
    },
}


def _edges(dag) -> set[tuple[str, str]]:
    return {
        (task.task_id, downstream)
        for task in dag.tasks
        for downstream in task.downstream_task_ids
    }


def check_model_dags(dag_bag: DagBag) -> set[str]:
    """DW 전체가 하나의 순차 실행과 품질 검사 뒤 서빙으로 연결되는지 확인한다."""
    dag = dag_bag.dags["pop_talk_dw_serving"]
    models = [t for t in dag.tasks if t.task_id.endswith("_run")]
    assert len(models) == 7
    assert len(dag.tasks) == 10
    assert dag.get_task("publish").upstream_task_ids == {"quality_checks"}
    for model in models:
        ancestors = {t.task_id for t in model.get_flat_relatives(upstream=True)}
        descendants = {t.task_id for t in model.get_flat_relatives(upstream=False)}
        assert "prepare" in ancestors
        assert {"quality_checks", "publish"} <= descendants
    assert "dw.models.staging.stg_movie_observations_run" in dag.get_task("dw.models.dw.dim_movie_run").upstream_task_ids
    assert dag.schedule is None and dag.max_active_runs == 1 and not dag.catchup
    assert all(task.trigger_rule == "all_success" for task in dag.tasks)
    assert "dw" in dag.task_group.children
    restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
    assert _edges(restored) == _edges(dag)
    return {dag.dag_id}


def check_contracts(dag_bag: DagBag | None = None) -> None:
    """현재 DagBag을 승인 baseline과 비교하고 차이가 있으면 즉시 실패한다."""
    dag_bag = dag_bag or DagBag(DAG_FOLDER)
    assert not dag_bag.import_errors, dag_bag.import_errors
    from airflow_workbench.mlops import (
        TAGS,
        ExternalModelJobOperator,
        ModelJobTrigger,
    )

    from airflow_workbench.environments import Environment
    from airflow_workbench.kubernetes_executor import PortableTrainingPodOperator
    from airflow_workbench.lab_contracts import Endpoint
    from airflow_workbench.lab_dag import RemoteEvaluationOperator

    managed = {
        name: dag for name, dag in dag_bag.dags.items() if "model-lab" in dag.tags
    }
    assert managed, "No Model Lab environment DAG bundle was parsed"
    model_dags = check_model_dags(dag_bag)
    manual_chains = {
        "workbench_retry_verification": ("already_succeeded", "fail_once", "downstream"),
        "pop_talk_movie_initial_load": (
            "deploy_notebook", "stage_bundle", "transform_candidate_bronze_silver",
            "publish_exchange", "load_snowflake", "confirm_initial_load", "build_dw_and_serve",
        ),
    }
    for name, chain in manual_chains.items():
        dag = dag_bag.dags[name]
        assert dag.schedule is None and dag.is_paused_upon_creation, name
        assert not dag.catchup and dag.max_active_runs == 1, name
        assert set(dag.task_ids) == set(chain), name
        assert all(b in dag.task_dict[a].downstream_task_ids for a,b in zip(chain,chain[1:])), name
    expected_ids = set(EXPECTED) | set(managed) | model_dags | set(manual_chains)
    initial = dag_bag.dags["pop_talk_movie_initial_load"]
    assert set(initial.params) == {"manifest_key"}
    assert _edges(initial) == set(zip(manual_chains[initial.dag_id], manual_chains[initial.dag_id][1:]))
    handoff = initial.get_task("build_dw_and_serve")
    assert handoff.trigger_dag_id == "pop_talk_dw_serving"
    assert handoff.wait_for_completion and handoff.deferrable and handoff.reset_dag_run
    assert set(dag_bag.dags) == expected_ids, {
        "unexpected": set(dag_bag.dags) - expected_ids,
        "missing": expected_ids - set(dag_bag.dags),
    }
    environment_managed = {
        dag_id: dag
        for dag_id, dag in managed.items()
        if hasattr(dag.tasks[0], "environment")
    }
    evaluation_managed = {
        dag_id: dag
        for dag_id, dag in managed.items()
        if isinstance(dag.tasks[0], RemoteEvaluationOperator)
    }
    assert set(managed) == set(environment_managed) | set(evaluation_managed), set(
        managed
    )

    for dag_id in environment_managed:
        dag = dag_bag.dags[dag_id]
        task = dag.tasks[0]
        environment = Environment.model_validate(task.environment)
        kind = task.kind
        assert environment.dag_ids()[kind] == dag_id
        assert dag.schedule is None and dag.catchup is False, dag_id
        assert (
            dag.max_active_runs == environment.max_active_runs
            and dag.max_active_tasks == 1
        ), dag_id
        assert dag.is_paused_upon_creation is True, dag_id
        assert TAGS.issubset(set(dag.tags)), dag_id
        assert len(dag.tasks) == 1, dag_id
        task = dag.tasks[0]
        assert isinstance(
            task,
            (
                ExternalModelJobOperator
                if environment.backend == "http"
                else PortableTrainingPodOperator
            ),
        ), dag_id
        assert {
            "environment:" + environment.id,
            "backend:" + environment.backend,
            "workload:" + kind,
            "config:" + environment.fingerprint(),
        }.issubset(set(dag.tags)), dag_id
        assert task.owner == "mlops" and task.retries == 0, dag_id
        assert (
            task.pool == environment.pool and task.pool_slots == environment.pool_slots
        ), dag_id
        seconds = (
            min(environment.timeout_seconds, 180)
            if kind == "diagnostic"
            else environment.timeout_seconds
        )
        expected_timeout = timedelta(seconds=seconds + 120)
        assert task.execution_timeout == expected_timeout, dag_id
        assert dag.dagrun_timeout == expected_timeout + timedelta(minutes=2), dag_id
        restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
        assert restored.tasks[0].pool == environment.pool, dag_id

    for dag_id in evaluation_managed:
        dag = dag_bag.dags[dag_id]
        task = dag.tasks[0]
        endpoint = Endpoint.model_validate(task.endpoint)
        assert endpoint.dag_id() == dag_id
        assert dag.schedule is None and dag.catchup is False, dag_id
        assert dag.max_active_runs == endpoint.capacity, dag_id
        assert dag.max_active_tasks == endpoint.capacity, dag_id
        assert dag.is_paused_upon_creation is True, dag_id
        assert len(dag.tasks) == 1 and task.task_id == "evaluate", dag_id
        assert {
            "model-lab",
            "mlops",
            "workload:evaluation",
            "contract:lab-v1",
            "endpoint:" + endpoint.id,
            "config:" + endpoint.fingerprint(),
        }.issubset(set(dag.tags)), dag_id
        assert task.owner == "mlops" and task.retries == 0, dag_id
        assert task.pool == endpoint.pool and task.pool_slots == 1, dag_id
        assert task.execution_timeout == timedelta(hours=6), dag_id
        assert dag.dagrun_timeout == timedelta(hours=6, minutes=5), dag_id
        restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
        assert restored.tasks[0].pool == endpoint.pool, dag_id
    serialized_trigger = ModelJobTrigger(job_id="test", deadline=123).serialize()
    assert serialized_trigger[1] == {
        "job_id": "test",
        "deadline": 123,
        "connection_id": "workbench_model_worker",
        "poll_seconds": 10,
    }

    for dag_id, expected in EXPECTED.items():
        dag = dag_bag.dags[dag_id]
        if "schedule_asset_uri" in expected:
            assert len(dag.schedule) == 1, dag_id
            assert dag.schedule[0].uri == expected["schedule_asset_uri"], dag_id
        else:
            assert str(dag.schedule) == expected["schedule"], dag_id
        assert dag.start_date.isoformat() == expected["start_date"], dag_id
        assert str(dag.timezone) == expected.get("timezone", "UTC"), dag_id
        assert dag.catchup is False, dag_id
        assert dag.is_paused_upon_creation == expected["paused"], dag_id
        assert dag.max_active_runs == expected["max_active_runs"], dag_id
        assert dag.max_active_tasks == expected["max_active_tasks"], dag_id
        assert dag.dagrun_timeout == expected.get("dagrun_timeout"), dag_id
        assert {task.task_id for task in dag.tasks} == expected["tasks"], dag_id
        assert _edges(dag) == expected["edges"], dag_id

        assert set(dag.params) == set(expected["params"]), dag_id
        for name, param_contract in expected["params"].items():
            # ParamsDict의 []는 resolve된 값이며 get_param()이 schema 객체를 반환한다.
            param = dag.params.get_param(name)
            assert param.value == param_contract["value"], (dag_id, name, param.value)
            for field, value in param_contract.items():
                if field != "value":
                    assert param.schema[field] == value, (dag_id, name, field)

        for task in dag.tasks:
            assert str(task.trigger_rule) == "TriggerRule.ALL_SUCCESS", (
                dag_id,
                task.task_id,
            )
            assert task.retries == expected["retries"], (dag_id, task.task_id)
            assert task.retry_delay == expected.get(
                "retry_delay", timedelta(minutes=5)
            ), (dag_id, task.task_id)
            assert bool(task.retry_exponential_backoff) is expected.get(
                "retry_exponential_backoff", False
            ), (dag_id, task.task_id)
            assert task.max_retry_delay == expected.get("max_retry_delay"), (
                dag_id,
                task.task_id,
            )
            is_dbt_task = task.task_id.startswith("build_snowflake_gold.")
            task_timeout = timedelta(minutes=30) if is_dbt_task else expected["timeout"]
            task_pool = "pop_talk_dbt_pool" if is_dbt_task else "default_pool"
            assert task.execution_timeout == task_timeout, (dag_id, task.task_id)
            assert task.pool == task_pool, (dag_id, task.task_id)
            assert getattr(task, "multiple_outputs", False) is False, (
                dag_id,
                task.task_id,
            )

    pipeline = dag_bag.dags["pop_talk_movie_databricks_daily"]
    assert {task.task_id for task in pipeline.roots} == {"resolve_ready_inputs"}
    raw_daily = dag_bag.dags["pop_talk_movie_raw_daily"]
    ready_outlets = raw_daily.get_task("validate_daily").outlets
    assert len(ready_outlets) == 1
    assert ready_outlets[0].uri == pipeline.schedule[0].uri

    mapped_writes = {
        task_id: pipeline.get_task(task_id)
        for task_id in (
            "stage_bundle",
            "transform_bronze_silver",
            "publish_exchange",
            "load_snowflake",
        )
    }
    assert all(task.max_active_tis_per_dag == 1 for task in mapped_writes.values())
    transform = pipeline.get_task("transform_bronze_silver")
    assert transform.operator_class is DatabricksSubmitRunOperator
    assert transform.expand_input.value.operator.task_id == "prepare_transform_requests"
    assert transform.partial_kwargs["databricks_conn_id"] == "pop_talk_databricks"
    assert "json" not in transform.partial_kwargs
    assert "idempotency_token" not in transform.partial_kwargs
    start = pipeline.get_task("start_model_generation")
    assert start.trigger_dag_id == "pop_talk_dw_serving"
    assert start.skip_when_already_exists and not start.wait_for_completion
    restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(pipeline))
    assert restored.get_task("start_model_generation").trigger_dag_id == start.trigger_dag_id


def check_failure_regressions(dag_bag: DagBag) -> None:
    """mapped 동시성과 retry 변조를 검사가 실제로 거부하는지 입증한다."""
    pipeline = dag_bag.dags["pop_talk_movie_databricks_daily"]
    transform = pipeline.get_task("transform_bronze_silver")
    max_active = transform.max_active_tis_per_dag
    try:
        transform.max_active_tis_per_dag = 2
        try:
            check_contracts(dag_bag)
        except AssertionError:
            pass
        else:
            raise AssertionError(
                "mapped Databricks 동시 실행 확대를 구조 검사가 허용했습니다"
            )
    finally:
        transform.max_active_tis_per_dag = max_active

    deploy = pipeline.get_task("deploy_notebook")
    retry_delay = deploy.retry_delay
    try:
        deploy.retry_delay = timedelta(0)
        try:
            check_contracts(dag_bag)
        except AssertionError:
            pass
        else:
            raise AssertionError("잘못된 retry_delay를 구조 검사가 허용했습니다")
    finally:
        deploy.retry_delay = retry_delay


if __name__ == "__main__":
    parsed_dags = DagBag(DAG_FOLDER)
    check_contracts(parsed_dags)
    check_failure_regressions(parsed_dags)
    print(f"DAG 구조 계약 통과: {len(parsed_dags.dags)}개 DAG")
