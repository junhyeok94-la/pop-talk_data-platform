"""Validate raw collection and platform DAG contracts without external I/O."""
from datetime import timedelta
import os
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

DAG_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
EDGES = {
    "pop_talk_environment_check": {("check_project_mount", "check_platform_postgres")},
    "pop_talk_s3_canary": {("check_bucket", "inspect_prefixes")},
    "pop_talk_movie_raw_daily": {
        ("plan_collection", "collect_boxoffice"), ("collect_boxoffice", "collect_candidates"),
        ("collect_candidates", "collect_details"), ("collect_details", "collect_kmdb"),
        ("collect_kmdb", "validate_daily"),
    },
    "pop_talk_movie_raw_initial": {
        ("plan_collection", "collect_movie_list"), ("plan_collection", "collect_boxoffice"),
        ("collect_movie_list", "collect_details"), ("collect_details", "collect_kmdb_candidates"),
        ("collect_kmdb_candidates", "validate_raw_run"), ("collect_boxoffice", "validate_raw_run"),
    },
}


def edges(dag):
    return {(task.task_id, downstream) for task in dag.tasks for downstream in task.downstream_task_ids}


def check_contracts(dag_bag=None):
    bag = dag_bag or DagBag(DAG_FOLDER)
    assert not bag.import_errors, bag.import_errors
    optional = {name for name, dag in bag.dags.items()
                if "model-lab" in dag.tags or name == "workbench_retry_verification"}
    assert set(bag.dags) == set(EDGES) | optional, set(bag.dags)
    for name, expected in EDGES.items():
        dag = bag.dags[name]
        assert not dag.catchup, name
        assert edges(dag) == expected, (name, edges(dag))
        assert set(dag.task_ids) == {task for pair in expected for task in pair}, name
        restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
        assert edges(restored) == expected, name
    daily = bag.dags["pop_talk_movie_raw_daily"]
    initial = bag.dags["pop_talk_movie_raw_initial"]
    assert daily.schedule == "0 3 * * *"
    assert initial.schedule is None
    for dag in (daily, initial):
        assert dag.is_paused_upon_creation and dag.max_active_runs == 1
        assert dag.max_active_tasks == 1
        for task in dag.tasks:
            assert task.retries == 2
            assert task.execution_timeout == timedelta(hours=8)
    assert daily.params["days_back"] == 7 and daily.params["max_movies"] == 0
    assert daily.params.get_param("days_back").schema["maximum"] == 31
    assert initial.params.get_param("max_movies_per_year").schema["maximum"] == 10000
    assert daily.get_task("validate_daily").outlets, "Raw READY must emit an Asset event"
    for task in initial.tasks:
        assert task.retry_exponential_backoff and task.max_retry_delay == timedelta(minutes=30)
    assert bag.dags["pop_talk_environment_check"].schedule is None
    assert bag.dags["pop_talk_environment_check"].is_paused_upon_creation
    assert bag.dags["pop_talk_s3_canary"].schedule is None
    for name in optional:
        dag = bag.dags[name]
        assert dag.schedule is None and dag.is_paused_upon_creation and not dag.catchup, name


if __name__ == "__main__":
    parsed = DagBag(DAG_FOLDER)
    check_contracts(parsed)
    print(f"DAG structure contracts passed: {len(parsed.dags)} DAGs")
