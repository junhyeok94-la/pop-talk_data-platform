"""Validate raw collection and platform DAG contracts without external I/O."""
from datetime import timedelta
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

# Direct script execution otherwise puts tests/pipelines ahead of the runtime package.
sys.path.insert(0, os.environ.get('POP_TALK_PROJECT_ROOT','/opt/airflow/modules'))

DAG_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
EDGES = {
    'pop_talk_warehouse': {('resolve_inputs','load_stg'),('load_stg','build_and_publish')},
    'pop_talk_reviews_warehouse': {('load_review_snapshot','build_review_marts')},
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
    optional = {"workbench_retry_verification"}
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
    warehouse = bag.dags['pop_talk_warehouse']
    assert warehouse.is_paused_upon_creation and warehouse.max_active_runs == 1
    assert warehouse.params['publish_service'] is False
    resolve = warehouse.get_task('resolve_inputs').python_callable
    bucket = 'amzn-s3-pop-talk-dw-047342411109-ap-northeast-2-an'
    uri = f's3://{bucket}/manifests/movie_api_daily/v1/DAILY_READY'
    key = 'manifests/movie_api_daily/v1/collection_date=2026-09-23/run_id=' + 'a'*24 + '/DAILY_READY.json'
    metadata = dict(contract_version=1,bucket=bucket,ready_manifest_key=key,
        source_dag_id='pop_talk_movie_raw_daily',source_run_id='source-run',
        collection_date='2026-09-23',raw_run_id='a'*24)
    event = SimpleNamespace(source_dag_id='pop_talk_movie_raw_daily',source_run_id='source-run',extra=metadata)
    context = {'dag_run':SimpleNamespace(run_type='asset_triggered'),
               'params':{'initial_success_manifest_key':'','ready_manifest_key':''},
               'triggering_asset_events':{uri:[event]}}
    with patch.dict(resolve.__globals__, get_current_context=lambda:context):
        assert resolve() == [metadata]
        context['dag_run'].run_type='manual'
        context['params']['initial_success_manifest_key']='manifests/movie_api/v1/run_id='+'b'*24+'/SUCCESS.json'
        assert resolve()[0]['kind']=='initial'
        context['params']['initial_success_manifest_key']=''
        context['params']['initial_snapshot_sha256']='c'*64
        assert resolve()==[{'kind':'legacy_snapshot','sha256':'c'*64}]
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
