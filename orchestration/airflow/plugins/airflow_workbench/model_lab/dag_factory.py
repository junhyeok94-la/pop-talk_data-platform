"""Portable DAG construction. No metadata DB, secrets, or network during parsing."""

from datetime import timedelta
import pendulum
from airflow.sdk import DAG
from airflow_workbench.model_lab.environments import Environment
from airflow_workbench.model_lab.mlops import ExternalModelJobOperator, TAGS


def build_dags(settings):
    environment = Environment.model_validate(settings)
    dags = []
    for kind, dag_id in environment.dag_ids().items():
        seconds = (
            min(environment.timeout_seconds, 180)
            if kind == "diagnostic"
            else environment.timeout_seconds
        )
        with DAG(
            dag_id=dag_id,
            description=f"Model Lab {kind} · {environment.name} ({environment.backend})",
            schedule=None,
            start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
            catchup=False,
            is_paused_upon_creation=True,
            max_active_runs=environment.max_active_runs,
            max_active_tasks=1,
            dagrun_timeout=timedelta(seconds=seconds + 240),
            tags=sorted(
                TAGS
                | {
                    "environment:" + environment.id,
                    "backend:" + environment.backend,
                    "workload:" + kind,
                    "config:" + environment.fingerprint(),
                }
            ),
            default_args={"owner": "mlops", "retries": 0},
            doc_md="Submit to the configured external executor. No training libraries or metadata DB access in Airflow task processes. Input: dag_run.conf.workbench; output: external artifact reference. Manual execution only.",
        ) as dag:
            options = dict(
                task_id="run_external_job",
                kind=kind,
                environment=environment.model_dump(),
                pool=environment.pool,
                pool_slots=environment.pool_slots,
                execution_timeout=timedelta(seconds=seconds + 120),
            )
            if environment.backend == "http":
                ExternalModelJobOperator(**options)
            else:
                from airflow_workbench.model_lab.kubernetes_executor import (
                    PortableTrainingPodOperator,
                )

                PortableTrainingPodOperator(**options)
        dags.append(dag)
    return dags
