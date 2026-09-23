"""Manual, side-effect-free verification of Workbench's native retry workflow."""

from datetime import datetime, timezone

from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="workbench_retry_verification",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["workbench-verification"],
    default_args={"owner": "workbench", "retries": 0},
)
def retry_verification():
    @task
    def already_succeeded():
        return "This successful task must not be cleared."

    @task
    def fail_once():
        if get_current_context()["ti"].try_number == 1:
            raise RuntimeError(
                "Intentional first-attempt failure for retry verification"
            )
        return "Recovered through the native Airflow Clear API."

    @task
    def downstream():
        return "Recovered from upstream_failed. No external systems were used."

    already_succeeded() >> fail_once() >> downstream()


retry_verification()
