"""Remote evaluation DAGs: bounded HTTP in a trigger, no model libraries or DB."""

import asyncio
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, BaseOperator
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow_workbench.model_lab.contracts import Endpoint


class EvaluationTrigger(BaseTrigger):
    def __init__(self, run_id, endpoint, config, dataset):
        super().__init__()
        self.args = dict(
            run_id=run_id, endpoint=endpoint, config=config, dataset=dataset
        )

    def serialize(self):
        return (
            "airflow_workbench.model_lab.evaluation_dag.EvaluationTrigger",
            self.args,
        )

    async def run(self):
        import fcntl
        from airflow_workbench.model_lab.evaluation import evaluate, artifact_path

        path = await asyncio.to_thread(artifact_path, self.args["run_id"])
        handle = await asyncio.to_thread(open, path.with_suffix(".lock"), "a+")
        try:
            # HA triggerers can briefly run the same trigger. Shared filesystem
            # locks prevent duplicate submissions and conflicting result writes.
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(5)
            result = await evaluate(**self.args)
            yield TriggerEvent(
                {"run_id": self.args["run_id"], "status": result["status"]}
            )
        finally:
            await asyncio.to_thread(handle.close)

    async def on_kill(self):
        from airflow_workbench.model_lab.evaluation import artifact_path

        path = await asyncio.to_thread(artifact_path, self.args["run_id"])
        await asyncio.to_thread(
            path.with_suffix(".cancel").write_text, "cancel requested", encoding="utf-8"
        )


class RemoteEvaluationOperator(BaseOperator):
    def __init__(self, *, endpoint, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint

    def execute(self, context):
        from airflow_workbench.model_lab.contracts import Dataset, Evaluation

        spec = Endpoint.model_validate(self.endpoint)
        payload = context["dag_run"].conf.get("model_lab", {})
        if payload.get("fingerprint") != spec.fingerprint():
            raise AirflowException("평가 DAG와 추론 연결 버전이 다릅니다.")
        config = Evaluation.model_validate(payload["config"]).model_dump()
        import uuid

        if (
            payload["run_id"] != uuid.UUID(config["request_id"]).hex
            or context["dag_run"].run_id != "model_lab__" + config["request_id"]
        ):
            raise AirflowException("평가 실행 ID와 DAG 실행 ID가 다릅니다.")
        dataset = Dataset.model_validate(payload["dataset"]).model_dump()
        if dataset["kind"] not in spec.capabilities:
            raise AirflowException("이 연결은 선택한 평가를 지원하지 않습니다.")
        self.defer(
            trigger=EvaluationTrigger(
                payload["run_id"], self.endpoint, config, dataset
            ),
            method_name="execute_complete",
            timeout=timedelta(hours=6),
        )

    def execute_complete(self, context, event=None):
        if not event or event.get("status") != "success":
            raise AirflowException(
                "Model Lab 평가 종료: " + str((event or {}).get("status", "unknown"))
            )
        # Large case results remain on the shared artifact volume.
        return {"run_id": event["run_id"], "artifact": event["run_id"] + ".json"}


def build_dag(endpoint):
    spec = Endpoint.model_validate(endpoint)
    with DAG(
        spec.dag_id(),
        description="Model Lab · " + spec.name + " 원격 평가",
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=spec.capacity,
        max_active_tasks=spec.capacity,
        dagrun_timeout=timedelta(hours=6, minutes=5),
        tags=[
            "model-lab",
            "mlops",
            "workload:evaluation",
            "contract:lab-v1",
            "endpoint:" + spec.id,
            "config:" + spec.fingerprint(),
        ],
        default_args={"owner": "mlops", "retries": 0},
    ) as dag:
        RemoteEvaluationOperator(
            task_id="evaluate",
            endpoint=spec.model_dump(),
            pool=spec.pool,
            pool_slots=1,
            execution_timeout=timedelta(hours=6),
        )
    return dag


def source(spec):
    return (
        "from airflow_workbench.model_lab.evaluation_dag import build_dag\n"
        + "dag = build_dag("
        + repr(spec.model_dump())
        + ")\n"
    )
