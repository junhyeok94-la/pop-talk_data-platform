"""Model Lab DAG 계약: 외부 GPU 실행, 수동 시작, pool 격리, 유한 timeout."""

import asyncio
import hashlib
import time
from datetime import timedelta

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow_workbench.model_lab import worker_client

POOL = "model_lab_gpu"
TAGS = {"model-lab", "mlops", "external-executor", "contract:v2"}
DAG_IDS = {
    "diagnostic": "model_lab_diagnostic",
    "llm_sft": "model_lab_llm_sft",
    "embedding_contrastive": "model_lab_embedding_contrastive",
}


class ModelJobTrigger(BaseTrigger):
    def __init__(
        self,
        job_id,
        deadline,
        connection_id=worker_client.CONNECTION_ID,
        poll_seconds=10,
    ):
        super().__init__()
        self.job_id = job_id
        self.deadline = deadline
        self.connection_id = connection_id
        self.poll_seconds = poll_seconds

    def serialize(self):
        return (
            "airflow_workbench.model_lab.mlops.ModelJobTrigger",
            {
                "job_id": self.job_id,
                "deadline": self.deadline,
                "connection_id": self.connection_id,
                "poll_seconds": self.poll_seconds,
            },
        )

    async def run(self):
        while time.time() < self.deadline:
            try:
                job = await worker_client.call(
                    "GET", "/jobs/" + self.job_id, connection_id=self.connection_id
                )
                if job["status"] not in {"queued", "running", "cancelling"}:
                    yield TriggerEvent({"job_id": self.job_id, "status": job["status"]})
                    return
            except Exception:
                # 일시적인 네트워크 장애는 실패로 오인하지 않고 유한 deadline 내 재확인한다.
                pass
            await asyncio.sleep(self.poll_seconds)
        try:
            await worker_client.call(
                "POST",
                "/jobs/" + self.job_id + "/cancel",
                connection_id=self.connection_id,
            )
        finally:
            yield TriggerEvent({"job_id": self.job_id, "status": "timeout"})


class ExternalModelJobOperator(BaseOperator):
    """submit 뒤 defer하여 LocalExecutor 프로세스를 해제한다. 학습 import는 없다."""

    def __init__(self, *, kind, environment=None, **kwargs):
        super().__init__(**kwargs)
        self.kind = kind
        self.environment = environment
        self._job_id = None

    def execute(self, context):
        conf = context["dag_run"].conf or {}
        from airflow_workbench.model_lab.environments import Environment

        environment = Environment.model_validate(self.environment)
        report = conf.get("workbench", {})
        if report.get("environment_fingerprint") != environment.fingerprint():
            raise AirflowException(
                "실행 환경과 DAG 설정 버전이 다릅니다. 다시 검증하세요."
            )
        identity = f"{context['dag'].dag_id}|{context['dag_run'].run_id}|{self.task_id}"
        key = "af_" + hashlib.sha256(identity.encode()).hexdigest()[:48]
        if self.kind == "diagnostic":
            spec = {
                "key": key,
                "kind": "diagnostic",
                "profile": "diagnostic",
                "required_gpu_mib": int(conf.get("required_gpu_mib", 256)),
                "payload": {},
            }
            timeout = min(environment.timeout_seconds, 180)
        else:
            from airflow_workbench.model_lab.schemas import TrainingRecipe

            payload = conf.get("workbench", {})
            recipe = TrainingRecipe.model_validate(payload.get("recipe", {}))
            if recipe.task != self.kind:
                raise AirflowException("DAG 학습 종류와 recipe가 다릅니다.")
            spec = {
                "key": key,
                "kind": self.kind,
                "profile": environment.resource_profiles.get(
                    recipe.method, recipe.method
                ),
                "payload": payload,
            }
            timeout = environment.timeout_seconds
        job = asyncio.run(
            worker_client.call(
                "POST", "/jobs", spec, connection_id=environment.connection_id
            )
        )
        self._job_id = job["id"]
        self.defer(
            trigger=ModelJobTrigger(
                job_id=job["id"],
                deadline=time.time() + timeout,
                connection_id=environment.connection_id,
                poll_seconds=environment.poll_seconds,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=timeout + 30),
        )

    def execute_complete(self, context, event=None):
        if not event or event.get("status") != "success":
            raise AirflowException(
                f"외부 모델 작업 종료: {(event or {}).get('status','unknown')}"
            )
        job = asyncio.run(
            worker_client.call(
                "GET",
                "/jobs/" + event["job_id"],
                connection_id=self.environment["connection_id"],
            )
        )
        if job.get("status") != "success":
            raise AirflowException("외부 실행기의 최종 성공 상태를 확인할 수 없습니다.")
        result = job.get("result") or {}
        if self.kind != "diagnostic" and (
            result.get("training_performed") is not True
            or not (result.get("artifact_id") or result.get("artifact_uri"))
        ):
            raise AirflowException(
                "외부 실행기의 학습 완료 결과와 artifact 참조가 필요합니다."
            )
        return {
            "job_id": job["id"],
            "status": job["status"],
            "artifact_id": result.get("artifact_id"),
            "artifact_uri": result.get("artifact_uri"),
            "training_performed": result.get("training_performed", False),
        }

    def on_kill(self):
        if self._job_id:
            asyncio.run(
                worker_client.call(
                    "POST",
                    "/jobs/" + self._job_id + "/cancel",
                    connection_id=self.environment["connection_id"],
                )
            )


def validate_dag_metadata(dag, tasks, environment=None):
    failures = []
    tags = {t.get("name") if isinstance(t, dict) else t for t in dag.get("tags", [])}
    if not TAGS.issubset(tags):
        failures.append("Model Lab 표준 tag가 없습니다.")
    if environment and not {
        "environment:" + environment.id,
        "config:" + environment.fingerprint(),
    }.issubset(tags):
        failures.append("저장한 실행 환경의 최신 DAG bundle을 배포해야 합니다.")
    if dag.get("max_active_runs") != (
        environment.max_active_runs if environment else 1
    ):
        failures.append("max_active_runs가 실행 환경 설정과 다릅니다.")
    if dag.get("max_active_tasks") != 1:
        failures.append("max_active_tasks=1이어야 합니다.")
    if dag.get("catchup") is not False:
        failures.append("catchup=False이어야 합니다.")
    if (
        dag.get("timetable_summary") is not None
        or dag.get("timetable_periodic") is not False
    ):
        failures.append("수동 실행 DAG이어야 합니다.")
    if "mlops" not in dag.get("owners", []):
        failures.append("owner=mlops가 필요합니다.")
    if not dag.get("dag_run_timeout"):
        failures.append("DAG run timeout이 필요합니다.")
    if len(tasks) != 1:
        failures.append("외부 실행 operator 하나가 필요합니다.")
    for task in tasks:
        expected = (
            "PortableTrainingPodOperator"
            if environment and environment.backend == "kubernetes"
            else "ExternalModelJobOperator"
        )
        if task.get("operator_name") != expected:
            failures.append("GPU 학습을 외부 실행기로 제출해야 합니다.")
        if task.get("pool") != (environment.pool if environment else POOL) or task.get(
            "pool_slots"
        ) != (environment.pool_slots if environment else 1):
            failures.append("실행 환경의 pool/slot 설정과 다릅니다.")
        if task.get("retries") != 0:
            failures.append("자동 학습 재시도는 기본 0이어야 합니다.")
        if task.get("execution_timeout") is None:
            failures.append("execution_timeout이 필요합니다.")
    return failures
