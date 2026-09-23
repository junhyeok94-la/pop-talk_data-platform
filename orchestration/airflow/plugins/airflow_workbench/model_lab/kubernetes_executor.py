"""Native deferrable Kubernetes execution using the maintained Airflow provider."""

import json
from kubernetes.client import V1EnvVar, V1ResourceRequirements
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow_workbench.model_lab.environments import Environment


class PortableTrainingPodOperator(KubernetesPodOperator):
    def __init__(self, *, kind, environment, **kwargs):
        target = Environment.model_validate(environment)
        self.kind, self.environment = kind, environment
        resources = {"cpu": target.cpu, "memory": target.memory}
        if target.gpu:
            resources["nvidia.com/gpu"] = str(target.gpu)
        super().__init__(
            kubernetes_conn_id=target.connection_id,
            namespace=target.namespace,
            image=target.image,
            service_account_name=target.service_account,
            container_resources=V1ResourceRequirements(
                requests=resources, limits=resources
            ),
            node_selector=target.node_selector,
            labels={
                "app.kubernetes.io/part-of": "airflow-model-lab",
                "workbench-environment": target.id,
            },
            name="model-lab-" + target.id,
            deferrable=True,
            reattach_on_restart=True,
            do_xcom_push=True,
            get_logs=True,
            schedule_timeout_seconds=120,
            startup_timeout_seconds=120,
            active_deadline_seconds=target.timeout_seconds,
            on_finish_action="delete_pod",
            log_pod_spec_on_failure=False,
            **kwargs,
        )

    def execute(self, context):
        from airflow_workbench.model_lab.portable_training import validate_remote_recipe

        target = Environment.model_validate(self.environment)
        report = (context["dag_run"].conf or {}).get("workbench", {})
        if report.get("environment_fingerprint") != target.fingerprint():
            raise AirflowException("실행 환경과 DAG 설정 버전이 다릅니다.")
        if self.kind != "diagnostic":
            validate_remote_recipe(report.get("recipe", {}), self.kind)
        payload = {
            "contract_version": 2,
            "kind": self.kind,
            "workbench": report,
            "run_id": context["dag_run"].run_id,
        }
        self.env_vars = [v for v in self.env_vars if v.name != "WORKBENCH_REQUEST"] + [
            V1EnvVar(name="WORKBENCH_REQUEST", value=json.dumps(payload))
        ]
        return super().execute(context)

    def trigger_reentry(self, context, event):
        result = super().trigger_reentry(context, event)
        if self.kind != "diagnostic" and (
            not isinstance(result, dict)
            or result.get("training_performed") is not True
            or not result.get("artifact_uri")
        ):
            raise AirflowException(
                "학습 완료 결과에 training_performed=true와 artifact_uri가 필요합니다."
            )
        return result
