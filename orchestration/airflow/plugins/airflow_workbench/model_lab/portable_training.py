"""Control-plane validation contains no local data/weight lookup."""

import hashlib
import json
from airflow_workbench.model_lab.schemas import TrainingRecipe


def recipe_report(recipe, environment):
    spec = recipe.model_dump()
    return {
        "ready": False,
        "blockers": [],
        "contract_version": 2,
        "recipe": spec,
        "recipe_sha256": hashlib.sha256(
            json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "environment_id": environment.id,
        "environment_fingerprint": environment.fingerprint(),
        "dag_id": environment.dag_ids().get(recipe.task),
        "effective_batch_size": recipe.batch_size * recipe.gradient_accumulation_steps,
        "datasets": [],
    }


def validate_remote_recipe(value, kind):
    recipe = TrainingRecipe.model_validate(value)
    if recipe.storage and recipe.storage.mode != "object":
        raise ValueError("Kubernetes 참조 실행기는 객체 저장소 프로필을 사용합니다.")
    if recipe.task != kind:
        raise ValueError("DAG workload와 학습 종류가 다릅니다.")
    if not all(
        "://" in v
        for v in (recipe.train_dataset, recipe.validation_dataset, recipe.artifact_uri)
    ):
        raise ValueError(
            "Kubernetes 실행은 학습/검증 데이터 URI와 결과 저장 URI가 필요합니다."
        )
    if not recipe.train_sha256 or not recipe.validation_sha256:
        raise ValueError("원격 학습/검증 데이터의 SHA256이 필요합니다.")
    if recipe.train_sha256 == recipe.validation_sha256:
        raise ValueError("학습/검증 데이터는 서로 달라야 합니다.")
    return recipe


def check_kubernetes(environment):
    from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

    # Read-only connectivity/RBAC check. Scheduling/admission happens on Kubernetes.
    hook = KubernetesHook(conn_id=environment.connection_id)
    client = hook.core_v1_client
    client.list_namespaced_pod(
        namespace=environment.namespace, limit=1, _request_timeout=10
    )
    return {
        "ready": True,
        "blockers": [],
        "admission": "Kubernetes scheduler",
        "namespace": environment.namespace,
        "image": environment.image,
    }
