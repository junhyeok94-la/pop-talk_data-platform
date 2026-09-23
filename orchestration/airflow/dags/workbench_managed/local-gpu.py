"""Model Lab environment bundle. Generated settings; execution is external."""
import json
from airflow_workbench.dag_factory import build_dags
ENVIRONMENT = json.loads('{"id":"local-gpu","name":"개발 GPU 워커","version":1,"backend":"http","connection_id":"workbench_model_worker","dag_prefix":"pop_talk_model_lab","pool":"model_lab_gpu","pool_slots":1,"capacity":1,"max_active_runs":1,"timeout_seconds":21600,"poll_seconds":3,"workloads":["diagnostic","llm_sft","embedding_contrastive"],"resource_profiles":{"diagnostic":"diagnostic","qlora":"qlora_8b","lora":"lora_small","full":"embedding_small"},"image":"","namespace":"default","service_account":"default","cpu":"2","memory":"8Gi","gpu":1,"node_selector":{}}')
for workflow in build_dags(ENVIRONMENT):
    globals()[workflow.dag_id] = workflow
