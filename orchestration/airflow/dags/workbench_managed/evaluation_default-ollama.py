from airflow_workbench.model_lab.evaluation_dag import build_dag
dag = build_dag({'id': 'default-ollama', 'name': '로컬 Ollama', 'provider': 'ollama', 'connection_id': '', 'capabilities': ['generation', 'embedding'], 'models': [], 'pool': 'model_lab_gpu', 'capacity': 1, 'reservation_environment': 'workbench_model_worker', 'timeout_seconds': 600, 'version': 1})
