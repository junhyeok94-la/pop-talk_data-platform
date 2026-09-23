# 빠른 제어/자원 진단 이미지. 이 stage는 학습 라이브러리를 설치하지 않는다.
FROM pop-talk-airflow:3.3.1-local AS control
USER airflow
ENV PYTHONPATH=/opt/airflow/plugins:/app HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
WORKDIR /app
ENTRYPOINT []
CMD ["python", "-m", "uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8091", "--workers", "1"]

# 검토된 학습 가중치/데이터와 충분한 메모리를 준비한 환경에서 별도로 빌드한다.
FROM pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime AS training
RUN pip install --no-cache-dir fastapi==0.116.1 uvicorn==0.35.0 pydantic==2.11.7 \
    transformers==4.56.2 peft==0.17.1 accelerate==1.10.1 bitsandbytes==0.47.0 \
    sentence-transformers==5.1.0 datasets==4.1.0
RUN pip install --no-cache-dir httpx==0.28.1
ENV PYTHONPATH=/opt/airflow/plugins:/app HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
WORKDIR /app
ENTRYPOINT []
CMD ["python", "-m", "uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8091", "--workers", "1"]

# Portable Pod entrypoint; service-account IAM supplies object-store access.
FROM training AS batch
RUN pip install --no-cache-dir fsspec==2025.9.0 s3fs==2025.9.0 gcsfs==2025.9.0
COPY orchestration/model_worker /app
COPY orchestration/airflow/plugins/airflow_workbench/__init__.py /opt/airflow/plugins/airflow_workbench/
COPY orchestration/airflow/plugins/airflow_workbench/shared/__init__.py orchestration/airflow/plugins/airflow_workbench/shared/contracts.py /opt/airflow/plugins/airflow_workbench/shared/
COPY orchestration/airflow/plugins/airflow_workbench/model_lab/__init__.py orchestration/airflow/plugins/airflow_workbench/model_lab/schemas.py orchestration/airflow/plugins/airflow_workbench/model_lab/training.py orchestration/airflow/plugins/airflow_workbench/model_lab/portable_training.py /opt/airflow/plugins/airflow_workbench/model_lab/
ENV HF_HUB_OFFLINE=0 TRANSFORMERS_OFFLINE=0
CMD ["python", "/app/batch_entrypoint.py"]
