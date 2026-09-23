ARG AIRFLOW_BASE=apache/airflow:3.3.1-python3.12@sha256:b01a795dfbd113bbbfdf3ee169b8f27e9a0090ccef105f1a452b3594a11ed316
FROM ${AIRFLOW_BASE}
USER airflow
COPY requirements.txt /tmp/workbench-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "${HOME}/constraints.txt" -r /tmp/workbench-requirements.txt && pip check
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root scripts/ /opt/workbench/scripts/
COPY --chown=airflow:root config/ /opt/workbench/config/
ENV PYTHONPATH=/opt/airflow/plugins
LABEL org.opencontainers.image.title="Airflow Workbench" \
      org.opencontainers.image.description="Dashboard, monitoring and Model Lab for Apache Airflow" \
      org.opencontainers.image.version="0.1.2-dev"
# Keep the official entrypoint. Migrations run once through the init service.
