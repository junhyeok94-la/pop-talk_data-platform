FROM apache/airflow:3.3.1-python3.12

USER airflow
COPY orchestration/airflow/requirements.txt /tmp/airflow-requirements.txt
# The upstream image bundles warehouse packages that Phase 1 does not use.
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt \
    && pip uninstall -y apache-airflow-providers-snowflake \
        snowflake-snowpark-python snowflake-sqlalchemy snowflake-connector-python \
    && pip check

WORKDIR /opt/airflow
