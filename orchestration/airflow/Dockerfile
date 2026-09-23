FROM apache/airflow:3.3.1-python3.12

USER root
RUN python -m venv /opt/dbt-venv \
    && chown -R airflow:0 /opt/dbt-venv
COPY orchestration/airflow/scripts/dbt_cosmos_runner.py /usr/local/bin/pop-talk-dbt-legacy
COPY orchestration/airflow/scripts/dbt_strict_runner.py /usr/local/bin/pop-talk-dbt-strict
RUN chmod 0555 /usr/local/bin/pop-talk-dbt-legacy /usr/local/bin/pop-talk-dbt-strict \
    && ln -s /usr/local/bin/pop-talk-dbt-legacy /usr/local/bin/pop-talk-dbt

USER airflow
COPY orchestration/airflow/requirements.txt /tmp/airflow-requirements.txt
COPY orchestration/airflow/dbt-requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
RUN /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt

WORKDIR /opt/airflow
