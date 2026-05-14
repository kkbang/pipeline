FROM apache/airflow:2.10.0-python3.11

COPY airflow/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
COPY airflow/config /opt/airflow/config
COPY airflow/dags /opt/airflow/dags
COPY airflow/plugins /opt/airflow/plugins
COPY worker /opt/airflow/worker
COPY scripts /opt/airflow/scripts
COPY infra /opt/airflow/infra

WORKDIR /opt/airflow
