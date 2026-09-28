FROM apache/airflow:3.1.7-python3.12

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.1.7" \
    -r /tmp/requirements.txt

WORKDIR /opt/project

COPY --chown=airflow:root config /opt/project/config
COPY --chown=airflow:root src /opt/project/src
COPY --chown=airflow:root dags /opt/project/dags
COPY --chown=airflow:root tests /opt/project/tests

ENV PYTHONPATH=/opt/project/src
