FROM apache/airflow:2.2.4-python3.8 

RUN pip install -U pip poetry
COPY poetry.lock pyproject.toml ./
RUN poetry export --without-hashes -f requirements.txt -o /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN pip install celery
RUN pip install setuptools

COPY --chown=airflow:root dags /opt/airflow/dags
