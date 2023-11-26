from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from organizations.steps import *

dag_params = {
    'dag_id': 'organizations',
    'start_date': datetime(2023, 11, 25),
    'schedule_interval': '30 02 * * *',
    'max_active_runs': 1,
}

with DAG(**dag_params) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    fill_organizations_task = PythonOperator(
        task_id=f"fill_organizations_task",
        python_callable=fill_organizations,
    )

    start >> fill_organizations_task  >> end