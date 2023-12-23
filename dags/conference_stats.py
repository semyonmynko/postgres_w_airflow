from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from conference.steps import *

dag_params = {
    'dag_id': 'conference',
    'start_date': datetime(2023, 11, 27),
    'schedule_interval': '30 02 * * *',
    'max_active_runs': 1,
}
print(fill_conference)
with DAG(**dag_params) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    fill_conference_task = PythonOperator(
        task_id=f"fill_conference_task",
        python_callable=fill_conference,
    )

    start >> fill_conference_task  >> end