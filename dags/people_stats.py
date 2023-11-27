from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from people.steps import *

dag_params = {
    'dag_id': 'people',
    'start_date': datetime(2023, 11, 27),
    'schedule_interval': '30 02 * * *',
    'max_active_runs': 1,
}
print(fill_people)
with DAG(**dag_params) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    fill_people_task = PythonOperator(
        task_id=f"fill_people_task",
        python_callable=fill_people,
    )

    start >> fill_people_task  >> end