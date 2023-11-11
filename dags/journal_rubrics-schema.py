from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, Date, DateTime, Time, Float, Integer, Interval, MetaData, String, ARRAY, UniqueConstraint, Boolean, LargeBinary
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def init_schema():
    db_hook = PostgresHook(postgres_conn_id='test_conn')
    db_engine = db_hook.get_sqlalchemy_engine()
    db_connection = db_hook.get_conn()
    db_session = sessionmaker(bind=db_connection)()

    meta = MetaData(db_engine).reflect()
    base_class = declarative_base(metadata=meta)

    class Journal_rubrics(base_class):
        __tablename__ = 'journal_rubrics'
        __table_args__ = (
            UniqueConstraint('journal_issue_id', 'title', 
                             name='uix_journal_title'),
            {'schema': 'public'}
        )
                        
        id = Column('id', Integer, primary_key=True, autoincrement=True)
        journal_issue_id = Column('journal_issue_id', Integer)
        title = Column('title', String)
        

    if not db_engine.dialect.has_table(db_engine, Journal_rubrics.__tablename__):
        Journal_rubrics.metadata.create_all(bind=db_engine)
        db_session.commit()

    db_session.close()
    db_connection.close()


dag_params = {
    'dag_id': 'journal_rubrics-schema',
    'start_date': datetime(2023, 7, 1),
    'schedule_interval': '@once',
}

with DAG(**dag_params) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    init_task = PythonOperator(
        task_id=f"init",
        python_callable=init_schema,
    )
    start >> init_task >> end