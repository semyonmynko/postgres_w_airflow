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

    class Sections(base_class):
        __tablename__ = 'sections'
        __table_args__ = (
            UniqueConstraint('event_id', 'title', 'chief_id', name='uix_event_title_chief'),
            {'schema': 'public'}
        )
                        
        id = Column('id', Integer, primary_key=True, autoincrement=True)
        event_id = Column('event_id', Integer)
        title = Column('title', String)
        chief_id = Column('chief_id', Integer)
        

    if not db_engine.dialect.has_table(db_engine, Sections.__tablename__):
        Sections.metadata.create_all(bind=db_engine)
        db_session.commit()

    db_session.close()
    db_connection.close()


dag_params = {
    'dag_id': 'sections-schema',
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