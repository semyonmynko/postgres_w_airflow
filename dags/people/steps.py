from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import nest_asyncio
nest_asyncio.apply()
import asyncio

from people.data_extractors import *

import logging
logger = logging.getLogger("airflow.task")

def get_connections():
    test_hook = PostgresHook(postgres_conn_id='test_conn')
    test_conn = test_hook.get_conn()
    test_cursor = test_conn.cursor()

    conns = {
        'test': {
            'hook': test_hook,
            'conn': test_conn,
            'cursor': test_cursor
        }
    }

    return conns

def fill_people():
  
    conns = get_connections()

    print('Getting data from CRM')
    people = asyncio.run(get_contacts_async())
    people = people.replace([np.nan], [None])
    people = people.rename(columns={'ID':'id', 'LAST_NAME':'surname', 'NAME':'name',
                                    'SECOND_NAME' : 'middle_name', 'EXPORT' : 'agreement'})
    people = people[['id','surname', 'name', 'middle_name', 'agreement']]

    print('Uploading to DB')
    
    conns['test']['hook'].insert_rows(
        table="public.people",
        replace=True,
        target_fields=[name for name in people.columns],
        replace_index=['id'],
        rows=[[cell for cell in row] for row in people.values]
    )

    pass

