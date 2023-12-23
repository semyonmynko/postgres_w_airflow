from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import nest_asyncio
nest_asyncio.apply()
import asyncio

from workers.data_extractors import *

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

def fill_workers():
  
    conns = get_connections()

    print('Getting data from CRM')
    workers = asyncio.run(get_workers_list_async())
    workers = workers.replace([np.nan], [None])
    workers = workers.rename(columns={'ID':'people_id', 'COMPANY_ID':'organization_id'})
    workers['people_id'] = workers['people_id'].astype('int')

    print('Uploading to DB')
    
    conns['test']['hook'].insert_rows(
        table="public.workers",
        replace=True,
        target_fields=[name for name in workers.columns],
        replace_index=['people_id'],
        rows=[[cell for cell in row] for row in workers.values]
    )

    pass

