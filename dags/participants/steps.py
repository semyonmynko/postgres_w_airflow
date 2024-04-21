from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import nest_asyncio
nest_asyncio.apply()
import asyncio

from participants.data_extractors import *

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

def fill_participants():
  
    conns = get_connections()

    print('Getting data from CRM')
    participants = asyncio.run(get_participants_list_async())
    participants = participants.replace([np.nan], [None])

    print('Uploading to DB')
    
    conns['test']['hook'].insert_rows(
        table="public.participants",
        replace=True,
        target_fields=[name for name in participants.columns],
        replace_index=['id'],
        rows=[[cell for cell in row] for row in participants.values]
    )

    pass

