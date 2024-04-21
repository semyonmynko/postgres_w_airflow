from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import nest_asyncio
nest_asyncio.apply()
import asyncio

from conference.data_extractors import *

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

def fill_conference():
  
    conns = get_connections()

    print('Getting data from CRM')
    conference = asyncio.run(get_conference_list_async())
    conference = conference.replace([np.nan], [None])

    print('Uploading to DB')
    
    conns['test']['hook'].insert_rows(
        table="public.conference",
        replace=True,
        target_fields=[name for name in conference.columns],
        replace_index=['id'],
        rows=[[cell for cell in row] for row in conference.values]
    )

    pass

