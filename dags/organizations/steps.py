from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np
import nest_asyncio
nest_asyncio.apply()
import asyncio

from organizations.data_extractors import *

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

def fill_organizations():
  
    conns = get_connections()

    print('Getting data from CRM')
    organizations = asyncio.run(get_organizations_list_async())
    organizations = organizations.replace([np.nan], [None])
    organizations = organizations.rename(columns={'ID':'id', 'TITLE':'title', 'COMPANY_TYPE':'attribute'})
    organizations['id'] = organizations['id'].astype(int)

    print('Uploading to DB')
    
    conns['test']['hook'].insert_rows(
        table="public.organizations",
        replace=True,
        target_fields=[name for name in organizations.columns],
        replace_index=['id'],
        rows=[[cell for cell in row] for row in organizations.values]
    )

    pass

