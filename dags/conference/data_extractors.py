from fast_bitrix24 import Bitrix
import pandas as pd
import json
from fast_bitrix24 import Bitrix
from dotenv import load_dotenv
import os
import logging
import time

load_dotenv()
logger = logging.getLogger("airflow.task")

    
async def get_conference_list_async():

    webhook = os.getenv("WEB_HOOK")
    b = Bitrix(str(webhook))

    deals = await b.get_all('crm.deal.list',params={'filter': {'CATEGORY_ID': '2'}})
    deals_df = pd.DataFrame(deals)
    deals_df['title'] = deals_df['TITLE'].apply(lambda x: x.split('/')[0])
    deals_df['title'] = deals_df['title'].apply(lambda x: x.split(' ')[0] if x[-1] == ' ' else x)
    deals_df['title'] = deals_df['title'].apply(lambda x: x.replace('-',' '))
    deals_df['title'] = deals_df['title'].apply(lambda x: x.replace('Бельтиков',''))
    deals_df = deals_df[deals_df['title'].str.contains('Ваку')]
    deals_df = deals_df[deals_df['title'].str[0] == 'В']
    deals_df = deals_df[~deals_df['title'].str.contains('отмена')]
    deals_df = deals_df[['BEGINDATE', 'CLOSEDATE', 'title']]
    deals_df['start_date'] = deals_df['BEGINDATE'].apply(lambda x: x[0:10])
    deals_df['end_date'] = deals_df['CLOSEDATE'].apply(lambda x: x[0:10])
    deals_df['start_date'] = pd.to_datetime(deals_df['start_date'])
    deals_df['end_date'] = pd.to_datetime(deals_df['end_date'])
    conf_df = deals_df.groupby('title', as_index=False).agg({'start_date' : 'min', 'end_date' : 'max'})

    return conf_df