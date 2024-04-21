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

    
async def get_participants_list_async():

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
    conf_df['id'] = conf_df['start_date'].rank(ascending=True).astype(int)

    deals_df = pd.DataFrame(deals)
    participant_df = deals_df[['TITLE', 'CONTACT_ID']]
    participant_df['title'] = participant_df['TITLE'].apply(lambda x: x.split('/')[0])
    participant_df['title'] = participant_df['title'].apply(lambda x: x.split(' ')[0] if x[-1] == ' ' else x)
    participant_df['title'] = participant_df['title'].apply(lambda x: x.replace('-',' '))
    participant_df['title'] = participant_df['title'].apply(lambda x: x.replace('Бельтиков',''))
    participant_df = participant_df[participant_df['title'].str.contains('Ваку')]
    participant_df = participant_df[participant_df['title'].str[0] == 'В']
    participant_df = participant_df[~participant_df['title'].str.contains('отмена')]
    participant_df = conf_df.merge(participant_df, on = 'title', how='outer')
    participant_df = participant_df[['id', 'CONTACT_ID']]
    participant_df = participant_df.rename(columns={'id' : 'conference_id', 'CONTACT_ID' : 'person_id'})
    participant_df = participant_df.drop_duplicates()
    participant_df = participant_df.dropna()
    participant_df['id'] = participant_df.sort_values(by=['conference_id', 'person_id']).index
    participant_df = participant_df[['id', 'conference_id', 'person_id']]


    return participant_df