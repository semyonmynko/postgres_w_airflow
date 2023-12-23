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

    
async def get_workers_list_async():

    webhook = os.getenv("WEB_HOOK")
    b = Bitrix(str(webhook))

    deals = await b.get_all('crm.deal.list',params={'filter': {'CATEGORY_ID': '2'}})
    deals_df = pd.DataFrame(deals)
    contact_id = list(deals_df['CONTACT_ID'].unique())
    contacts = await b.get_all('crm.contact.list')
    contacts_df = pd.DataFrame(contacts)
    contacts_df = contacts_df[['ID', 'COMPANY_ID']].query('ID in @contact_id')

    return contacts_df