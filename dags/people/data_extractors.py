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

    
async def get_contacts_async():

    webhook = os.getenv("WEB_HOOK")
    b = Bitrix(str(webhook))
    contacts = await b.get_all('crm.contact.list')
    contacts_df = pd.DataFrame(contacts)
    contact_df = contacts_df[['ID', 'NAME', 'SECOND_NAME', 'LAST_NAME', 'EXPORT']]
    contact_df['ID'] = contact_df['ID'].astype(int)

    return contacts_df