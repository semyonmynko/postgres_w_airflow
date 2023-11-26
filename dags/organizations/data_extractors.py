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

    
async def get_organizations_list_async():

    webhook = os.getenv("WEB_HOOK")
    b = Bitrix(str(webhook))

    deals = await b.get_all('crm.deal.list',params={'filter': {'CATEGORY_ID': '2'}})
    deals_df = pd.DataFrame(deals)

    company_ids = list(deals_df['COMPANY_ID'].unique())
    companies = await b.get_all('crm.company.list')
    companies_df = pd.DataFrame(companies)    
    companies_df = companies_df[['ID', 'TITLE', 'COMPANY_TYPE']].query('ID in @company_ids')

    return companies_df