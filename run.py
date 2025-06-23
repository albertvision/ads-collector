import datetime
import time
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from datetime import date, datetime, timedelta
from tqdm import tqdm
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# --- CONFIGURATION ---

load_dotenv()

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
APP_ID = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")

BG_SERVICE_ACCOUNT_JSON = os.getenv("BG_SERVICE_ACCOUNT_JSON")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

# Timeframe
START_DATE = '2025-01-01'
END_DATE = '2025-01-02'
#END_DATE = date.today().isoformat()
OUTPUT_CSV = f"meta_ads_data_{START_DATE}_to_{END_DATE}"

# Init API
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# Init BigQuery client
bq_client = bigquery.Client.from_service_account_json(BG_SERVICE_ACCOUNT_JSON)
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

def get_dates_between(start_date, end_date):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.isoformat())
        current_date += timedelta(days=1)
    
    return date_list

# Rate-limit safe function
def safe_api_call(fetch_fn, max_retries=5, initial_wait=60):
    retries = 0
    while retries <= max_retries:
        try:
            return fetch_fn()
        except Exception as e:
            if 'rate' in str(e).lower():
                wait_time = initial_wait * (2 ** retries)  # exponential backoff
                print(f"Rate limit hit. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise

    raise Exception("Max retries exceeded due to rate limits.")

# Fetch insights from Meta Ads API
def fetch_ads_data(start_date, end_date):
    account = AdAccount(AD_ACCOUNT_ID)
    params = {
        'time_range': {
            'since': start_date,
            'until': end_date
        },
        'level': 'ad',
        'fields': [
            'account_id',
            'campaign_id',
            'campaign_name',
            'adset_id',
            'adset_name',
            'ad_id',
            'ad_name',
            'spend',
            'impressions',
            'clicks',
            'date_start',
            'date_stop'
        ],
        'limit': 1000,
    }

    data = []
    def get_data():
        return account.get_insights(params=params)

    insights = safe_api_call(get_data)

    while True:
        data += insights
        
        if not insights.load_next_page():
            break

    return data

def normalize_data(df):
    df['account_type'] = 'meta'
    df['account_id'] = pd.to_numeric(df['account_id'], errors='coerce')
    df['campaign_id'] = pd.to_numeric(df['campaign_id'], errors='coerce')
    df['adset_id'] = pd.to_numeric(df['adset_id'], errors='coerce')
    df['ad_id'] = pd.to_numeric(df['ad_id'], errors='coerce')
    df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
    df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce')
    df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce')
    df['date'] = pd.to_datetime(df['date_start'])
    
    df.drop(columns=['date_start', 'date_stop'], inplace=True)
    
    return df

def generate_bq_schema(df):
    dtype_map = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'DATE',
        'object': 'STRING'
    }
    
    return [
        bigquery.SchemaField(col, dtype_map.get(str(dtype), 'STRING'))
        for col, dtype in df.dtypes.items()
    ]
    
def upload_to_bigquery(df):
    schema=generate_bq_schema(df)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        # schema=schema
    )
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete
    print(f"Uploaded {len(df)} rows to BigQuery.")

# Main runner
if __name__ == '__main__':
    print(f"Fetching Meta Ads data from {START_DATE} to {END_DATE}...")
    
    # cast date ranges
    if isinstance(START_DATE, str):
        START_DATE = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    if isinstance(END_DATE, str):
        END_DATE = datetime.strptime(END_DATE, '%Y-%m-%d').date()
    
    data = []
    # get all days between start and end date
    for loop_date in tqdm(get_dates_between(START_DATE, END_DATE)):
        month_data = fetch_ads_data(loop_date, loop_date)
        # merge month_data into data
        data += month_data
    if(len(data) == 0):
        print("No data fetched. Exiting.")
        exit(0)
    data = pd.DataFrame(data)
    data = normalize_data(data)
    data.to_csv(f'{OUTPUT_CSV}.csv', index=False)
    data.to_excel(f'{OUTPUT_CSV}.xlsx', index=False)
    
    upload_to_bigquery(data)

    print(f"Data saved to {OUTPUT_CSV}")
