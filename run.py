import datetime
import time
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.ads.googleads.client import GoogleAdsClient
from datetime import date, datetime, timedelta
from tqdm import tqdm
from google.cloud import bigquery
import mysql.connector
import os
from dotenv import load_dotenv
import argparse

# --- CONFIGURATION ---

load_dotenv()

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
APP_ID = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")

GOOGLEADS_CONFIG = os.getenv("GOOGLEADS_CONFIG")
GOOGLEADS_CUSTOMER_ID = os.getenv("GOOGLEADS_CUSTOMER_ID")

BG_SERVICE_ACCOUNT_JSON = os.getenv("BG_SERVICE_ACCOUNT_JSON")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "ads_data")

# Timeframe
START_DATE = '2025-01-01'
END_DATE = '2025-01-02'
#END_DATE = date.today().isoformat()
OUTPUT_CSV = f"ads_data_{START_DATE}_to_{END_DATE}"

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
def fetch_meta_ads_data(start_date, end_date):
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

# Fetch insights from Google Ads API
def fetch_google_ads_data(start_date, end_date):
    if not GOOGLEADS_CONFIG or not GOOGLEADS_CUSTOMER_ID:
        raise RuntimeError("Google Ads configuration missing")

    client = GoogleAdsClient.load_from_storage(GOOGLEADS_CONFIG)
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            customer.id,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            segments.date
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
    """

    stream = ga_service.search_stream(
        customer_id=GOOGLEADS_CUSTOMER_ID, query=query
    )
    results = []
    for batch in stream:
        for row in batch.results:
            results.append({
                'account_id': row.customer.id,
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'adset_id': row.ad_group.id,
                'adset_name': row.ad_group.name,
                'ad_id': row.ad_group_ad.ad.id,
                'ad_name': row.ad_group_ad.ad.name,
                'spend': row.metrics.cost_micros / 1_000_000,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'date_start': str(row.segments.date),
                'date_stop': str(row.segments.date),
            })
    return results

def normalize_data(df, provider):
    df['account_type'] = provider
    df['account_id'] = pd.to_numeric(df['account_id'], errors='coerce')
    df['campaign_id'] = pd.to_numeric(df['campaign_id'], errors='coerce')
    df['adset_id'] = pd.to_numeric(df['adset_id'], errors='coerce')
    df['ad_id'] = pd.to_numeric(df['ad_id'], errors='coerce')
    df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
    df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce')
    df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce')
    if 'date_start' in df.columns:
        df['date'] = pd.to_datetime(df['date_start'])
    else:
        df['date'] = pd.to_datetime(df['date'])

    for col in ['date_start', 'date_stop']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    
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

def connect_mysql():
    """Create a MySQL connection using .env settings."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def upload_to_mysql(df, conn):
    cursor = conn.cursor()


    insert_query = f"""
        INSERT IGNORE INTO {MYSQL_TABLE} (
            account_type, account_id, campaign_id, campaign_name,
            adset_id, adset_name, ad_id, ad_name, spend,
            impressions, clicks, date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = df[[
        'account_type', 'account_id', 'campaign_id', 'campaign_name',
        'adset_id', 'adset_name', 'ad_id', 'ad_name', 'spend',
        'impressions', 'clicks', 'date'
    ]].values.tolist()

    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f"Uploaded {cursor.rowcount} rows to MySQL.")
    cursor.close()

# Main runner
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Collect advertising data")
    parser.add_argument(
        "--providers",
        required=True,
        help="Comma separated list of ad providers (e.g. meta,google)",
    )
    args = parser.parse_args()
    AD_PROVIDERS = [p.strip() for p in args.providers.split(',') if p.strip()]

    print(
        f"Fetching ads data from {START_DATE} to {END_DATE} for {', '.join(AD_PROVIDERS)}..."
    )
    
    # cast date ranges
    if isinstance(START_DATE, str):
        START_DATE = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    if isinstance(END_DATE, str):
        END_DATE = datetime.strptime(END_DATE, '%Y-%m-%d').date()
    
    data_frames = []

    for provider in AD_PROVIDERS:
        provider_data = []
        if provider == 'meta':
            for loop_date in tqdm(get_dates_between(START_DATE, END_DATE)):
                provider_data += fetch_meta_ads_data(loop_date, loop_date)
        elif provider == 'google':
            provider_data += fetch_google_ads_data(START_DATE, END_DATE)
        else:
            print(f"Unknown provider: {provider}")
            continue

        if provider_data:
            df = pd.DataFrame(provider_data)
            df = normalize_data(df, provider)
            data_frames.append(df)

    if not data_frames:
        print("No data fetched. Exiting.")
        exit(0)

    data = pd.concat(data_frames, ignore_index=True)
    data = data.sort_values('date')
    data.to_csv(f'{OUTPUT_CSV}.csv', index=False)
    data.to_excel(f'{OUTPUT_CSV}.xlsx', index=False)

    upload_to_bigquery(data)
    conn = connect_mysql()
    try:
        upload_to_mysql(data, conn)
    finally:
        conn.close()

    print(f"Data saved to {OUTPUT_CSV}")
