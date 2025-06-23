import pandas as pd
from datetime import date, datetime, timedelta
from google.cloud import bigquery
import mysql.connector
import os
from dotenv import load_dotenv
import argparse

from providers import PROVIDER_CLASSES

# --- CONFIGURATION ---

load_dotenv()


BG_SERVICE_ACCOUNT_JSON = os.getenv("BG_SERVICE_ACCOUNT_JSON")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "ads_data")

# Default timeframe
DEFAULT_DATE = (date.today() - timedelta(days=1)).isoformat()

# Init BigQuery client
bq_client = bigquery.Client.from_service_account_json(BG_SERVICE_ACCOUNT_JSON)
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

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
    parser.add_argument(
        "--start-date",
        default=DEFAULT_DATE,
        help="Start date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--end-date",
        default=DEFAULT_DATE,
        help="End date in YYYY-MM-DD format (default: today)",
    )
    args = parser.parse_args()
    AD_PROVIDERS = [p.strip() for p in args.providers.split(',') if p.strip()]
    START_DATE = args.start_date
    END_DATE = args.end_date
    OUTPUT_CSV = f"ads_data_{START_DATE}_to_{END_DATE}"

    print(
        f"Fetching ads data from {START_DATE} to {END_DATE} for {', '.join(AD_PROVIDERS)}..."
    )
    
    # cast date ranges
    if isinstance(START_DATE, str):
        START_DATE = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    if isinstance(END_DATE, str):
        END_DATE = datetime.strptime(END_DATE, '%Y-%m-%d').date()
    
    data_frames = []

    for provider_name in AD_PROVIDERS:
        provider_cls = PROVIDER_CLASSES.get(provider_name)
        if not provider_cls:
            print(f"Unknown provider: {provider_name}")
            continue

        provider = provider_cls()
        provider_data = provider.fetch_data(START_DATE, END_DATE)

        if provider_data:
            df = pd.DataFrame(provider_data)
            df = normalize_data(df, provider_name)
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
