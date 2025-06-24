import pandas as pd
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import argparse

from providers import PROVIDER_CLASSES
from storages import STORAGE_CLASSES

# --- CONFIGURATION ---

load_dotenv()


DEFAULT_DATE = (date.today() - timedelta(days=1)).isoformat()


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
    parser.add_argument(
        "--storages",
        default="csv",
        help="Comma separated list of storage backends (csv,excel,bigquery,mysql) (default: csv)",
    )
    args = parser.parse_args()
    AD_PROVIDERS = [p.strip() for p in args.providers.split(',') if p.strip()]
    START_DATE = args.start_date
    STORAGE_NAMES = [s.strip() for s in args.storages.split(",") if s.strip()]
    STORAGES = []
    for sname in STORAGE_NAMES:
        cls = STORAGE_CLASSES.get(sname)
        if not cls:
            print(f"Unknown storage: {sname}")
            continue
        STORAGES.append(cls())
    if not STORAGES:
        print("No valid storage services specified. Exiting.")
        exit(1)
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

    for storage in STORAGES:
        storage.save(data, OUTPUT_CSV)

    print(f"Data saved to {OUTPUT_CSV}")
