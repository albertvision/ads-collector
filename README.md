# Ads Collector

Ads Collector is a Python script that retrieves advertising insights from the Meta Ads (Facebook) API and uploads the results to Google BigQuery. The tool pulls data for each day in a configured date range and saves it as CSV and Excel files in addition to loading it into BigQuery.

## Features
- Connects to the Meta Ads API using credentials stored in a `.env` file.
- Iterates through a date range to fetch ad level metrics such as impressions, clicks and spend.
- Handles API rate limits with an exponential backoff strategy.
- Normalizes numeric fields and timestamps before saving.
- Loads the final dataset into a BigQuery table and writes CSV/Excel copies locally.

## Configuration
Create a `.env` file in the project directory with the following variables:

```
META_ACCESS_TOKEN=<your Meta access token>
META_AD_ACCOUNT_ID=<ad account id>
META_APP_ID=<app id>
META_APP_SECRET=<app secret>
BQ_DATASET=<BigQuery dataset name>
BQ_TABLE=<BigQuery table name>
```

Provide a Google Cloud service account JSON key and update the path in `run.py` if needed. You can also adjust `START_DATE` and `END_DATE` in the script to define the period to collect.

## Usage
Install the dependencies and run the script:

```
pip install -r requirements.txt
python run.py
```

The script will export `meta_ads_data_<start>_to_<end>.csv` and `.xlsx` files and append the data to the configured BigQuery table.
