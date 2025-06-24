# Ads Collector

Ads Collector is a Python script that retrieves advertising insights from the Meta Ads (Facebook) API, Google Ads and other providers. It uploads the results to Google BigQuery and a MySQL database. The tool pulls data for each day in a configured date range and saves it as CSV and Excel files in addition to loading it into BigQuery and MySQL.

## Features
- Connects to the Meta Ads and Google Ads APIs using credentials stored in a `.env` file.
- Iterates through a date range to fetch ad level metrics such as impressions, clicks and spend.
- Handles API rate limits with an exponential backoff strategy.
- Normalizes numeric fields and timestamps before saving.
- Supports multiple storage backends (CSV, Excel, BigQuery, MySQL) configured via the `STORAGES` environment variable.
- Stores the data in a MySQL table while skipping records that already exist. The table schema is managed via migrations.
- Easily extendable to additional advertising providers.
- Providers are implemented as classes in the `providers` package.

## Configuration
Create a `.env` file in the project directory with the following variables:

```
META_ACCESS_TOKEN=<your Meta access token>
META_AD_ACCOUNT_ID=<ad account id>
META_APP_ID=<app id>
META_APP_SECRET=<app secret>
BG_SERVICE_ACCOUNT_JSON=<BQ service account JSON>
BQ_DATASET=<BigQuery dataset name>
BQ_TABLE=<BigQuery table name>
MYSQL_HOST=<MySQL host>
MYSQL_USER=<MySQL user>
MYSQL_PASSWORD=<MySQL password>
MYSQL_DATABASE=<MySQL database name>
MYSQL_TABLE=<MySQL table name>  # optional, defaults to 'ads_data'
GOOGLEADS_CONFIG=google-ads.yaml
GOOGLEADS_CUSTOMER_ID=<google customer id>
AD_PROVIDERS=meta,google
STORAGES=csv,excel,bigquery,mysql
```

Provide a Google Cloud service account JSON key and update the path in `run.py` if needed. Specify the collection period with the `--start-date` and `--end-date` arguments when running the script. Both default to yesterday.

## Usage
Install the dependencies and run the script:

```
pip install -r requirements.txt
python migrate.py  # run once to create/update tables
python run.py --start-date 2023-01-01 --end-date 2023-01-02

Providers listed in `AD_PROVIDERS` map to classes in the `providers` package.

The script saves results to the storage backends listed in `STORAGES`.

### Running MySQL with Docker Compose

The repository includes a `docker-compose.yml` file for starting a local MySQL
server. Bring up the database with:

```bash
docker compose up -d
```

This launches MySQL 8 and stores the data in a named volume `mysql_data`.
Docker Compose reads the connection variables from your `.env` file so the
container uses the same settings as `run.py`. The service falls back to the
following defaults if a variable is not provided:

- database: `${MYSQL_DATABASE:-ads}`
- user: `${MYSQL_USER:-ads}`
- password: `${MYSQL_PASSWORD:-ads_password}`
- root password: `${MYSQL_ROOT_PASSWORD:-root}`

Set matching values in your `.env` file so `run.py` can connect.
