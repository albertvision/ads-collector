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
- Loads environment variables from a `.env` file using `python-dotenv`.
- Logs progress and errors to stdout using Python's `logging` module.
- Providers are implemented as classes in the `providers` package.
- Automatically collects data from all Meta ad accounts available to the token, or a subset specified via `META_AD_ACCOUNT_ID`.

## Configuration
Copy `.env.example` to `.env` and fill in your credentials. The example file lists
all supported variables and recommended defaults.

Provide a Google Cloud service account JSON key and update the path in
`src/run.py` if needed. Specify the collection period with the `--start-date`
and `--end-date` arguments when running the script. Both default to yesterday.

## Usage
Install the dependencies and run the script:

```
pip install -r requirements.txt
python src/migrate.py  # run once to create/update tables
python src/run.py --start-date 2023-01-01 --end-date 2023-01-02
```

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
container uses the same settings as `src/run.py`. The service falls back to the
following defaults if a variable is not provided:

- database: `${MYSQL_DATABASE:-ads}`
- user: `${MYSQL_USER:-ads}`
- password: `${MYSQL_PASSWORD:-ads_password}`
- root password: `${MYSQL_ROOT_PASSWORD:-root}`

Set matching values in your `.env` file so `src/run.py` can connect.

### Running the Collector with Docker Compose

The compose file also defines a `collector` service for local development.
It builds the image from the Dockerfile, mounts the project directory and
connects to the `mysql` container. Start the stack with:

```bash
docker compose up --build collector
```

The collector uses the environment variables from your `.env` file and
automatically runs migrations before executing `src/run.py`.

### Building the Docker Image

Build the production image using the provided `Dockerfile`:

```bash
docker build -t ads-collector .
```

Then run the container with your environment variables:

```bash
docker run --env-file .env ads-collector
```

The entrypoint applies database migrations before starting the collector.

### Continuous Deployment

A GitHub Actions workflow builds and publishes the Docker image to GitHub Packages when changes are merged into the main branch. The image will be available at ghcr.io/<owner>/ads-collector.
