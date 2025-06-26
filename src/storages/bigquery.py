import os
import logging
from google.cloud import bigquery

from .base import BaseStorage


logger = logging.getLogger(__name__)


class BigQueryStorage(BaseStorage):
    name = "bigquery"

    def __init__(self):
        service_account_json = os.getenv("BG_SERVICE_ACCOUNT_JSON")
        dataset = os.getenv("BQ_DATASET")
        table = os.getenv("BQ_TABLE")
        self.client = bigquery.Client.from_service_account_json(service_account_json)
        self.table_ref = self.client.dataset(dataset).table(table)

    def _generate_schema(self, df):
        dtype_map = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'DATE',
            'object': 'STRING',
        }
        return [
            bigquery.SchemaField(col, dtype_map.get(str(dtype), 'STRING'))
            for col, dtype in df.dtypes.items()
        ]

    def save(self, df, output_name: str) -> None:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        job.result()
        logger.info("Uploaded %s rows", len(df))
