import os
import logging
import mysql.connector

from .base import BaseStorage


logger = logging.getLogger(__name__)


class MySQLStorage(BaseStorage):
    name = "mysql"

    def __init__(self):
        self.host = os.getenv("MYSQL_HOST")
        self.user = os.getenv("MYSQL_USER")
        self.password = os.getenv("MYSQL_PASSWORD")
        self.database = os.getenv("MYSQL_DATABASE")
        self.table = os.getenv("MYSQL_TABLE", "ads_data")

    def save(self, df, output_name: str) -> None:
        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        cursor = conn.cursor()
        insert_query = f"""
            INSERT IGNORE INTO {self.table} (
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
        logger.info("Uploaded %s rows to MySQL.", cursor.rowcount)
        cursor.close()
        conn.close()
