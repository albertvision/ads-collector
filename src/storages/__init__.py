from .base import BaseStorage
from .csv_storage import CSVStorage
from .excel import ExcelStorage
from .bigquery import BigQueryStorage
from .mysql import MySQLStorage

STORAGE_CLASSES = {
    CSVStorage.name: CSVStorage,
    ExcelStorage.name: ExcelStorage,
    BigQueryStorage.name: BigQueryStorage,
    MySQLStorage.name: MySQLStorage,
}

__all__ = [
    "BaseStorage",
    "CSVStorage",
    "ExcelStorage",
    "BigQueryStorage",
    "MySQLStorage",
    "STORAGE_CLASSES",
]
