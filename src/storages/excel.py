import os
import logging

from .base import BaseStorage


logger = logging.getLogger(__name__)


class ExcelStorage(BaseStorage):
    name = "excel"

    def save(self, df, output_name: str) -> None:
        os.makedirs("dist", exist_ok=True)
        path = os.path.join("dist", f"{output_name}.xlsx")
        df.to_excel(path, index=False)
        logger.info("Saved Excel to %s", path)
