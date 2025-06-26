import os
import logging

from .base import BaseStorage


logger = logging.getLogger(__name__)


class CSVStorage(BaseStorage):
    name = "csv"

    def save(self, df, output_name: str) -> None:
        os.makedirs("dist", exist_ok=True)
        path = os.path.join("dist", f"{output_name}.csv")
        df.to_csv(path, index=False)
        logger.info("Saved CSV to %s", path)
