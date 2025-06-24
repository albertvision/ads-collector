import os

from .base import BaseStorage


class CSVStorage(BaseStorage):
    name = "csv"

    def save(self, df, output_name: str) -> None:
        os.makedirs("dist", exist_ok=True)
        path = os.path.join("dist", f"{output_name}.csv")
        df.to_csv(path, index=False)
        print(f"Saved CSV to {path}")
