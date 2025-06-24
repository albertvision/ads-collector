import os

from .base import BaseStorage


class ExcelStorage(BaseStorage):
    name = "excel"

    def save(self, df, output_name: str) -> None:
        os.makedirs("dist", exist_ok=True)
        path = os.path.join("dist", f"{output_name}.xlsx")
        df.to_excel(path, index=False)
        print(f"Saved Excel to {path}")
