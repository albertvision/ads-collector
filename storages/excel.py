from .base import BaseStorage


class ExcelStorage(BaseStorage):
    name = "excel"

    def save(self, df, output_name: str) -> None:
        df.to_excel(f"{output_name}.xlsx", index=False)
        print(f"Saved Excel to {output_name}.xlsx")
