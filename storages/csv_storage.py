from .base import BaseStorage


class CSVStorage(BaseStorage):
    name = "csv"

    def save(self, df, output_name: str) -> None:
        df.to_csv(f"{output_name}.csv", index=False)
        print(f"Saved CSV to {output_name}.csv")
