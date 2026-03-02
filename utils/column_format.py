import os
from typing import Dict
import pandas as pd

from utils.base_format import BaseFormat
from model.StorageModel import StorageModel
from model.ColumnModel import Column


class ColumnFormat(BaseFormat):
    """Column-oriented file I/O engine. Each column → one .col file."""

    FORMAT_NAME = "column"

    def write(self, df: pd.DataFrame, db_path: str, metadata: dict) -> dict:
        os.makedirs(db_path, exist_ok=True)

        columns = []
        for col in df.columns:
            file_path = os.path.join(db_path, f"{col}.col")
            df[col].astype(str).to_csv(file_path, index=False, header=False)
            columns.append(col)

        metadata.update({
            "columns": columns
        })

        print(f"[ColumnFormat] Wrote {len(df)} rows × {len(columns)} columns → '{db_path}'")
        return metadata

    def write_units(self, units: Dict[str, StorageModel], db_path: str) -> None:
        os.makedirs(db_path, exist_ok=True)

        for name, unit in units.items():
            file_path = os.path.join(db_path, f"{name}.col")
            with open(file_path, "w", encoding="utf-8") as f:
                for value in unit.scan():
                    f.write(str(value) + "\n")

        print(f"[ColumnFormat] Wrote {len(units)} columns → '{db_path}'")

    def read_units(self, db_path: str, schema: Dict[str, type]) -> Dict[str, StorageModel]:
        """
        Load all columns from disk using schema.
        Returns {column_name: Column()}
        """
        units: Dict[str, StorageModel] = {}

        for name, dtype in schema.items():
            path = os.path.join(db_path, f"{name}.col")
            if not os.path.exists(path):
                raise FileNotFoundError(f"Missing column file: {path}")

            col = Column(name, dtype)

            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    value = line.strip()
                    if value != "":
                        try:
                            col.append(dtype(value))
                        except Exception:
                            col.append(value)  # safe fallback

            units[name] = col

        print(f"[ColumnFormat] Loaded {len(units)} columns from '{db_path}'")
        return units

    def read_column(self, db_path: str, column_name: str) -> list:
        path = os.path.join(db_path, f"{column_name}.col")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Column file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = [line.strip() for line in f if line.strip() != ""]

        print(f"[ColumnFormat] Read column '{column_name}' ({len(data)} rows)")
        return data

    def read_row(self, db_path: str, row_index: int) -> dict:
        """Column-store row access — O(num_columns)."""
        row = {}

        for file in os.listdir(db_path):
            if file.endswith(".col"):
                col_name = file[:-4]
                path = os.path.join(db_path, file)

                with open(path, "r", encoding="utf-8") as f:
                    for i, line in enumerate(f):
                        if i == row_index:
                            row[col_name] = line.strip()
                            break
                    else:
                        raise IndexError(f"Row index {row_index} out of range for column '{col_name}'")

        return row

    def read_head(self, db_path: str) -> Dict[str, list]:
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database directory not found: {db_path}")

        column_data = {}

        for file in sorted(os.listdir(db_path)):
            if file.endswith(".col"):
                col_name = file[:-4]
                column_data[col_name] = self.read_column(db_path, col_name)

        print(f"[ColumnFormat] read_head loaded {len(column_data)} columns from '{db_path}'")
        return column_data

    def make_unit(self, name: str, dtype: type) -> Column:
        """Factory: create correct unit type for this engine."""
        return Column(name, dtype)