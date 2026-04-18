# model/TableModel.py

from typing import Dict, List
from model.StorageModel import StorageModel
from model.UnitModel import UnitModel
from utils.metadata import MetaLoader
from utils.helpers import Helpers


class Table:
    """
    A collection of StorageModel units (columns or rows).
    Delegates all file I/O to a StorageModel engine.
    """

    def __init__(self, engine: StorageModel, name: str = None):
        self.engine = engine
        self.name = name
        self.storage_units: Dict[str, StorageModel] = {}
        self.sorted_columns: List = []

    def add_unit(self, name: str, unit: StorageModel) -> None:
        self.storage_units[name] = unit

    def get_unit(self, name: str) -> StorageModel:
        if name not in self.storage_units:
            raise KeyError(f"Storage unit '{name}' not found.")
        return self.storage_units[name]

    def create_unit(self, name: str, dtype: type = str) -> None:
        self.storage_units[name] = UnitModel.create(name, dtype)

    def get_rows(self, indexes: list) -> list[dict]:
        """Efficient row retrieval using cached column scans."""
        column_cache = {
            name: unit.scan()
            for name, unit in self.storage_units.items()
        }

        return [
            {col: column_cache[col][i] for col in column_cache}
            for i in indexes
        ]

    def insert(self, row: dict) -> None:
        for field, value in row.items():
            if field not in self.storage_units:
                raise KeyError(f"Field '{field}' not in table schema.")
            self.storage_units[field].append(value)

    def save(self) -> None:
        self.engine.write_units(self.storage_units)

    def load(self) -> "Table":
        try:
            column_data: Dict[str, list] = self.engine.read()

            meta = {}
            db_path = getattr(self.engine, "db_path", None)
            if db_path:
                try:
                    meta = MetaLoader.load(db_path)
                except FileNotFoundError:
                    meta = {}
                except Exception:
                    meta = {}

            sorted_columns = meta.get("sorted_columns", [])

            for col_name, raw_values in column_data.items():
                dtype = Helpers._infer_dtype(raw_values)
                unit = UnitModel.create(col_name, dtype)
                unit.data = [Helpers._safe_cast(v, dtype) for v in raw_values]
                self.storage_units[col_name] = unit
                self.sorted_columns =  sorted_columns

            return self

        except Exception as e:
            print(f"Error loading table from '{self.engine.db_path}': {e}")
            return self