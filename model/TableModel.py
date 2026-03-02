# table.py

from typing import Dict
from model.StorageModel import StorageModel
from model.ColumnModel import Column
from utils.base_format import BaseFormat
from model.UnitModel import UnitModel
from utils.helpers import Helpers


class Table:
    """
    A collection of StorageModel units (columns or rows).
    Delegates all file I/O to a BaseFormat engine.
    """

    def __init__(self, engine: BaseFormat, name: str = None):
        self.engine = engine
        self.name = name
        self.storage_units: Dict[str, StorageModel] = {}

    def add_unit(self, name: str, unit: StorageModel) -> None:
        """Add a storage unit (column/row)."""
        self.storage_units[name] = unit

    def get_unit(self, name: str) -> StorageModel:
        if name not in self.storage_units:
            raise KeyError(f"Storage unit '{name}' not found.")
        return self.storage_units[name]

    def create_unit(self, name: str, dtype: type = str) -> None:
        """Convenience method for column-store schema creation."""
        self.storage_units[name] = UnitModel.create(name, dtype)

    def get_rows(self, indexes: list) -> list[dict]:
        """Return rows as a list of dicts for the given indexes using scan()."""
        if not indexes:
            return []
        scanned = {
            col_name: col.scan(indexes)
            for col_name, col in self.storage_units.items()
        }
        col_names = list(scanned.keys())
        return [
            dict(zip(col_names, row))
            for row in zip(*[scanned[c] for c in col_names])
        ]

    def insert(self, row: dict) -> None:
        """Insert a row into column storage."""
        for field, value in row.items():
            if field not in self.storage_units:
                raise KeyError(f"Field '{field}' not in table schema.")
            self.storage_units[field].append(value)

    def save(self, db_path: str) -> None:
        """Ask the engine to write all data."""
        self.engine.write_units(self.storage_units, db_path)

    def load(self, db_path: str) -> "Table":
        """Ask the engine to read all data and populate storage_units."""
        column_data: Dict[str, list] = self.engine.read_head(db_path)
        for col_name, raw_values in column_data.items():
            dtype = Helpers._infer_dtype(raw_values)
            unit = UnitModel.create(col_name, dtype)
            for v in raw_values:
                unit.append(Helpers._safe_cast(v, dtype))
            self.storage_units[col_name] = unit
        return self