# table.py

from typing import Dict
from model.StorageModel import StorageModel
from model.ColumnModel import Column
from utils.base_format import BaseFormat
from model.UnitModel import UnitModel


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

    def insert(self, row: dict) -> None:
        """Insert a row into column storage."""
        for field, value in row.items():
            if field not in self.storage_units:
                raise KeyError(f"Field '{field}' not in table schema.")
            self.storage_units[field].append(value)

    def save(self, db_path: str) -> None:
        """Ask the engine to write all data."""
        self.engine.write_units(self.storage_units, db_path)

    def load(self, db_path: str) -> None:
        """Ask the engine to read all data and populate storage_units."""
        column_data: Dict[str, list] = self.engine.read_head(db_path)

        for col_name, raw_values in column_data.items():
            dtype = self._infer_dtype(raw_values)
            unit = UnitModel.create(col_name, dtype)
            for v in raw_values:
                unit.append(self._safe_cast(v, dtype))
            self.storage_units[col_name] = unit

        print(f"Loaded table '{self.name}' with columns: {list(self.storage_units.keys())}")
        return self
    
    @staticmethod
    def _infer_dtype(values: list) -> type:
        """
        Sample up to 100 non-empty values.
        Only picks int/float if ALL sampled values are that type.
        Falls back to str otherwise.
        """
        sample = [v for v in values if v != ""][:100]
        if not sample:
            return str

        if all(_is_int(v) for v in sample):
            return int

        if all(_is_float(v) for v in sample):
            return float

        return str

    @staticmethod
    def _safe_cast(value: str, dtype: type):
        """Cast value to dtype, fall back to str if it fails."""
        try:
            return dtype(value)
        except (ValueError, TypeError):
            return value


def _is_int(v) -> bool:
    try:
        int(v)
        return True
    except (ValueError, TypeError):
        return False


def _is_float(v) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False