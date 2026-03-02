# table.py

from typing import Dict
from model.StorageModel import StorageModel
from utils.base_format import BaseFormat


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
        self.storage_units[name] = unit

    def get_unit(self, name: str) -> StorageModel:
        if name not in self.storage_units:
            raise KeyError(f"Storage unit '{name}' not found.")
        return self.storage_units[name]

    def units(self) -> list:
        return list(self.storage_units.keys())

    def row_count(self) -> int:
        if not self.storage_units:
            return 0
        return len(next(iter(self.storage_units.values())).scan())

    def load(self, db_path: str) -> "Table":
        """Ask the engine to read all data, then populate storage units."""
        column_data: Dict[str, list] = self.engine.read_head(db_path)

        for col_name, raw_values in column_data.items():
            dtype = self._infer_dtype(raw_values)
            unit = self.engine.make_unit(col_name, dtype)

            for v in raw_values:
                unit.data.append(self._safe_cast(v, dtype))

            self.storage_units[col_name] = unit

        print(f"[{self.name}] Loaded columns: {self.units()}, rows: {self.row_count()}")
        return self

    def save(self, db_path: str) -> None:
        """Ask the engine to write all data."""
        self.engine.write_units(self.storage_units, db_path)

    def insert(self, row: dict) -> None:
        for field, value in row.items():
            if field not in self.storage_units:
                raise KeyError(f"Field '{field}' not in table schema.")
            self.storage_units[field].append(value)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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