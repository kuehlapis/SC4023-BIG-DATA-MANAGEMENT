# model/TableModel.py

from typing import Dict, List
from model.StorageModel import StorageModel
from model.UnitModel import UnitModel
from utils.metadata import MetaLoader
from utils.helpers import Helpers
from optimization.BitmapIndex import BitmapIndex
from optimization.ZoneMap import ZoneMap


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
        # zonemaps: column -> ZoneMap
        self.zonemaps: Dict[str, ZoneMap] = {}
        # bitmap_indexes holds raw base64 strings loaded from metadata.
        # Use `get_bitmap(col, val)` to obtain a decoded BitmapIndex (cached in-memory).
        self.bitmap_indexes: Dict[str, Dict[str, str]] = {}
        self._bitmap_cache: Dict[str, Dict[str, BitmapIndex]] = {}

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

            # Load bitmap indexes metadata (store base64 strings; decode lazily)
            bitmap_meta = meta.get("bitmap_indexes", {})
            for col, value_map in bitmap_meta.items():
                self.bitmap_indexes[col] = {}
                self._bitmap_cache[col] = {}
                for val, b64 in value_map.items():
                    try:
                        # keep the serialized form; decoding happens on demand
                        self.bitmap_indexes[col][val] = b64
                    except Exception:
                        # Ignore corrupted or incompatible bitmap entries
                        continue

            # Load zonemap metadata (deserialize into ZoneMap instances)
            zonemap_meta = meta.get("zonemaps", {})
            for col, zm_d in zonemap_meta.items():
                try:
                    self.zonemaps[col] = ZoneMap.from_dict(zm_d)
                except Exception:
                    # ignore malformed zonemap entries
                    continue

            return self

        except Exception as e:
            print(f"Error loading table from '{self.engine.db_path}': {e}")
            return self

    def get_bitmap(self, column: str, value) -> BitmapIndex | None:
        """Get decoded BitmapIndex for a column value (cached in-memory).
        
        Args:
            column: Column name
            value: Value to get bitmap for (must match key in bitmap_indexes)
            
        Returns:
            BitmapIndex if available, None otherwise
        """
        # Check cache first
        if column in self._bitmap_cache and value in self._bitmap_cache[column]:
            return self._bitmap_cache[column][value]
        
        # Check if bitmap exists in metadata
        if column not in self.bitmap_indexes:
            return None
        
        if value not in self.bitmap_indexes[column]:
            return None
        
        # Decode from base64
        try:
            b64 = self.bitmap_indexes[column][value]
            bitmap = BitmapIndex.from_base64(b64)
            # Cache it
            if column not in self._bitmap_cache:
                self._bitmap_cache[column] = {}
            self._bitmap_cache[column][value] = bitmap
            return bitmap
        except Exception as e:
            print(f"Warning: Failed to decode bitmap for {column}={value}: {e}")
            return None