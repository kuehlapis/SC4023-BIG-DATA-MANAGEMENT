# model/ColumnModel.py

from typing import Dict

from model.StorageModel import StorageModel
from utils.column_format import ColumnFormat
import pandas as pd


class Column(StorageModel):
    """A single typed column of in-memory data."""

    def __init__(self, dtype: type = str, db_path: str = None):
        self.dtype = dtype
        self.columnformat = ColumnFormat(db_path)
        self.db_path = db_path
        self.data: list = []

    def append(self, value) -> None:
        self.data.append(self.dtype(value))

    def scan(self) -> list:
        """Return entire column data."""
        return self.data
    
    def write(self, df: pd.DataFrame, metadata: dict) -> None:
        """Write this column as a .col file using ColumnFormat."""
        self.columnformat.write(df, metadata)
    
    def write_units(self, units: Dict[str, StorageModel]) -> None:
        """Write this column as a .col file using ColumnFormat."""
        self.columnformat.write_units(units)

    def format_name(self) -> str:
        return self.columnformat.FORMAT_NAME
    
    def read(self) -> Dict[str, list]:
        """Read this column from a .col file using ColumnFormat."""
        return self.columnformat.read()

