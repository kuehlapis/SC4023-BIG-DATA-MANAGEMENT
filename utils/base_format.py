from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd
from model.StorageModel import StorageModel


class BaseFormat(ABC):
    """
    Abstract storage engine.
    Subclass this for Column-oriented, Row-oriented, etc.
    """

    @abstractmethod
    def write(self, df: pd.DataFrame, db_path: str, metadata: dict) -> dict:
        """Persist a DataFrame to disk."""
        pass

    @abstractmethod
    def write_units(self, units: Dict[str, StorageModel], db_path: str) -> None:
        """Persist StorageModel units to disk."""
        pass

    @abstractmethod
    def read_column(self, db_path: str, column_name: str) -> list:
        """Read a single column by name."""
        pass

    @abstractmethod
    def read_row(self, db_path: str, row_index: int) -> dict:
        """Read a single row by index."""
        pass

    @abstractmethod
    def read_head(self, db_path: str) -> Dict[str, list]:
        """Read ALL data, returned as {column_name: [values]}."""
        pass

    @abstractmethod
    def make_unit(self, name: str, dtype: type) -> StorageModel:
        """Factory: create the correct StorageModel for this engine."""
        pass