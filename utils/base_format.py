from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd
from model.StorageModel import StorageModel


class BaseFormat(ABC):
    """
    Abstract storage engine.
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
    def write_month_num(self) -> None:
        """Calculate and write 'month_num.col' based on 'month.col'."""
        pass

    @abstractmethod
    def write_psm_price(self) -> None:
        """Calculate and write 'psm_price.col' based on 'resale_price.col' and 'floor_area_sqm.col'."""
        pass

    @abstractmethod
    def read(self, db_path: str) -> Dict[str, list]:
        """Read ALL data, returned as {column_name: [values]}."""
        pass

    @abstractmethod
    def read_column(self, column_name: str, db_path: str) -> list:
        """Read a single column by name."""
        pass

