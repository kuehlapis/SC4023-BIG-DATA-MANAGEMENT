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
    def month_num(self) -> None:
        """Calculate and write 'month_num' based on 'month'."""
        pass

    @abstractmethod
    def psm_price(self) -> None:
        """Calculate and write 'psm_price' based on 'resale_price' and 'floor_area_sqm'."""
        pass

    @abstractmethod
    def read(self, db_path: str) -> Dict[str, list]:
        """Read ALL data"""
        pass

    @abstractmethod
    def compress_town(self) -> None:
        """Compress 'town' by mapping town names to integers."""
        pass

    @abstractmethod
    def encode_flat_type(self) -> None:
        """Encode 'flat_type' by mapping flat types to integers."""
        pass

    @abstractmethod
    def encode_storey_range(self) -> None:
        """Encode 'storey_range' by mapping storey ranges to integers."""
        pass

    @abstractmethod
    def encode_flat_model(self) -> None:
        """Encode 'flat_model' by mapping flat models to integers."""
        pass

