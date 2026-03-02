from model.ColumnModel import Column
from model.StorageModel import StorageModel


class UnitModel:
    """Factory for creating StorageModel units based on orientation."""

    _REGISTRY = {
        "column": Column
    }

    @classmethod
    def create(cls, name: str, dtype: type = str, orientation: str = "column") -> StorageModel:
        if orientation not in cls._REGISTRY:
            raise ValueError(
                f"Unknown orientation '{orientation}'. "
                f"Supported: {list(cls._REGISTRY.keys())}"
            )
        return cls._REGISTRY[orientation](name, dtype)
