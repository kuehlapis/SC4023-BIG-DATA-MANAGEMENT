import os
from model.StorageModel import StorageModel


class Column(StorageModel):
    """A single typed column of in-memory data."""

    def __init__(self, name: str, dtype: type = str):
        self.name = name
        self.dtype = dtype
        self.data: list = []

    def append(self, value) -> None:
        self.data.append(self.dtype(value))

    def scan(self, indexes: list = None) -> list:
        if indexes is not None:
            return [self.data[i] for i in indexes]
        return list(self.data)

    def aggregate(self, func: str, indexes: list = None):
        data = self.scan(indexes)
        if not data:
            raise ValueError(f"No data to aggregate on column '{self.name}'.")
        ops = {
            "sum":   lambda d: sum(d),
            "avg":   lambda d: sum(d) / len(d),
            "min":   lambda d: min(d),
            "max":   lambda d: max(d),
            "count": lambda d: len(d),
        }
        if func not in ops:
            raise ValueError(f"Unsupported aggregation '{func}'.")
        return ops[func](data)