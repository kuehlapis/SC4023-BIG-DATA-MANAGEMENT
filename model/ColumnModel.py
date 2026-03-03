# model/ColumnModel.py

from model.StorageModel import StorageModel


class Column(StorageModel):
    """A single typed column of in-memory data."""

    def __init__(self, name: str, dtype: type = str):
        self.name = name
        self.dtype = dtype
        self.data: list = []

    def append(self, value) -> None:
        self.data.append(self.dtype(value))

    def scan(self) -> list:
        """Return entire column data."""
        return self.data