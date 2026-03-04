# model/StorageModel.py

from abc import ABC, abstractmethod

class StorageModel(ABC):
    """Abstract base for a single storage unit (Column or Row)."""

    @abstractmethod
    def append(self, value) -> None:
        pass

    @abstractmethod
    def scan(self) -> list:
        """Return all stored values."""
        pass
