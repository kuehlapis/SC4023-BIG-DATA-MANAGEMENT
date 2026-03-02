from abc import ABC, abstractmethod


class StorageModel(ABC):
    """Abstract base for a single storage unit (Column or Row)."""

    @abstractmethod
    def append(self, value) -> None:
        pass

    @abstractmethod
    def scan(self, indexes: list = None) -> list:
        pass

    @abstractmethod
    def aggregate(self, func: str, indexes: list = None):
        pass

