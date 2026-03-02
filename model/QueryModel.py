from abc import ABC, abstractmethod
from typing import Any, Callable
from model.TableModel import Table


class QueryModel(ABC):
    """Abstract base query interface. Subclass for column or row oriented queries."""

    def __init__(self, table: Table):
        self.table = table
        self._n: int = table.row_count()

    @abstractmethod
    def reset(self) -> "QueryModel":
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def where(self, field: str, predicate: Callable) -> "QueryModel":
        pass

    @abstractmethod
    def where_in(self, field: str, values: set) -> "QueryModel":
        pass

    @abstractmethod
    def where_multi(self, predicates: dict[str, Callable]) -> "QueryModel":
        pass

    @abstractmethod
    def filter_towns(self, valid_towns: set[str]) -> "QueryModel":
        pass

    @abstractmethod
    def filter_months(self, valid_months: set[tuple[int, int]]) -> "QueryModel":
        pass

    @abstractmethod
    def filter_min_area(self, min_sqm: float) -> "QueryModel":
        pass

    @abstractmethod
    def fetch(self, field: str) -> list:
        pass

    @abstractmethod
    def fetch_rows(self, fields: list[str] = None) -> list[dict]:
        pass

    @abstractmethod
    def fetch_computed(self, field: str, transform: Callable) -> list:
        pass

    @abstractmethod
    def aggregate(self, func: str, field: str):
        pass

    @abstractmethod
    def price_per_sqm(self, i: int) -> float:
        pass

    @abstractmethod
    def min_by_psm(self) -> tuple[int | None, float]:
        pass

    @abstractmethod
    def best_row_for_xy(
        self,
        x: int,
        y: float,
        valid_towns: set[str],
        valid_months: set[tuple[int, int]],
        max_psm: float,
    ) -> int | None:
        pass

    @abstractmethod
    def scan_all_pairs(
        self,
        valid_towns: set[str],
        valid_months_for: Callable[[int], set[tuple[int, int]]],
        x_range: range,
        y_range: range,
        max_psm: float,
    ) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def execute(self) -> list[dict]:
        pass