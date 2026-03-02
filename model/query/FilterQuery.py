from __future__ import annotations
from typing import Callable
from model.query.BaseQuery import BaseQuery
from utils.helpers import _parse_month, _safe_float


class FilterCol(BaseQuery):
    """
    Column-oriented filter operations.
    Each method does ONE full column pass and ANDs result into the bitmap.
    """

    def where(self, field: str, predicate: Callable) -> "FilterCol":
        """Generic column scan with a predicate. O(n)."""
        col = self._get_col(field)
        self._bitmap = [
            self._bitmap[i] and predicate(col[i])
            for i in range(self._n)
        ]
        return self

    def where_in(self, field: str, values: set) -> "FilterCol":
        """Keep rows where field (uppercased) is in values set. O(n)."""
        col = self._get_col(field)
        self._bitmap = [
            self._bitmap[i] and (str(col[i]).strip().upper() in values)
            for i in range(self._n)
        ]
        return self

    def where_multi(self, predicates: dict[str, Callable]) -> "FilterCol":
        """
        Apply multiple column filters in one bitmap pass.
        Each column loaded once — more efficient than chaining where().
        """
        cols = {field: self._get_col(field) for field in predicates}
        self._bitmap = [
            self._bitmap[i] and all(
                pred(cols[field][i]) for field, pred in predicates.items()
            )
            for i in range(self._n)
        ]
        return self

    def filter_towns(self, valid_towns: set[str]) -> "FilterCol":
        """Column scan on 'town'. O(n)."""
        return self.where_in("town", valid_towns)

    def filter_months(self, valid_months: set[tuple[int, int]]) -> "FilterCol":
        """Column scan on 'month'. Parse entire column once. O(n)."""
        col = self._get_col("month")
        self._bitmap = [
            self._bitmap[i] and (_parse_month(col[i]) in valid_months)
            for i in range(self._n)
        ]
        return self

    def filter_min_area(self, min_sqm: float) -> "FilterCol":
        """Column scan on 'floor_area_sqm'. O(n)."""
        col = self._get_col("floor_area_sqm")
        self._bitmap = [
            self._bitmap[i] and (_safe_float(col[i]) >= min_sqm)
            for i in range(self._n)
        ]
        return self