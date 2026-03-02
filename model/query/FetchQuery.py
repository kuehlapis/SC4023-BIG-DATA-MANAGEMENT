from __future__ import annotations
from typing import Callable
from model.query.FilterQuery import FilterCol


class FetchCol(FilterCol):
    """
    Materialise query results from the bitmap.
    """

    def fetch(self, field: str) -> list:
        """Return column values for selected rows."""
        col = self._get_col(field)
        return [col[i] for i in self._selected_indexes()]

    def fetch_rows(self, fields: list[str] = None) -> list[dict]:
        """
        Materialise selected rows as list of dicts.
        Each column loaded once — O(cols × selected) not O(rows × cols).
        """
        fields = fields or self.table.units()
        cols = {f: self._get_col(f) for f in fields}
        return [
            {f: cols[f][i] for f in fields}
            for i in self._selected_indexes()
        ]

    def fetch_computed(self, field: str, transform: Callable) -> list:
        """Apply a transform over a column for selected rows."""
        col = self._get_col(field)
        return [transform(col[i]) for i in self._selected_indexes()]