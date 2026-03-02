from __future__ import annotations
from model.QueryModel import QueryModel
from model.TableModel import Table


class BaseQuery(QueryModel):
    """
    Concrete base state for all query engines.
    Holds column cache and implements count/reset/execute.
    """

    def __init__(self, table: Table):
        super().__init__(table)
        self._col_cache: dict[str, list] = {}

    def _get_col(self, field: str) -> list:
        """Return cached full column array. Each column read from disk once."""
        if field not in self._col_cache:
            self._col_cache[field] = self.table.get_unit(field).scan()
        return self._col_cache[field]

    def _selected_indexes(self) -> list[int]:
        raise NotImplementedError("Subclass must implement _selected_indexes()")

    def reset(self) -> "BaseQuery":
        self._col_cache.clear()
        return self

    def count(self) -> int:
        return len(self._selected_indexes())

    def execute(self) -> list[dict]:
        return self.fetch_rows()