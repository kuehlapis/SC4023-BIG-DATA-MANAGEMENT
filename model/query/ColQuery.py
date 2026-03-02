from __future__ import annotations
from model.TableModel import Table
from model.query.DomainCol import DomainCol


class Query(DomainCol):
    """
    Column-oriented query engine.
    """

    def __init__(self, table: Table):
        super().__init__(table)
        self._bitmap: list[bool] = [True] * self._n

    def _selected_indexes(self) -> list[int]:
        return [i for i, keep in enumerate(self._bitmap) if keep]

    def reset(self) -> "Query":
        self._bitmap = [True] * self._n
        return self

    def count(self) -> int:
        return sum(self._bitmap)