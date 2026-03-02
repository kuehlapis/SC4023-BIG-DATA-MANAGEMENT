# query.py

from model.TableModel import Table

class Query:
    def __init__(self, table: Table):
        self.table = table
        self.indexes = list(range(len(next(iter(table.storage_units.values())).scan())))
        self._selected_indexes = self.indexes.copy()
        self._agg_func = None
        self._agg_col = None
        self._columns = None

    def select(self, indexes=None):
        """Return values from selected indexes. Optionally pass a subset of indexes."""
        if indexes is not None:
            self._selected_indexes = [i for i in indexes if i in self._selected_indexes]
        return self._selected_indexes

    def where(self, column: str, predicate) -> "Query":
        """Filter indexes based on predicate on a column."""
        col = self.table.get_unit(column)
        all_values = col.scan()
        self._selected_indexes = [
            i for i in self._selected_indexes
            if _safe_predicate(predicate, all_values[i])
        ]
        return self

    def aggregate(self, column: str, func: str):
        """Aggregate values from selected indexes only via scan()."""
        if not self._selected_indexes:
            return None
        try:
            col = self.table.get_unit(column)
            # delegate to Column.aggregate() which uses scan() internally
            return col.aggregate(func, self._selected_indexes)
        except Exception as e:
            print(f"Error in aggregation: {e}")
            return None

    # def execute(self) -> list[dict]:
    #     """Return results as list of row dicts."""
    #     if not self._selected_indexes:
    #         return []

    #     cols = self._columns or list(self.table.storage_units.keys())
    #     scanned = {
    #         c: self.table.get_unit(c).scan(self._selected_indexes)
    #         for c in cols
    #     }
    #     return [
    #         dict(zip(cols, row))
    #         for row in zip(*[scanned[c] for c in cols])
    #     ]

    def fetch(self) -> list[dict]:
        """Retrieve actual values for the current selected indexes."""
        return self.table.get_rows(self._selected_indexes)


def _safe_predicate(predicate, value) -> bool:
    try:
        return predicate(value)
    except (TypeError, ValueError):
        return False