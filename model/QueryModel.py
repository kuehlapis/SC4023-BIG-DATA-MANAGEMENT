# query.py

from model.TableModel import Table

class Query:
    def __init__(self, table: Table):
        self.table = table
        
        # all row indexes
        self.indexes = list(range(len(next(iter(table.storage_units.values())).data)))
        self._selected_indexes = self.indexes.copy()

        # query state
        self._agg_func = None
        self._agg_col = None
        self._columns = None   # selected columns

    def select(self, indexes=None):
        """Return values from selected indexes. Optionally pass a subset of indexes."""
        if indexes is not None:
            self._selected_indexes = [i for i in indexes if i in self._selected_indexes]
        return self._selected_indexes


    def where(self, column: str, predicate):
        """Filter indexes based on predicate on a column."""
        col = self.table.get_unit(column)
        self._selected_indexes = [
            i for i in self._selected_indexes
            if predicate(col.data[i])
        ]
        return self

    def aggregate(self, func: str, column: str):
        """e.g. query.aggregate('sum', 'price')"""
        self._agg_func = func
        self._agg_col = column
        return self

    def execute(self):
        # -------- Aggregation path --------
        if self._agg_func:
            col = self.table.get_unit(self._agg_col)
            return {
                self._agg_func: col.aggregate(self._agg_func, self._selected_indexes)
            }

        # -------- Scan path --------
        cols = self._columns or list(self.table.storage_units.keys())

        # column-oriented scan
        scanned = {
            c: self.table.get_unit(c).scan(self._selected_indexes)
            for c in cols
        }

        # zip columns -> rows
        return [
            dict(zip(cols, row))
            for row in zip(*[scanned[c] for c in cols])
        ]

    def fetch(self, column: str):
        """Retrieve actual values for current selected indexes from a column."""
        col = self.table.get_unit(column)
        return col.scan(self._selected_indexes)