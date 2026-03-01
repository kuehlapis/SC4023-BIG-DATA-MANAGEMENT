# query.py

from table import Table

class Query:
    def __init__(self, table: Table):
        self.table = table
        self.indexes = list(range(len(next(iter(table.columns.values())).data)))
        self._selected_indexes = self.indexes

    def select(self, indexes=None):
        """Return values from selected indexes. Optionally pass a subset of indexes."""
        if indexes is not None:
            self._selected_indexes = [i for i in indexes if i in self._selected_indexes]
        return self._selected_indexes

    def where(self, column: str, predicate):
        """Filter indexes based on a predicate applied to a column."""
        col = self.table.get_column(column)
        self._selected_indexes = [i for i in self._selected_indexes if predicate(col.data[i])]
        return self

    def aggregate(self, func: str, column: str):
        """e.g. query.aggregate('sum', 'price')"""
        self._agg_func = func
        self._agg_col = column
        return self

    def execute(self):
        # Aggregation path
        if self._agg_func:
            col = self.table.get_column(self._agg_col)
            data = col.scan(self._predicate)
            ops = {
                "sum":   sum(data),
                "avg":   sum(data) / len(data),
                "min":   min(data),
                "max":   max(data),
                "count": len(data),
            }
            return {self._agg_func: ops[self._agg_func]}

        # Scan path — return selected columns as a list of rows
        cols = self._columns or list(self.table.columns.keys())
        scanned = {c: self.table.get_column(c).scan() for c in cols}

        # Apply predicate across the first selected column as a row filter
        if self._predicate:
            first = scanned[cols[0]]
            mask = [i for i, v in enumerate(first) if self._predicate(v)]
            scanned = {c: [scanned[c][i] for i in mask] for c in cols}

        # Zip columns back into rows for output
        return [dict(zip(cols, row)) for row in zip(*[scanned[c] for c in cols])]

    def fetch(self, column: str):
        """Retrieve actual values for the current selected indexes from a column."""
        col = self.table.get_column(column)
        return [col.data[i] for i in self._selected_indexes]