# model/QueryModel.py

from model.TableModel import Table


class Query:
    def __init__(self, table: Table):
        self.table = table

        # Cache all columns once
        self._column_cache = {
            name: unit.scan()
            for name, unit in table.storage_units.items()
        }

        # Initialize all row indexes
        first_column = next(iter(self._column_cache.values()))
        self._selected_indexes = list(range(len(first_column)))

    def clone(self) -> "Query":
        """Create a lightweight copy for reuse."""
        new_q = Query(self.table)
        new_q._column_cache = self._column_cache
        new_q._selected_indexes = self._selected_indexes.copy()
        return new_q

    def select(self, indexes=None):
        if indexes is not None:
            self._selected_indexes = [
                i for i in indexes if i in self._selected_indexes
            ]
        return self._selected_indexes

    def where(self, column: str, predicate) -> "Query":
        col_data = self._column_cache[column]

        self._selected_indexes = [
            i for i in self._selected_indexes
            if predicate(col_data[i])
        ]
        return self

    def fetch(self) -> list[dict]:
        column_cache = self._column_cache

        return [
            {col: column_cache[col][i] for col in column_cache}
            for i in self._selected_indexes
        ]

    def aggregate(self, column: str, func: str):
        if not self._selected_indexes:
            return None

        col_data = self._column_cache[column]
        data = [col_data[i] for i in self._selected_indexes]

        if not data:
            return None

        if func == "max":
            return max(data)
        elif func == "min":
            return min(data)
        elif func == "sum":
            return sum(data)
        elif func == "avg":
            return sum(data) / len(data)
        elif func == "count":
            return len(data)
        else:
            raise ValueError(f"Invalid aggregation function '{func}'")