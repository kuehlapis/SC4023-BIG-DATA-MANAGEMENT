# model/QueryModel.py

import bisect
import os
from model.TableModel import Table
from utils.indexer import load_index, bpt_search_equal, bpt_search_range


class Query:
    def __init__(self, table: Table):
        self.table = table

        # Cache all columns once
        self._column_cache = {
            name: unit.scan()
            for name, unit in table.storage_units.items()
        }

        # DB path and B+ tree roots from table metadata (if available)
        self._db_path = None
        self._bpt_roots = {}
        try:
            self._db_path = getattr(self.table.engine, 'db_path', None) or getattr(self.table.engine, 'column_path', None)
            meta = getattr(self.table, 'meta', {}) or {}
            self._bpt_roots = meta.get('bpt_roots', {})
        except Exception:
            self._db_path = None
            self._bpt_roots = {}

        # Initialize all row indexes
        first_column = next(iter(self._column_cache.values()))
        self._selected_indexes = list(range(len(first_column)))

    def clone(self) -> "Query":
        """Create a lightweight copy for reuse."""
        new_q = Query.__new__(Query)
        new_q.table = self.table
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

    def where_eq(self, column: str, value) -> "Query":
        col_data = self._column_cache[column]

        # Fast path: B+ tree exact lookup
        try:
            if self._db_path and column in self._bpt_roots:
                root = self._bpt_roots.get(column)
                rows = bpt_search_equal(self._db_path, root, str(value))
                rows_set = set(rows)
                self._selected_indexes = [i for i in self._selected_indexes if i in rows_set]
                return self
        except Exception:
            pass

        # Fallback to in-memory scan
        self._selected_indexes = [
            i for i in self._selected_indexes
            if col_data[i] == value
        ]
        return self

    def where_in(self, column: str, values) -> "Query":
        col_data = self._column_cache[column]
        value_set = set(values)
        # Fast path: B+ tree value_map union via equal searches
        try:
            if self._db_path and column in self._bpt_roots:
                root = self._bpt_roots.get(column)
                rows_set = set()
                for v in value_set:
                    rows_set.update(bpt_search_equal(self._db_path, root, str(v)))
                self._selected_indexes = [i for i in self._selected_indexes if i in rows_set]
                return self
        except Exception:
            pass

        self._selected_indexes = [
            i for i in self._selected_indexes
            if col_data[i] in value_set
        ]
        return self

    def where_gte(self, column: str, threshold) -> "Query":
        col_data = self._column_cache[column]
        # Fast path: B+ tree range lookup if available
        try:
            if self._db_path and column in self._bpt_roots:
                root = self._bpt_roots.get(column)
                rows = bpt_search_range(self._db_path, root, str(threshold), None)
                rows_set = set(rows)
                self._selected_indexes = [i for i in self._selected_indexes if i in rows_set]
                return self
        except Exception:
            pass

        if column in self.table.sorted_columns and len(self._selected_indexes) == len(col_data):
            print("using binary search for where_gte")
            start_idx = bisect.bisect_left(col_data, threshold)
            self._selected_indexes = list(range(start_idx, len(col_data)))
        else:
            # Fallback to linear scan
            self._selected_indexes = [
                i for i in self._selected_indexes
                if col_data[i] >= threshold
            ]
        return self

    def where_lte(self, column: str, threshold) -> "Query":
        col_data = self._column_cache[column]
        # Fast path: B+ tree range lookup if available
        try:
            if self._db_path and column in self._bpt_roots:
                root = self._bpt_roots.get(column)
                rows = bpt_search_range(self._db_path, root, None, str(threshold))
                rows_set = set(rows)
                self._selected_indexes = [i for i in self._selected_indexes if i in rows_set]
                return self
        except Exception:
            pass

        if column in self.table.sorted_columns and len(self._selected_indexes) == len(col_data):
            print("using binary search for where_lte")
            end_idx = bisect.bisect_right(col_data, threshold)
            self._selected_indexes = list(range(end_idx))
        else:
            # Fallback to linear scan
            self._selected_indexes = [
                i for i in self._selected_indexes
                if col_data[i] <= threshold
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

        # Use generators to avoid materializing entire list in memory
        if func == "max":
            return max((col_data[i] for i in self._selected_indexes), default=None)
        elif func == "min":
            return min((col_data[i] for i in self._selected_indexes), default=None)
        elif func == "sum":
            return sum(col_data[i] for i in self._selected_indexes)
        elif func == "avg":
            total = 0
            count = 0
            for i in self._selected_indexes:
                total += col_data[i]
                count += 1
            return total / count if count > 0 else None
        elif func == "count":
            return len(self._selected_indexes)
        else:
            raise ValueError(f"Invalid aggregation function '{func}'")