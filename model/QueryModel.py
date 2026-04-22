# model/QueryModel.py

import bisect
from optimization.BitmapIndex import BitmapIndex
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
        # Lazy bitmap-backed selection (BitmapIndex) when available
        self._bitmap_selection: BitmapIndex | None = None

    def clone(self) -> "Query":
        """Create a lightweight copy for reuse."""
        new_q = Query.__new__(Query)
        new_q.table = self.table
        new_q._column_cache = self._column_cache
        new_q._selected_indexes = self._selected_indexes.copy()
        new_q._bitmap_selection = self._bitmap_selection
        return new_q

    def select(self, indexes=None):
        if indexes is not None:
            # explicit select overrides any bitmap selection
            self._bitmap_selection = None
            self._selected_indexes = [
                i for i in indexes if i in self._selected_indexes
            ]
            return self._selected_indexes

        # materialize bitmap selection if present
        if self._bitmap_selection is not None:
            self._selected_indexes = self._bitmap_selection.get_positions()
            self._bitmap_selection = None
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
        # Try bitmap-backed lookup first
        bm_map = getattr(self.table, "bitmap_indexes", {})
        col_bms = bm_map.get(column)
        if col_bms is not None:
            # find metadata key for the requested value (metadata keys are strings)
            value_key = None
            if str(value) in col_bms:
                value_key = str(value)
            elif value in col_bms:
                value_key = value

            b = self.table.get_bitmap(column, value_key) if value_key is not None else None
            if b is not None and getattr(b, "length", None) == len(col_data):
                print("using bitmap for where_eq on column", column)
                # Lazy bitmap intersection: adopt or AND without materializing
                if len(self._selected_indexes) == len(col_data) and self._bitmap_selection is None:
                    # no prior restrictions, adopt bitmap directly
                    self._bitmap_selection = b
                    return self

                if self._bitmap_selection is not None:
                    # intersect with existing bitmap selection
                    self._bitmap_selection = self._bitmap_selection.and_(b)
                    return self

                # Convert current selection to bitmap and intersect
                bits = 0
                for i in self._selected_indexes:
                    bits |= (1 << i)
                cur_b = BitmapIndex(bits, len(col_data))
                self._bitmap_selection = cur_b.and_(b)
                return self

        # Fallback to linear scan
        self._selected_indexes = [
            i for i in self._selected_indexes
            if col_data[i] == value
        ]
        return self

    def where_in(self, column: str, values) -> "Query":
        col_data = self._column_cache[column]
        # Try bitmap-backed lookup first
        bm_map = getattr(self.table, "bitmap_indexes", {})
        col_bms = bm_map.get(column)
        if col_bms is not None:
            # combine bitmaps for the requested values
            combined = None
            valid = True
            for v in values:
                # find matching metadata key
                v_key = str(v) if str(v) in col_bms else (v if v in col_bms else None)
                if v_key is None:
                    # missing bitmap for this value — skip
                    continue
                b = self.table.get_bitmap(column, v_key)
                if b is not None:
                    combined = b if combined is None else combined.or_(b)
                else:
                    continue

            if combined is not None and getattr(combined, "length", None) == len(col_data):
                print("using bitmap for where_in on column", column)
                # Lazy bitmap intersection similar to where_eq
                if len(self._selected_indexes) == len(col_data) and self._bitmap_selection is None:
                    self._bitmap_selection = combined
                    return self

                if self._bitmap_selection is not None:
                    self._bitmap_selection = self._bitmap_selection.and_(combined)
                    return self

                bits = 0
                for i in self._selected_indexes:
                    bits |= (1 << i)
                cur_b = BitmapIndex(bits, len(col_data))
                self._bitmap_selection = cur_b.and_(combined)
                return self

        # Fallback to linear scan
        value_set = set(values)
        self._selected_indexes = [
            i for i in self._selected_indexes
            if col_data[i] in value_set
        ]
        return self

    def where_gte(self, column: str, threshold) -> "Query":
        col_data = self._column_cache[column]
        
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