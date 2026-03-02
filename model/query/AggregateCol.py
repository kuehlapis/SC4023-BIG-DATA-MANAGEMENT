from __future__ import annotations
from model.query.FetchQuery import FetchCol
from utils.helpers import _safe_float


class AggregateCol(FetchCol):
    """
    Column-level aggregation.
    """

    def aggregate(self, func: str, field: str):
        """
        Run a named aggregation (sum/avg/min/max/count) on a column
        restricted to the current bitmap selection.
        """
        unit = self.table.get_unit(field)
        return unit.aggregate(func, self._selected_indexes())

    def _build_psm_column(self) -> list[float]:
        """
        Compute price-per-sqm for ALL rows as a full column array.
        Both source columns loaded once — purely columnar. O(n).
        """
        price_col = self._get_col("resale_price")
        area_col  = self._get_col("floor_area_sqm")
        return [
            _safe_float(price_col[i]) / _safe_float(area_col[i])
            if _safe_float(area_col[i]) > 0
            else float("inf")
            for i in range(self._n)
        ]

    def min_by_psm(self) -> tuple[int | None, float]:
        """
        Scan the psm column restricted to the current bitmap.
        Returns (best_row_index, best_psm).
        Purely columnar — no row dicts created. O(n).
        """
        psm_col = self._build_psm_column()
        best_idx = None
        best_val = float("inf")
        for i in range(self._n):
            if self._bitmap[i] and psm_col[i] < best_val:
                best_val = psm_col[i]
                best_idx = i
        return best_idx, best_val

    def price_per_sqm(self, i: int) -> float:
        """Compute price/sqm for a single row index (used for display only)."""
        area = _safe_float(self._get_col("floor_area_sqm")[i])
        if area <= 0:
            raise ValueError(f"Invalid floor area at row {i}.")
        return _safe_float(self._get_col("resale_price")[i]) / area