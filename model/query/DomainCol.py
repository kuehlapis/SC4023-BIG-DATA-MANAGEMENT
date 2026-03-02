from __future__ import annotations
from typing import Any, Callable
from model.query.AggregateCol import AggregateCol


class DomainCol(AggregateCol):
    """
    Domain-specific high-level queries.
    Composes filter + aggregate mixins — no new data access logic here.
    """

    def best_row_for_xy(
        self,
        x: int,
        y: float,
        valid_towns: set[str],
        valid_months: set[tuple[int, int]],
        max_psm: float = 4725.0,
    ) -> int | None:
        """
        Column-oriented search for best row for one (x, y) pair.

        Filter pipeline (each step is one full column pass):
          1. town column scan   O(n)
          2. month column scan  O(n)
          3. area column scan   O(n)
          4. psm column scan    O(n)
        Total: O(4n), zero row materialisation.
        """
        self.reset()
        self.filter_towns(valid_towns)
        self.filter_months(valid_months)
        self.filter_min_area(y)

        best_idx, best_psm = self.min_by_psm()
        self.reset()

        if best_idx is None or best_psm > max_psm:
            return None
        return best_idx

    def scan_all_pairs(
        self,
        valid_towns: set[str],
        valid_months_for: Callable[[int], set[tuple[int, int]]],
        x_range: range = range(1, 9),
        y_range: range = range(80, 151),
        max_psm: float = 4725.0,
    ) -> list[dict[str, Any]]:
        """
        Scan all (x, y) pairs — columnar optimised.

        Optimisation:
          - Column cache persists across ALL (x, y) — disk read once per column.
          - Town + month bitmap computed ONCE per x value.
          - Only area bitmap re-applied per y (cheapest filter).
        """
        results: list[dict[str, Any]] = []

        for x in x_range:
            valid_months = valid_months_for(x)

            # Compute town + month bitmap once for this x
            self.reset()
            self.filter_towns(valid_towns)
            self.filter_months(valid_months)
            base_bitmap = self._bitmap.copy()   # snapshot

            for y in y_range:
                # Restore town+month bitmap, add area filter only
                self._bitmap = base_bitmap.copy()
                self.filter_min_area(y)

                best_idx, best_psm = self.min_by_psm()

                if best_idx is not None and best_psm <= max_psm:
                    results.append({
                        "x": x,
                        "y": y,
                        "row_index": best_idx,
                        "psm": best_psm,
                    })
                else:
                    results.append({
                        "x": x,
                        "y": y,
                        "row_index": None,
                        "psm": None,
                    })

        self.reset()
        return results