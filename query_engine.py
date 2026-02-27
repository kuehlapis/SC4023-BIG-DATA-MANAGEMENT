from __future__ import annotations

from typing import Any

from conditions import (
    build_time_window,
    start_month_from_matric,
    target_year_from_matric,
    towns_from_matric,
)
from parsing import parse_float, parse_month


def row_matches(
    i: int,
    columns: dict[str, list[str]],
    valid_towns: set[str],
    valid_months: set[tuple[int, int]],
    y: int,
) -> bool:
    """Return whether row i satisfies town, month and floor-area filters."""
    town = columns["town"][i].strip().upper()
    if town not in valid_towns:
        return False

    year_month = parse_month(columns["month"][i])
    if year_month not in valid_months:
        return False

    area = parse_float(columns["floor_area_sqm"][i])
    return area >= y


def price_per_sqm(columns: dict[str, list[str]], i: int) -> float:
    """Compute resale price per sqm for row i."""
    area = parse_float(columns["floor_area_sqm"][i])
    if area <= 0:
        raise ValueError(f"Invalid floor area at row {i}: {area}")
    price = parse_float(columns["resale_price"][i])
    return price / area


def best_row_for_xy(
    columns: dict[str, list[str]],
    x: int,
    y: int,
    matric_num: str,
) -> int | None:
    """
    Find row index with minimum price_per_sqm for one (x, y).
    Returns None if no row matches or minimum psm > 4725.
    """
    target_year = target_year_from_matric(matric_num)
    start_month = start_month_from_matric(matric_num)
    valid_months = set(build_time_window(target_year, start_month, x))
    valid_towns = set(t.upper() for t in towns_from_matric(matric_num))

    best_idx: int | None = None
    best_psm = float("inf")
    row_count = len(columns["month"])

    for i in range(row_count):
        try:
            if not row_matches(i, columns, valid_towns, valid_months, y):
                continue

            psm = price_per_sqm(columns, i)
            if psm < best_psm or (psm == best_psm and (best_idx is None or i < best_idx)):
                best_psm = psm
                best_idx = i
        except (ValueError, KeyError):
            # Skip malformed rows but keep scanning.
            continue

    if best_idx is None or best_psm > 4725:
        return None
    return best_idx


def scan_all_pairs(columns: dict[str, list[str]], matric_num: str) -> list[dict[str, Any]]:
    """Scan all required (x, y) pairs and return structured results."""
    results: list[dict[str, Any]] = []
    for x in range(1, 9):
        for y in range(80, 151):
            idx = best_row_for_xy(columns, x, y, matric_num)
            results.append({"x": x, "y": y, "row_index": idx})
    return results
