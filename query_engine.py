from __future__ import annotations

from typing import Any

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
