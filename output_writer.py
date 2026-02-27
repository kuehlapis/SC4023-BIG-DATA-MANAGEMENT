from __future__ import annotations

import csv
from typing import Any

from parsing import parse_month
from query_engine import price_per_sqm

HEADER = [
    "(x, y)",
    "Year",
    "Month",
    "Town",
    "Block",
    "Floor_Area",
    "Flat_Model",
    "Lease_Commence_Date",
    "Price_Per_Square_Meter",
]


def format_output_row(entry: dict[str, Any], columns: dict[str, list[str]]) -> list[str]:
    """Format one output row for result CSV."""
    x = entry["x"]
    y = entry["y"]
    idx = entry["row_index"]

    if idx is None:
        return [f"({x}, {y})", "No result", "", "", "", "", "", "", ""]

    year, month = parse_month(columns["month"][idx])
    psm = round(price_per_sqm(columns, idx))
    return [
        f"({x}, {y})",
        str(year),
        f"{month:02d}",
        columns["town"][idx],
        columns["block"][idx],
        columns["floor_area_sqm"][idx],
        columns["flat_model"][idx],
        columns["lease_commence_date"][idx],
        str(psm),
    ]


def write_scan_result(
    out_path: str,
    results: list[dict[str, Any]],
    columns: dict[str, list[str]],
) -> None:
    """Write final ScanResult_<MatricNum>.csv file."""
    with open(out_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(HEADER)
        for entry in results:
            writer.writerow(format_output_row(entry, columns))
