from __future__ import annotations

import csv
from typing import Any


class OutputWriter:

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

    def __init__(self, matric_num: str):
        self.output_file = f"ScanResult_{matric_num}.csv"

    def write(self, results: list[dict[str, Any]]) -> None:
        """Write query results to CSV in the required output format."""
        with open(self.output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self.HEADER)

            for entry in results:
                x = entry["x"]
                y = entry["y"]
                row = entry["row"]

                if row is None:
                    writer.writerow([f"({x}, {y})", "No result", "", "", "", "", "", "", ""])
                    continue

                month_num = row["month_num"]
                year = month_num // 100
                month = month_num % 100

                writer.writerow([
                    f"({x}, {y})",
                    str(year),
                    f"{month:02d}",
                    row["town"],
                    row["block"],
                    row["floor_area_sqm"],
                    row["flat_model"],
                    row["lease_commence_date"],
                    str(round(row["psm_price"])),
                ])

        print(f"Results written to {self.output_file}")

