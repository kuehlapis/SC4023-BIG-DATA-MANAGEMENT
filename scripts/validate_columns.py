from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parsing import parse_float, parse_int
from storage import REQUIRED_COLUMNS, load_all_columns, validate_column_lengths


def main() -> None:
    db_dir = PROJECT_ROOT / "Database"

    print(f"Checking Database directory: {db_dir}")
    columns = load_all_columns(str(db_dir))
    row_count = validate_column_lengths(columns)

    # Basic type checks on the three numeric columns required by the assignment.
    for i in range(row_count):
        parse_float(columns["floor_area_sqm"][i])
        parse_float(columns["resale_price"][i])
        parse_int(columns["lease_commence_date"][i])

    print("Validation passed.")
    print(f"Columns present: {', '.join(REQUIRED_COLUMNS)}")
    print(f"Row count: {row_count}")


if __name__ == "__main__":
    main()
