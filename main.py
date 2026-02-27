from __future__ import annotations

import argparse
from pathlib import Path

from output_writer import write_scan_result
from query_engine import scan_all_pairs
from storage import load_all_columns, validate_column_lengths


def main() -> None:
    parser = argparse.ArgumentParser(description="SC4023 column-store query runner")
    parser.add_argument("matric_num", help="Matric number used to generate query conditions")
    parser.add_argument(
        "--db-dir",
        default="Database",
        help="Directory containing the 10 .col files (default: Database)",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: ScanResult_<matric_num>.csv)",
    )
    args = parser.parse_args()

    columns = load_all_columns(args.db_dir)
    validate_column_lengths(columns)

    results = scan_all_pairs(columns, args.matric_num)
    out_path = args.out or f"ScanResult_{args.matric_num}.csv"
    write_scan_result(out_path, results, columns)
    print(f"Wrote output to {Path(out_path).resolve()}")


if __name__ == "__main__":
    main()
