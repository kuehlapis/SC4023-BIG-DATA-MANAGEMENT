from __future__ import annotations

from pathlib import Path


REQUIRED_COLUMNS = [
    "month",
    "town",
    "flat_type",
    "block",
    "street_name",
    "storey_range",
    "floor_area_sqm",
    "flat_model",
    "lease_commence_date",
    "resale_price",
]


def load_column(path: str) -> list[str]:
    """Load one .col file and return stripped row values."""
    with open(path, "r", encoding="utf-8") as file:
        return [line.rstrip("\n\r") for line in file]


def load_all_columns(db_dir: str) -> dict[str, list[str]]:
    """Load all required columns from Database directory."""
    db_path = Path(db_dir)
    columns: dict[str, list[str]] = {}

    for col in REQUIRED_COLUMNS:
        col_path = db_path / f"{col}.col"
        if not col_path.exists():
            raise FileNotFoundError(f"Missing required column file: {col_path}")
        columns[col] = load_column(str(col_path))

    return columns


def validate_column_lengths(columns: dict[str, list[str]]) -> int:
    """Ensure all columns have identical row count and return N."""
    if not columns:
        raise ValueError("No columns loaded.")

    lengths = {name: len(values) for name, values in columns.items()}
    unique_lengths = set(lengths.values())
    if len(unique_lengths) != 1:
        mismatch = ", ".join(f"{name}={length}" for name, length in sorted(lengths.items()))
        raise ValueError(f"Column length mismatch: {mismatch}")

    return unique_lengths.pop()
