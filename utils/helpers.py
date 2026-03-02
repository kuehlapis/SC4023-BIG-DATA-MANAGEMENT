from __future__ import annotations

from datetime import datetime
    

def _parse_month(month_str: str) -> tuple[int, int]:
    """Parse month string like 'Jan-15' into (2015, 1)."""
    parsed = datetime.strptime(month_str.strip(), "%b-%y")
    return parsed.year, parsed.month


def _safe_float(value: str) -> float:
    """Parse numeric value into float with whitespace trimmed."""
    return float(value)


def parse_int(value: str) -> int:
    """Parse numeric value into int with whitespace trimmed."""
    return int(value)
