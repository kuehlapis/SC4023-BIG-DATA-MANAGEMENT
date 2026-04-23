from __future__ import annotations

from datetime import datetime
    
class Helpers:
    """Utility functions for parsing and type inference."""
    @staticmethod
    def _parse_month(month_str: str) -> tuple[int, int]:
        """Parse month string like 'Jan-15' into (2015, 1)."""
        parsed = datetime.strptime(month_str.strip(), "%b-%y")
        return parsed.year, parsed.month

    @staticmethod
    def _safe_float(value: str) -> float:
        """Parse numeric value into float with whitespace trimmed."""
        return float(value)

    @staticmethod
    def parse_int(value: str) -> int:
        """Parse numeric value into int with whitespace trimmed."""
        return int(value)
    
    @staticmethod
    def add_months(yearmonth: int, months: int) -> int:
        year = yearmonth // 100
        month = yearmonth % 100

        month += months
        year += (month - 1) // 12
        month = (month - 1) % 12 + 1

        return year * 100 + month
        
    @staticmethod
    def cast(val):
        try: return int(val)
        except ValueError: pass
        try: return float(val)
        except ValueError: pass
        return val  # fallback to string

    @staticmethod
    def _infer_dtype(values: list) -> type:
        """Infer dtype from a sample of values. Defaults to str if mixed or empty."""
        try:
            sample = [v for v in values if str(v).strip() != ""][:100]
            if not sample:
                return str
            if all(Helpers._is_int(v) for v in sample):
                return int
            if all(Helpers._is_float(v) for v in sample):
                return float
            return str
        except Exception as e:
            print(f"Warning: Failed to infer dtype, defaulting to str. Error: {e}")
            return str

    @staticmethod
    def _safe_cast(value: str, dtype: type):
        """Cast value to dtype, fall back to str if it fails."""
        try:
            if value is None:
                return float("nan") if dtype == float else (None if dtype == int else "")

            s = str(value).strip()
            if s == "" or s.lower() == "nan":
                if dtype == int:
                    return None
                if dtype == float:
                    return float("nan")
                return ""

            if dtype == int:
                # handle "80.0" -> 80
                return int(float(s))
            if dtype == float:
                return float(s)
            return dtype(s)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _is_int(v) -> bool:
        try:
            int(v)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _is_float(v) -> bool:
        try:
            float(v)
            return True
        except (ValueError, TypeError):
            return False