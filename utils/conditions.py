from __future__ import annotations


class Condition:
    """Utility functions to evaluate conditions based on matric number."""

    TOWN_BY_DIGIT = {
        "0": "BEDOK",
        "1": "BUKIT PANJANG",
        "2": "CLEMENTI",
        "3": "CHOA CHU KANG",
        "4": "HOUGANG",
        "5": "JURONG WEST",
        "6": "PASIR RIS",
        "7": "TAMPINES",
        "8": "WOODLANDS",
        "9": "YISHUN",
    }

    TOWN_INT_BY_DIGIT = {
        "0": "2",
        "1": "6",
        "2": "10",
        "3": "9",
        "4": "12",
        "5": "14",
        "6": "17",
        "7": "23",
        "8": "25",
        "9": "26",
    }



    def _matric_digits(self, matric_num: str) -> list[str]:
        # cache digits to avoid reparsing on every call
        if not hasattr(self, "_cached_digits") or self._cached_matric != matric_num:
            digits = [ch for ch in matric_num if ch.isdigit()]
            if len(digits) < 2:
                raise ValueError("Matric number must contain at least 2 digits.")
            self._cached_matric = matric_num
            self._cached_digits = digits
        return self._cached_digits

    def towns_from_matric(self, matric_num: str) -> list[str]:
        """Return sorted unique towns mapped from digits in matric number."""
        digits = self._matric_digits(matric_num)
        towns = {self.TOWN_BY_DIGIT[d] for d in digits}
        return sorted(towns)
    
    def town_ints_from_matric(self, matric_num: str) -> list[str]:
        """Return sorted unique town integers mapped from digits in matric number."""
        digits = self._matric_digits(matric_num)
        town_ints = {self.TOWN_INT_BY_DIGIT[d] for d in digits}
        return sorted(town_ints)

    def target_year_from_matric(self, matric_num: str) -> int:
        last_digit = int(self._matric_digits(matric_num)[-1])
        if last_digit >= 5:
            return 2010 + last_digit   # 5->2015, ..., 9->2019
        else:
            return 2020 + last_digit   # 0->2020, ..., 4->2024

    def start_month_from_matric(self, matric_num: str) -> int:
        second_last = int(self._matric_digits(matric_num)[-2])
        return 10 if second_last == 0 else second_last

    def start_yr_mth_from_matric(self, matric_num: str) -> int:
        year = self.target_year_from_matric(matric_num)
        month = self.start_month_from_matric(matric_num)
        return int(f"{year}{month:02d}")
    
    
