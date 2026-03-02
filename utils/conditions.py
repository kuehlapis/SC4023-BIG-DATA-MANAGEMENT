from __future__ import annotations

class Condition:
    """Utility functions to evaluate conditions for filtering rows based on matric number."""

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

    def _matric_digits(self, matric_num: str) -> list[str]:
        digits = [ch for ch in matric_num if ch.isdigit()]
        if len(digits) < 2:
            raise ValueError("Matric number must contain at least 2 digits.")
        return digits


    def towns_from_matric(self, matric_num: str) -> list[str]:
        """Return sorted unique towns mapped from digits in matric number."""
        digits = self._matric_digits(matric_num)
        towns = {self.TOWN_BY_DIGIT[d] for d in digits}
        return sorted(towns)


    def target_year_from_matric(self, matric_num: str) -> int:
        """
        Map the last digit in matric number to 2015..2024.
        If last digit is 5, year=2015; ...; 9=>2019; 0=>2020; ...; 4=>2024.
        """
        last_digit = int(self._matric_digits(matric_num)[-1])
        return 2010 + last_digit if last_digit >= 5 else 2020 + last_digit


    def start_month_from_matric(self, matric_num: str) -> int:
        """Use second-last digit as month, with 0 interpreted as October (10)."""
        second_last = int(self._matric_digits(matric_num)[-2])
        return 10 if second_last == 0 else second_last


    def build_time_window(self, year: int, start_month: int, x: int) -> list[tuple[int, int]]:
        """
        Build x-month window beginning from (year, start_month), rolling into next year.
        Example: year=2019, start=11, x=3 => [(2019,11),(2019,12),(2020,1)]
        """
        if not (1 <= x <= 8):
            raise ValueError("x must be in range [1, 8].")
        if not (1 <= start_month <= 12):
            raise ValueError("start_month must be in range [1, 12].")

        window: list[tuple[int, int]] = []
        y, m = year, start_month
        for _ in range(x):
            window.append((y, m))
            m += 1
            if m > 12:
                m = 1
                y += 1
        return window
    