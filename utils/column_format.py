import os
from typing import Dict
import pandas as pd

from utils.base_format import BaseFormat
from model.StorageModel import StorageModel



class ColumnFormat(BaseFormat):
    """Column-oriented file I/O engine. Each column → one .col file."""

    FORMAT_NAME = "column"

    TOWN_MAP = {
        "ANG MO KIO": 1,
        "BEDOK": 2,
        "BISHAN": 3,
        "BUKIT BATOK": 4,
        "BUKIT MERAH": 5,
        "BUKIT PANJANG": 6,
        "BUKIT TIMAH": 7,
        "CENTRAL AREA": 8,
        "CHOA CHU KANG": 9,
        "CLEMENTI": 10,
        "GEYLANG": 11,
        "HOUGANG": 12,
        "JURONG EAST": 13,
        "JURONG WEST": 14,
        "KALLANG/WHAMPOA": 15,
        "MARINE PARADE": 16,
        "PASIR RIS": 17,
        "PUNGGOL": 18,
        "QUEENSTOWN": 19,
        "SEMBAWANG": 20,
        "SENGKANG": 21,
        "SERANGOON": 22,
        "TAMPINES": 23,
        "TOA PAYOH": 24,
        "WOODLANDS": 25,
        "YISHUN": 26
    }

    def __init__(self, db_path: str = None):
        self.column_path = db_path

    def write(self, df: pd.DataFrame, metadata: dict) -> None:
        try:
            os.makedirs(self.column_path, exist_ok=True)
            
            df = self.psm_price(df)
            df = self.month_num(df)
            df = self.compress_town(df)
            df =  self.sort_column("month_num", df)
            
            columns = df.columns.tolist()
            for col in columns:
                file_path = os.path.join(self.column_path, f"{col}.col")
                df[col].astype(str).to_csv(file_path, index=False, header=False)

            metadata.update({
                "columns": columns
            })
            print(f"[ColumnFormat] Wrote {len(df)} rows × {len(columns)} columns → '{self.column_path}'")
        except Exception as e:
            print(f"Error in ColumnFormat.write: {e}")

    def write_units(self, units: Dict[str, StorageModel]) -> None:
        try:
            os.makedirs(self.column_path, exist_ok=True)

            for name, unit in units.items():
                file_path = os.path.join(self.column_path, f"{name}.col")
                with open(file_path, "w", encoding="utf-8") as f:
                    for value in unit.scan():
                        f.write(str(value) + "\n")

            print(f"[ColumnFormat] Wrote {len(units)} columns → '{self.column_path}'")
        except Exception as e:
            print(f"Error in ColumnFormat.write_units: {e}")

    def read_column(self, col_name: str)-> list:
        """Reads only the necessary column file."""
        try:
            file_path = os.path.join(self.column_path, f"{col_name}.col")
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    data = [line.strip() for line in f if line.strip() != ""]
                    print(f"[ColumnFormat] Read column '{col_name}' ({len(data)} rows)")
                    return data
            return []
        except Exception as e:
            print(f"Error in read_column: {e}")
            return []
    
    def month_num(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert 'month.col' (MMM-YY format) to 'month_num' column in YYYYMM format.
        e.g. 'Jan-15' → '201501'"""
        try:
            if "month" not in df.columns:
                print("Warning: 'month' column not found.")
                return df

            month_map = {
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
            }

            def parse_month(month_str):
                try:
                    mmm, yy = str(month_str).strip().split("-")
                    yyyy = f"20{yy}"
                    mm = month_map[mmm]
                    return f"{yyyy}{mm}"
                except (ValueError, KeyError):
                    print(f"Warning: Skipping unrecognised month '{month_str}'")
                    return ""

            df["month_num"] = df["month"].apply(parse_month)
            print(f"Created 'month_num' column with {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error in month_num: {e}")
            return df

    def psm_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate price per square metre (PSM) from resale_price and floor_area_sqm columns."""
        try:
            if "resale_price" not in df.columns or "floor_area_sqm" not in df.columns:
                print("Error: 'resale_price' or 'floor_area_sqm' columns not found.")
                return df

            def calc_psm(row):
                try:
                    price = float(row["resale_price"])
                    area = float(row["floor_area_sqm"])
                    if area == 0:
                        return ""
                    return f"{price / area:.2f}"
                except (ValueError, TypeError):
                    return ""

            df["psm_price"] = df.apply(calc_psm, axis=1)
            print(f"Created 'psm_price' column with {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error in psm_price: {e}")
            return df

    def read(self) -> Dict[str, list]:
        try:
            if not os.path.exists(self.column_path):
                raise FileNotFoundError(f"Database directory not found: {self.column_path}")

            column_data = {}

            for file in sorted(os.listdir(self.column_path)):
                if file.endswith(".col"):
                    col_name = file[:-4]
                    column_data[col_name] = self.read_column(col_name)

            print(f"[ColumnFormat] read loaded {len(column_data)} columns from '{self.column_path}'")
            return column_data
        except Exception as e:
            print(f"Error in read: {e}")
            return {}
        
    def compress_town(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compress town names to integers."""
        try:
            if "town" not in df.columns:
                print("Warning: 'town' column not found.")
                return df

            df["town_int"] = df["town"].apply(lambda t: str(self.TOWN_MAP.get(t, "")))
            print(f"Created 'town_int' column with {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error in compress_town: {e}")
            return df

    def sort_column(self, col_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Sort the DataFrame by one column and save it as sorted_{column_name}.csv."""
        try:
            if df is None or df.empty:
                raise ValueError("Input DataFrame is empty.")

            if col_name not in df.columns:
                raise KeyError(f"Column '{col_name}' not found in DataFrame.")

            sorted_df = df.sort_values(by=col_name, kind="mergesort")
            print(f"Sorted DataFrame by column '{col_name}' with {len(sorted_df)} rows.")
            
            return sorted_df
        except Exception as e:
            print(f"Error in sort_column: {e}")
            return df
