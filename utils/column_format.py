import os
from typing import Dict
import pandas as pd

from utils.base_format import BaseFormat
from model.StorageModel import StorageModel
from utils.indexer import build_index, build_bpt



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

    FLAT_TYPE_MAP = {
        "1 ROOM": 0,
        "2 ROOM": 1,
        "3 ROOM": 2,
        "4 ROOM": 3,
        "5 ROOM": 4,
        "EXECUTIVE": 5,
        "MULTI-GENERATION": 6
    }

    STOREY_RANGE_MAP = {
        "01 TO 03": 0,
        "04 TO 06": 1,
        "07 TO 09": 2,
        "10 TO 12": 3,
        "13 TO 15": 4,
        "16 TO 18": 5,
        "19 TO 21": 6,
        "22 TO 24": 7,
        "25 TO 27": 8,
        "28 TO 30": 9,
        "31 TO 33": 10,
        "34 TO 36": 11,
        "37 TO 39": 12,
        "40 TO 42": 13,
        "43 TO 45": 14,
        "46 TO 48": 15,
        "49 TO 51": 16
    }

    FLAT_MODEL_MAP = {
        "2-room": 0,
        "3Gen": 1,
        "Adjoined flat": 2,
        "Apartment": 3,
        "DBSS": 4,
        "Improved": 5,
        "Improved-Maisonette": 6,
        "Maisonette": 7,
        "Model A": 8,
        "Model A2": 9,
        "Model A-Maisonette": 10,
        "Multi Generation": 11,
        "New Generation": 12,
        "Premium Apartment": 13,
        "Premium Apartment Loft": 14,
        "Premium Maisonette": 15,
        "Simplified": 16,
        "Standard": 17,
        "Terrace": 18,
        "Type S1": 19,
        "Type S2": 20
    }

    def __init__(self, db_path: str = None):
        self.column_path = db_path

    def format_name(self) -> str:
        """Return the format name for metadata."""
        return self.FORMAT_NAME

    def write(self, df: pd.DataFrame, metadata: dict) -> None:
        try:
            os.makedirs(self.column_path, exist_ok=True)
            
            df = self.psm_price(df)
            df = self.month_num(df)
            df = self.compress_town(df)
            # df = self.encode_flat_type(df)
            # df = self.encode_storey_range(df)
            # df = self.encode_flat_model(df)
            df = self.sort_column("month_num", df)
            
            columns = df.columns.tolist()
            sorted_columns = ["month_num"]
            
            for col in columns:
                file_path = os.path.join(self.column_path, f"{col}.col")
                # Preserve native numeric representations when writing.
                df[col].to_csv(file_path, index=False, header=False)

            # build per-column indexes (JSON sidecars)
            indexes = {}
            bpt_roots = {}
            for col in columns:
                try:
                    idx = build_index(self.column_path, col)
                    if idx:
                        indexes[col] = f"{col}.idx.json"
                except Exception:
                    pass
                try:
                    root = build_bpt(self.column_path, col)
                    if root:
                        bpt_roots[col] = root
                except Exception:
                    pass

            metadata.update({
                "columns": columns,
                "sorted_columns": sorted_columns,
                "indexes": indexes,
                "bpt_roots": bpt_roots,
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
                    # Preserve empty lines so row alignment is maintained.
                    data = [line.rstrip("\n") for line in f]
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
                    yyyy = int(f"20{yy}")
                    mm = month_map[mmm]
                    return int(f"{yyyy}{mm}")
                except (ValueError, KeyError):
                    print(f"Warning: Skipping unrecognised month '{month_str}'")
                    return None

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
                        return float("nan")
                    return price / area
                except (ValueError, TypeError):
                    return float("nan")

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

    def encode_flat_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode flat_type to integers."""
        try:
            if "flat_type" not in df.columns:
                print("Warning: 'flat_type' column not found.")
                return df

            df["flat_type_int"] = df["flat_type"].apply(
                lambda t: str(self.FLAT_TYPE_MAP.get(t.strip().upper(), ""))
            )
            print(f"Created 'flat_type_int' column with {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error in encode_flat_type: {e}")
            return df

    def encode_storey_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode storey_range to integers."""
        try:
            if "storey_range" not in df.columns:
                print("Warning: 'storey_range' column not found.")
                return df

            df["storey_range_int"] = df["storey_range"].apply(
                lambda s: str(self.STOREY_RANGE_MAP.get(s.strip().upper(), ""))
            )
            print(f"Created 'storey_range_int' column with {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error in encode_storey_range: {e}")
            return df

    def encode_flat_model(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode flat_model to integers."""
        try:
            if "flat_model" not in df.columns:
                print("Warning: 'flat_model' column not found.")
                return df

            df["flat_model_int"] = df["flat_model"].apply(
                lambda m: str(self.FLAT_MODEL_MAP.get(m.strip(), ""))
            )
            print(f"Created 'flat_model_int' column with {len(df)} rows")
            return df
        except Exception as e:
            print(f"Error in encode_flat_model: {e}")
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
