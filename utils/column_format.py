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

            columns = []
            for col in df.columns:
                file_path = os.path.join(self.column_path, f"{col}.col")
                df[col].astype(str).to_csv(file_path, index=False, header=False)
                columns.append(col)

            self.psm_price()
            self.month_num()
            self.compress_town()
            columns.extend(["psm_price", "month_num", "town_int"])
            
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
    
    def month_num(self) -> None:
        """Reads 'month.col' (MMM-YY format) and writes a new 'month_num.col' in YYYYMM format.
        e.g. 'Jan-15' → '201501'"""
        try:
            month_map = {
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
            }
            
            raw = self.read_column("month")

            if not raw:
                print("Error: 'month.col' not found or is empty.")
                return
            
            month_nums = []
            for row in raw:
                try:
                    mmm, yy = row.split("-")
                    yyyy = f"20{yy}"
                    mm = month_map[mmm]
                    month_nums.append(f"{yyyy}{mm}")
                except (ValueError, KeyError):
                    print(f"Warning: Skipping unrecognised row '{row}'")
                    month_nums.append("")

            target_path = os.path.join(self.column_path, "month_num.col")
            with open(target_path, "w", encoding="utf-8") as f:
                f.write("\n".join(month_nums) + "\n")
            print(f"Written {len(month_nums)} rows to 'month_num.col'")
        except Exception as e:
            print(f"Error in write_month_num: {e}")

    def psm_price(self) -> None:
        """Calculates price per square metre (PSM) from resale_price and floor_area_sqm columns,
        and writes the result to 'psm_price.col'."""
        try:
            prices = self.read_column("resale_price")
            areas = self.read_column("floor_area_sqm")
            
            if not prices or not areas:
                print("Error: 'resale_price.col' or 'floor_area_sqm.col' not found or is empty.")
                return
            
            if len(prices) != len(areas):
                print("Error: Column lengths do not match.")
                return
            
            psm_prices = []
            for price, area in zip(prices, areas):
                try:
                    psm = float(price) / float(area)
                    psm_prices.append(f"{psm:.2f}")
                except (ValueError, ZeroDivisionError):
                    print(f"Warning: Skipping invalid row (price='{price}', area='{area}')")
                    psm_prices.append("")

            target_path = os.path.join(self.column_path, "psm_price.col")
            with open(target_path, "w", encoding="utf-8") as f:
                f.write("\n".join(psm_prices) + "\n")
            print(f"Written {len(psm_prices)} rows to 'psm_price.col'")
        except Exception as e:
            print(f"Error in write_psm_price: {e}")

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
        
    def compress_town(self) -> None:
            """Compress town names to integers."""
            try:
                town_str = self.read_column("town")
                
                if not town_str:
                    print("Error: 'town.col' not found or is empty.")
                    return
                
                town_int = []   

                for t in town_str:
                    town_int.append(str(self.TOWN_MAP.get(t)))

                target_path = os.path.join(self.column_path, "town_int.col")
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(town_int) + "\n")
                print(f"Written {len(town_int)} rows to 'town_int.col'")
            except Exception as e:
                print(f"Error in compress_town: {e}")
