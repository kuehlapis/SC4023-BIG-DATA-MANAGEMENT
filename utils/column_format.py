import os
from typing import Dict
import pandas as pd

from utils.base_format import BaseFormat
from model.StorageModel import StorageModel
from model.ColumnModel import Column


class ColumnFormat(BaseFormat):
    """Column-oriented file I/O engine. Each column → one .col file."""

    FORMAT_NAME = "column"

    def write(self, df: pd.DataFrame, db_path: str, metadata: dict) -> dict:
        os.makedirs(db_path, exist_ok=True)

        columns = []
        for col in df.columns:
            file_path = os.path.join(db_path, f"{col}.col")
            df[col].astype(str).to_csv(file_path, index=False, header=False)
            columns.append(col)

        metadata.update({
            "columns": columns
        })

        print(f"[ColumnFormat] Wrote {len(df)} rows × {len(columns)} columns → '{db_path}'")
        return metadata

    def write_units(self, units: Dict[str, StorageModel], db_path: str) -> None:
        os.makedirs(db_path, exist_ok=True)

        for name, unit in units.items():
            file_path = os.path.join(db_path, f"{name}.col")
            with open(file_path, "w", encoding="utf-8") as f:
                for value in unit.scan():
                    f.write(str(value) + "\n")

        print(f"[ColumnFormat] Wrote {len(units)} columns → '{db_path}'")

    def read_column(self, col_name: str, path: str)-> list:
        """Reads only the necessary column file."""
        try:
            file_path = os.path.join(path, f"{col_name}.col")
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    data = [line.strip() for line in f if line.strip() != ""]
                    print(f"[ColumnFormat] Read column '{col_name}' ({len(data)} rows)")
                    return data
            return []
        except Exception as e:
            print(f"Error in read_column: {e}")
            return []
    
    def write_month_num(self, path: str) -> None:
        """Reads 'month.col' (MMM-YY format) and writes a new 'month_num.col' in YYYYMM format.
        e.g. 'Jan-15' → '201501'"""
        try:
            month_map = {
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
            }
            
            raw = self.read_column("month", path)

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

            target_path = os.path.join(path, "month_num.col")
            with open(target_path, "w", encoding="utf-8") as f:
                f.write("\n".join(month_nums) + "\n")
            print(f"Written {len(month_nums)} rows to 'month_num.col'")
        except Exception as e:
            print(f"Error in write_month_num: {e}")

    def write_psm_price(self, path: str) -> None:
        """Calculates price per square metre (PSM) from resale_price and floor_area_sqm columns,
        and writes the result to 'psm_price.col'."""
        try:
            prices = self.read_column("resale_price", path)
            areas = self.read_column("floor_area_sqm", path)
            
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

            target_path = os.path.join(path, "psm_price.col")
            with open(target_path, "w", encoding="utf-8") as f:
                f.write("\n".join(psm_prices) + "\n")
            print(f"Written {len(psm_prices)} rows to 'psm_price.col'")
        except Exception as e:
            print(f"Error in write_psm_price: {e}")

    def read(self, db_path: str) -> Dict[str, list]:
        try:
            if not os.path.exists(db_path):
                raise FileNotFoundError(f"Database directory not found: {db_path}")

            column_data = {}

            for file in sorted(os.listdir(db_path)):
                if file.endswith(".col"):
                    col_name = file[:-4]
                    column_data[col_name] = self.read_column(col_name, db_path)

            print(f"[ColumnFormat] read loaded {len(column_data)} columns from '{db_path}'")
            return column_data
        except Exception as e:
            print(f"Error in read: {e}")
            return {}
