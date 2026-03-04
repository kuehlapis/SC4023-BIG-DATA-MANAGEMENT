import pandas as pd
import functools
import os

class ColumnFormat:

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
        
    def __init__(self, csv_path="Data/ResalePricesSingapore.csv"):
        self.csv_path = csv_path
        
    @functools.lru_cache(maxsize=None)  
    def load_data(self):
        try:
            return pd.read_csv(self.csv_path)
        except FileNotFoundError:
            print(f"Error: File '{self.csv_path}' not found.")
            return pd.DataFrame()
        
    def write_batch(self):
        """Efficiently saves each Pandas series as a column file."""
        df = self.load_data()
        if df.empty:
            return
        
        if not os.path.exists("Database"):
            os.makedirs("Database")

        # Iterate through columns directly (faster than to_dict)
        for col in df.columns:
            target_path = os.path.join("Database", f"{col}.col")
            
            # Use Pandas' built-in to_csv for speed, or manual write
            df[col].astype(str).to_csv(target_path, index=False, header=False)
            print(f"Serialized column: {col}")

    def query_column(self, col_name):
        """Reads only the necessary column file."""
        file_path = os.path.join("Database/ResalePrices", f"{col_name}.col")
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return [line.strip() for line in f]
        return []
    
    def write_month_num(self):
        """Reads 'month.col' (MMM-YY format) and writes a new 'month_num.col' in YYYYMM format.
        e.g. 'Jan-15' → '201501'"""
        
        month_map = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }
        
        raw = self.query_column("month")
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

        target_path = os.path.join("Database", "month_num.col")
        with open(target_path, "w") as f:
            f.write("\n".join(month_nums) + "\n")
        print(f"Written {len(month_nums)} rows to 'month_num.col'")

    def write_psm_price(self):
        """Calculates price per square metre (PSM) from resale_price and floor_area_sqm columns,
        and writes the result to 'psm_price.col'."""
        
        prices = self.query_column("resale_price")
        areas = self.query_column("floor_area_sqm")
        
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

        target_path = os.path.join("Database", "psm_price.col")
        with open(target_path, "w") as f:
            f.write("\n".join(psm_prices) + "\n")
        print(f"Written {len(psm_prices)} rows to 'psm_price.col'")

    def compress_town(self) -> None:
            """Compress town names to integers."""
            try:
                town_str = self.query_column("town")
                
                if not town_str:
                    print("Error: 'town.col' not found or is empty.")
                    return
                
                town_int = []   

                for t in town_str:
                    town_int.append(str(self.TOWN_MAP.get(t)))

                target_path = os.path.join("Database", "town_int.col")
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(town_int) + "\n")
                print(f"Written {len(town_int)} rows to 'town_int.col'")
            except Exception as e:
                print(f"Error in compress_town: {e}")


if __name__ == "__main__":
    db = ColumnFormat()
    #db.write_batch()
    
    # Example: Grab just the resale prices
    #prices = db.query_column("resale_price")
    #print(f"Loaded {len(prices)} price points.")
    #db.write_month_num()
    #db.write_psm_price()
    db.compress_town()