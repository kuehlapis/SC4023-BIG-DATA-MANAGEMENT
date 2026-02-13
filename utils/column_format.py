import pandas as pd
import functools
import os

class ColumnFormat:
    def __init__(self, csv_path="Database/Data/ResalePricesSingapore.csv"):
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
        file_path = os.path.join("Database", f"{col_name}.col")
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return [line.strip() for line in f]
        return []

if __name__ == "__main__":
    db = ColumnFormat()
    db.write_batch()
    
    # Example: Grab just the resale prices
    prices = db.query_column("resale_price")
    print(f"Loaded {len(prices)} price points.")