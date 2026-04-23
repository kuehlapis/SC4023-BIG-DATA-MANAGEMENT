import functools
import os
import pandas as pd

class CSVLoader:
    def __init__(self, csv_path="Data/ResalePricesSingapore.csv"):
        self.csv_path = csv_path
        
    @functools.lru_cache(maxsize=None)  
    def load_data(self):
        try:
            if not os.path.exists(self.csv_path):
                raise FileNotFoundError(f"Source CSV '{self.csv_path}' not found.")
            return pd.read_csv(self.csv_path)
        except Exception as e:
            raise RuntimeError(f"Utility Error (Load): {e}")