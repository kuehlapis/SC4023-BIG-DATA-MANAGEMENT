import pandas as pd
import functools

class ParquetFormat:
    def __init__(self, file_path="Database/Data/ResalePricesSingapore.csv"):
        self.file_path = file_path

    @functools.lru_cache(maxsize=None)  
    def load_data(self, file_path):
        try:
            return pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
            return pd.DataFrame()
        
    def save_file(self):
        # 1. Load the row-oriented CSV into memory
        df = self.load_data(self.file_path)

        # 2. Save as column-oriented Parquet
        # 'snappy' is a compression codec that balances size and speed
        df.to_parquet('Database/data/data.parquet', engine='pyarrow', compression='snappy')

        # # Verification: Read back ONLY specific columns (Super fast)
        # df_cols = pd.read_parquet('Database/data/data.parquet', columns=['User_ID', 'Amount'])
        # print(df_cols.head())


if __name__ == "__main__":
    parquet_format = ParquetFormat()
    parquet_format.save_file()
