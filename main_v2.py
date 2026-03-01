# main.py

import os
import json
from pathlib import Path
from table import Table
from query import Query

def load_table_from_folder(folder_path: str) -> Table:
    folder = Path(folder_path)
    col_files = list(folder.glob("*.col"))

    if not col_files:
        raise FileNotFoundError(f"No .col files found in {folder_path}")

    table = Table(folder.name)

    for col_file in col_files:
        col_name = col_file.stem
        raw = col_file.read_text().strip().splitlines()

        # Infer dtype and cast values
        def cast(val):
            try: return int(val)
            except ValueError: pass
            try: return float(val)
            except ValueError: pass
            return val  # fallback to string

        data = [cast(v) for v in raw]
        dtype = type(data[0]) if data else str
        table.add_column(col_name, dtype)
        table.columns[col_name].data = data

    print(f"Loaded table '{table.name}' with columns: {list(table.columns.keys())}")
    return table


def main():
    folder_path = "Database"

    if not os.path.isdir(folder_path):
        print(f"Error: '{folder_path}' is not a valid directory.")
        return

    table = load_table_from_folder(folder_path)
    q = Query(table)

    print(f"Total rows: {len(q.indexes)}")
    print(f"Columns: {list(table.columns.keys())}")

    amk_flats_count = q.where("town", lambda x: x=="ANG MO KIO").select()

    print(f"Number of flats in amk: {len(amk_flats_count)}")

if __name__ == "__main__":
    main()