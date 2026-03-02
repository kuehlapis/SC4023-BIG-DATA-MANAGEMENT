# main.py

import os
import json
from pathlib import Path
from table import Table
from query import Query
import csv
import time

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

def add_months(yearmonth: int, months: int) -> int:
    year = yearmonth // 100
    month = yearmonth % 100

    month += months
    year += (month - 1) // 12
    month = (month - 1) % 12 + 1

    return year * 100 + month

def main():
    folder_path = "Database"

    if not os.path.isdir(folder_path):
        print(f"Error: '{folder_path}' is not a valid directory.")
        return

    table = load_table_from_folder(folder_path)

    #print(f"Total rows: {len(q.indexes)}")
    #print(f"Columns: {list(table.columns.keys())}")

    query_towns = ["CLEMENTI", "PASIR RIS"]
    start_month = 201501
    answers = []

    start_time = time.time()

    for months in range(1, 9):
        for sqm in range(80, 151):
            end_month = add_months(start_month, months)

            q = Query(table)  # fresh query each iteration instead of reset
            q.where("town", lambda x, t=query_towns: x in t)
            q.where("month_num", lambda x, s=start_month, e=end_month: s <= x <= e)
            q.where("floor_area_sqm", lambda x, y=sqm: x >= y)
            min_psm = q.aggregate("psm_price", "min")
            q.where("psm_price", lambda x, m=min_psm: x == m)
            flats = q.fetch()

            answers.append({
                "months": months,
                "sqm": sqm,
                "end_month": end_month,
                "flats": flats
            })

    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    
    with open("results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        
        # Write header
        col_names = list(table.columns.keys())
        writer.writerow(["months", "sqm"] + col_names)
        
        # Write rows
        for answer in answers:
            for flat in answer["flats"]:
                writer.writerow(
                    [answer["months"], answer["sqm"]] +
                    [flat[col] for col in col_names]
                )

    print(f"Results written to results.csv")

if __name__ == "__main__":
    main()