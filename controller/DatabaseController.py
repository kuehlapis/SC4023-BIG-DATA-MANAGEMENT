import csv
from model.DatabaseModel import DatabaseModel
from model.TableModel import Table
from utils.column_format import ColumnFormat
from view.DatabaseView import DatabaseView
from utils.conditions import Condition
from utils.helpers import Helpers
from model.QueryModel import Query
import time
from utils.output_writer import write_scan_result


class DatabaseController:

    def __init__(self):
        self.db_view = DatabaseView()

    def load_menu(self) -> list:
        databases = DatabaseModel.list_all_databases()
        if not databases:
            self.db_view.display_error("No databases found.")
        else:
            self.db_view.display_databases(databases)
        return databases

    def create_db(self) -> None:
        self.db_view.show_header()

        path = self.db_view.prompt_user("\nEnter path to source CSV")
        DatabaseModel.validate_source_dir(path)

        name = self.db_view.prompt_user("\nEnter name for new database")
        DatabaseModel.validate_new_name(name)

        self.db_view.select_orientation()
        orientation_choice = self.db_view.prompt_user("\nEnter orientation choice (column / row)")
        choice = DatabaseModel.validate_orientation_choice(orientation_choice)

        # Pick the engine — extend here when RowFormat is ready
        if choice == "column":
            engine = ColumnFormat()
        elif choice == "row":
            raise NotImplementedError("Row-oriented format not yet implemented.")
        else:
            raise ValueError(f"Unknown orientation: {choice}")

        db_model = DatabaseModel(name, engine)
        db_model.create_database(path)
        self.db_view.display_success(f"Database '{name}' created successfully.")

    def select_db(self) -> None:
        start = time.time()
        databases = DatabaseModel.list_all_databases()
        if not databases:
            self.db_view.display_error("No databases found.")
            return

        self.db_view.display_databases(databases)
        db_name = self.db_view.prompt_user("\nEnter database name")
        matric_num = self.db_view.prompt_user("\nEnter matric number")

        db_model = DatabaseModel(db_name)
        engine = db_model.get_engine()
        table = Table(engine, name=db_name)
        path = db_model.get_path()
        table.load(path) 
        q = Query(table)

        condition = Condition()
        start_yr_mth = condition.start_yr_mth_from_matric(matric_num)
        valid_towns = set(t.upper() for t in condition.towns_from_matric(matric_num))
        
        answers= []
        try:
            for months in range(1, 9):
                for sqm in range(80, 151):
                    end_month = Helpers.add_months(start_yr_mth, months)
                    q.where("town", lambda x, t=valid_towns: x in t)
                    q.where("month_num", lambda x, s=start_yr_mth, e=end_month: s <= x <= e)
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

            print(f"{len(answers)} queries executed successfully.")
                    
        except Exception as e:
            print(f"Error during query execution: {e}")
            return
        
        load_end = time.time()
        print(f"\nQuery completed in {load_end - start:.2f} seconds.")

        with open("results.csv", "w", newline="") as f:
            writer = csv.writer(f)
            
            # Write header
            col_names = list(table.storage_units.keys())
            writer.writerow(["months", "sqm"] + col_names)
            
            # Write rows
            for answer in answers:
                for flat in answer["flats"]:
                    writer.writerow(
                        [answer["months"], answer["sqm"]] +
                        [flat[col] for col in col_names]
                    )

        print(f"Results written to results.csv")





