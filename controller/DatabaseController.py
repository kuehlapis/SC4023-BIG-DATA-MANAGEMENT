from model.DatabaseModel import DatabaseModel
from model.TableModel import Table
from model.query.ColQuery import Query
from utils.column_format import ColumnFormat
from view.DatabaseView import DatabaseView
from utils.conditions import Condition
from utils.helpers import _parse_month, _safe_float, parse_int
import time


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
        table.load(db_model.get_path())
        condition =  Condition()
        
        valid_towns = set(t.upper() for t in condition.towns_from_matric(matric_num))
        target_year = condition.target_year_from_matric(matric_num)
        start_month = condition.start_month_from_matric(matric_num)

        def valid_months_for(x: int) -> set[tuple[int, int]]:
            return set(condition.build_time_window(target_year, start_month, x))

        q = Query(table)
        results = q.scan_all_pairs(
            valid_towns=valid_towns,
            valid_months_for=valid_months_for,
            x_range=range(1, 9),
            y_range=range(80, 151),
            max_psm=4725.0,
        )

        matched = [r for r in results if r["row_index"] is not None]
        end = time.time()
        self.db_view.display_success(f"Query completed in {end - start:.2f} seconds.")
        print(f"\nTotal (x,y) pairs scanned : {len(results)}")
        print(f"Pairs with a valid match  : {len(matched)}")


        display_fields = ["month", "town", "block", "floor_area_sqm", "flat_model", "lease_commence_date"]
        col_data = {f: table.get_unit(f).scan() for f in display_fields}

        for r in matched:
            idx = r["row_index"]
            month_raw = col_data["month"][idx]
            year, _ = _parse_month(month_raw)

            print(
                f"  x={r['x']:>2}, y={r['y']:>3} "
                f"  year={year} "
                f"→ town={col_data['town'][idx]:<20} "
                f"block={col_data['block'][idx]:<10} "
                f"floor_area={col_data['floor_area_sqm'][idx]:>6} "
                f"model={col_data['flat_model'][idx]:<15} "
                f"lease={col_data['lease_commence_date'][idx]} "
                f"psm={r['psm']:.2f}"
            )
