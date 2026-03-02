from model.DatabaseModel import DatabaseModel
from model.TableModel import Table
from utils.column_format import ColumnFormat
from view.DatabaseView import DatabaseView
from utils.conditions import Condition
from utils.helpers import _parse_month, _safe_float, parse_int
from model.QueryModel import Query
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
        # matric_num = self.db_view.prompt_user("\nEnter matric number")

        db_model = DatabaseModel(db_name)
        engine = db_model.get_engine()
        table = Table(engine, name=db_name)
        table.load(db_model.get_path()) 
        q = Query(table)

        condition = Condition()

        amk_count = q.where("town", lambda x: x == "ANG MO KIO").select()
        print(f"Flats in ANG MO KIO: {len(amk_count)}")
        load_end = time.time()
        
        print(f"\nDatabase '{db_name}' loaded in {load_end - start:.2f} seconds.")


        
