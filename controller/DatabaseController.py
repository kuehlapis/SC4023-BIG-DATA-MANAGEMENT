import csv
import time

from model.DatabaseModel import DatabaseModel
from model.TableModel import Table
from model.ColumnModel import Column
from view.DatabaseView import DatabaseView
from utils.conditions import Condition
from utils.helpers import Helpers
from model.QueryModel import Query
from utils.output_writer import OutputWriter


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
        try:
            self.db_view.show_header()

            path = self.db_view.prompt_user("\nEnter path to source CSV")
            DatabaseModel.validate_source_dir(path)

            name = self.db_view.prompt_user("\nEnter name for new database")
            DatabaseModel.validate_new_name(name)

            self.db_view.select_orientation()
            orientation_choice = self.db_view.prompt_user(
                "\nEnter orientation choice (column / row)"
            )
            choice = DatabaseModel.validate_orientation_choice(orientation_choice)

            if choice == "column":
                engine = Column
            elif choice == "row":
                raise NotImplementedError("Row-oriented format not yet implemented.")
            else:
                raise ValueError(f"Unknown orientation: {choice}")

            db_model = DatabaseModel(name, engine)
            db_model.create_database(path)
            self.db_view.display_success(f"Database '{name}' created successfully.")
        except Exception as e:
            self.db_view.display_error(str(e))

    def select_db(self) -> None:
        try:

            databases = DatabaseModel.list_all_databases()
            if not databases:
                self.db_view.display_error("No databases found.")
                return

            self.db_view.display_databases(databases)

            db_name = self.db_view.prompt_user("\nEnter database name")
            matric_num = self.db_view.prompt_user("\nEnter matric number")
 
            # Load database
            db_model = DatabaseModel(db_name)
            engine = db_model.get_engine()
            table = Table(engine, name=db_name)
            table.load()

            condition = Condition()
            start_yr_mth = condition.start_yr_mth_from_matric(matric_num)
            # valid_towns = set(
            #     t.upper() for t in condition.towns_from_matric(matric_num)
            # )
            valid_towns_int = set(condition.town_ints_from_matric(matric_num))

            results = []

            try:
                start = time.time()
                base_query = Query(table)

                
                # base_query.where(
                #     "town",
                #     lambda x, towns=valid_towns: str(x).strip().upper() in towns
                # )
                
                base_query.where(
                    "town_int",
                    lambda x, towns_int=valid_towns_int: str(x) in towns_int
                )

                print(f"Number of records after town filter: {len(base_query.select())}")

                
                base_query.where(
                    "month_num",
                    lambda x, start=start_yr_mth: x >= start
                )

                print(f"Number of records after month filter: {len(base_query.select())}")


                for x in range(1, 9):
                    end_month = Helpers.add_months(start_yr_mth, int(x))
                    base_query.where("month_num", lambda m, end=end_month: m <= end)

                    for y in range(80, 151):
                        q = base_query.clone()
                        q.where("floor_area_sqm", lambda area, min_sqm=float(y): area >= min_sqm)

                        min_psm = q.aggregate("psm_price", "min")
                        
                        if min_psm is None or min_psm > 4725:
                            results.append({"x": x, "y": y, "row": None})
                            continue

                        q.where("psm_price", lambda p, m=min_psm: p == m)

                        flats = q.fetch()

                        if not flats:
                            results.append({"x": x, "y": y, "row": None})
                        else:
                            results.append({"x": x, "y": y, "row": flats[0]})

                end_time = time.time()
                print(f"\nQuery completed in {end_time - start:.4f} seconds.")

            except Exception as e:
                print(f"Error during query execution: {e}")
                return
            
            OutputWriter(matric_num).write(results)
        except Exception as e:
            self.db_view.display_error(str(e))