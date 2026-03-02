from controller.DatabaseController import DatabaseController
from model.DatabaseModel import DatabaseModel
from utils.column_format import ColumnFormat
from view.MainView import MainView


# Register all available engines here — add RowFormat here in future
DatabaseModel.ENGINE_REGISTRY = {
    "column": ColumnFormat,
    # "row": RowFormat,   ← add when ready
}


class MainController:
    _VALID_CHOICES = {"1", "2", "3"}

    def __init__(self):
        self.main = MainView()
        self.db_controller = DatabaseController()

    def validate_choice(self, choice: str) -> str:
        clean = choice.strip()
        if not clean:
            raise ValueError("Input cannot be empty.")
        if not clean.isdigit():
            raise ValueError(f"'{clean}' is not a valid number.")
        if clean not in self._VALID_CHOICES:
            raise ValueError("Please select 1, 2, or 3.")
        return clean

    def run(self) -> None:
        while True:
            try:
                self.db_controller.load_menu()
                self.main.display_menu()
                choice = self.validate_choice(self.main.get_input())

                if choice == "1":
                    self.db_controller.select_db()
                elif choice == "2":
                    self.db_controller.create_db()
                elif choice == "3":
                    self.main.display_message("Exiting. Goodbye!")
                    break

            except (KeyboardInterrupt, EOFError):
                self.main.display_message("Exiting. Goodbye!")
                break
            except Exception as e:
                self.main.display_message(f"Error: {e}")