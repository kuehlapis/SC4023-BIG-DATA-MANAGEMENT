import os
import json

from utils.base_format import BaseFormat
from utils.csv_loader import CSVLoader
from utils.metadata import MetaLoader
from utils.column_format import ColumnFormat


class DatabaseModel:
    """Manages database lifecycle: creation, loading metadata, and path resolution."""

    BASE_DIR = "Database"
         
    _ENGINE_MAP = {
        "column" : ColumnFormat,
    }


    def __init__(self, name: str, engine: BaseFormat = None):
        self.name = name
        self.path = os.path.join(self.BASE_DIR, name)
        self.engine: BaseFormat = engine

    def create_database(self, csv_path: str) -> None:
        if self.engine is None:
            raise ValueError("No storage engine provided for database creation.")

        loader = CSVLoader(csv_path)
        df = loader.load_data()
        os.makedirs(self.path, exist_ok=True)

        metadata = {
            "name": self.name,
            "path": self.path,
            "engine": self.engine.FORMAT_NAME,
        }
        
        self.engine.write(df, self.path, metadata)
        MetaLoader.save(self.path, metadata)
        print(f"[DatabaseModel] Created database '{self.name}' at '{self.path}'")


    def get_engine(self) -> BaseFormat:
        """
        Resolve the correct BaseFormat engine from saved metadata.
        """
        if self.engine is not None:
            return self.engine

        meta = MetaLoader.load(self.path)
        fmt = meta.get("engine")
        print(fmt)

        if fmt not in self._ENGINE_MAP:
            raise ValueError(
                f"Unknown engine '{fmt}'. "
                f"Registered engines: {list(self._ENGINE_MAP.keys())}"
            )
        self.engine = self._ENGINE_MAP[fmt]()
        return self.engine

    def get_path(self) -> str:
        return self.path

    @staticmethod
    def list_all_databases() -> list:
        if not os.path.exists(DatabaseModel.BASE_DIR):
            return []
        return [
            d for d in os.listdir(DatabaseModel.BASE_DIR)
            if os.path.isdir(os.path.join(DatabaseModel.BASE_DIR, d))
        ]

    @staticmethod
    def validate_source_dir(path: str) -> None:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Source CSV not found: '{path}'")

    @staticmethod
    def validate_new_name(name: str) -> None:
        if not name or not name.strip():
            raise ValueError("Database name cannot be empty.")

    @staticmethod
    def validate_orientation_choice(choice: str) -> str:
        choice = choice.strip().lower()
        if choice not in {"column", "row"}:
            raise ValueError("Choice must be 'column' or 'row'.")
        return choice