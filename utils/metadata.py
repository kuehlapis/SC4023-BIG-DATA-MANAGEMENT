import json
import os

class MetaLoader:
    META_FILE = "db.meta.json"

    def __init__(self):
        pass

    def metadata_format(self, name: str, path: str, engine: str) -> dict:
        return {
            "name": name,
            "path": path,
            "engine": engine,
        }

    @staticmethod
    def load(db_path: str) -> dict:
        meta_path = os.path.join(db_path, MetaLoader.META_FILE)
        if not os.path.exists(meta_path):
            raise FileNotFoundError("Database metadata missing")

        with open(meta_path, "r") as f:
            return json.load(f)

    @staticmethod
    def save(db_path: str, meta: dict):
        meta_path = os.path.join(db_path, MetaLoader.META_FILE)
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=4)