# column.py

import json
from pathlib import Path

class Column:
    def __init__(self, name: str, dtype: type):
        self.name = name
        self.dtype = dtype
        self.data = []

    def append(self, value):
        self.data.append(self.dtype(value))

    def scan(self, predicate=None):
        """Return all values, optionally filtered by a predicate function."""
        if predicate:
            return [v for v in self.data if predicate(v)]
        return self.data

    def aggregate(self, func: str):
        """Basic aggregations: sum, avg, min, max, count."""
        ops = {
            "sum":   sum(self.data),
            "avg":   sum(self.data) / len(self.data),
            "min":   min(self.data),
            "max":   max(self.data),
            "count": len(self.data),
        }
        return ops[func]

    def save(self, directory: str):
        """Persist column to a flat file."""
        path = Path(directory) / f"{self.name}.col"
        path.write_text(json.dumps(self.data))

    def load(self, directory: str):
        """Load column from disk."""
        path = Path(directory) / f"{self.name}.col"
        self.data = json.loads(path.read_text())