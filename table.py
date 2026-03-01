# table.py

from column import Column

class Table:
    def __init__(self, name: str):
        self.name = name
        self.columns: dict[str, Column] = {}

    def add_column(self, name: str, dtype: type):
        self.columns[name] = Column(name, dtype)

    def insert(self, row: dict):
        """Insert a row by distributing values across columns."""
        for col_name, value in row.items():
            self.columns[col_name].append(value)

    def get_column(self, name: str) -> Column:
        return self.columns[name]

    def save(self, directory: str):
        for column in self.columns.values():
            column.save(directory)

    def load(self, directory: str, schema: dict[str, type]):
        """Load columns from disk given a schema {col_name: dtype}."""
        for name, dtype in schema.items():
            col = Column(name, dtype)
            col.load(directory)
            self.columns[name] = col