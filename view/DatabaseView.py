class DatabaseView:
    def show_header(self):
        print("\n" + "="*30)
        print("      DATABASE MANAGER ")
        print("="*30)

    def display_databases(self, databases: list):
        print("\nAvailable databases:")
        if not databases:
            print("  (No databases found)")
        for i, db in enumerate(databases, 1):
            print(f"{i}. {db}")

    def prompt_user(self, message):
        try:
            return input(f"{message}: ").strip()
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            return None

    def display_success(self, message):
        print(f"\n[SUCCESS] {message}")

    def display_error(self, message):
        print(f"\n[ERROR] {message}")

    def select_orientation(self):
        print("\nSelect orientation:")
        print("1. Row-oriented")
        print("2. Column-oriented")