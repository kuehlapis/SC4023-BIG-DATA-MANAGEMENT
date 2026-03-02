class MainView:
    def __init__(self):
        pass
    
    def display_menu(self):
        print("\n=== Big Data Management System ===")
        print("1. Load existing database")
        print("2. Create new database")
        print("3. Exit")

    def get_input(self, prompt="\nEnter your choice: "):
        return input(prompt).strip()

    def display_message(self, message):
        print(f"\n[SYSTEM] {message}")