from controller.MainController import MainController


class Main:
    def __init__(self):
        self.main = MainController()

    def run(self):
        self.main.run()


if __name__ == "__main__":

    app = Main()
    app.run()
