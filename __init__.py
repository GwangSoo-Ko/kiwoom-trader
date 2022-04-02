from PyQt5.QtWidgets import *

from kiwoom.kiwoom import *


class Main:
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.kiwoom = Kiwoom()
        self.app.exec_()


if __name__ == "__main__":
    Main()
    # analysis.checkBuyList.check_buy_list()
