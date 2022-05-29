from datetime import datetime

import exchange_calendars as ecals
from PyQt5.QtWidgets import *
from pytz import timezone

from kiwoom.kiwoom import *


class Main:
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.kiwoom = Kiwoom()
        self.app.exec_()

    def my_exception_hook(exctype, value, traceback):  # 에러 발생시 표기를 위해 추가된 부분  (# 표기)
        print(exctype, value, traceback)  #
        sys.__excepthook__(exctype, value, traceback)  #

    sys._excepthook = sys.excepthook  #
    sys.excepthook = my_exception_hook


if __name__ == "__main__":
    XKRX = ecals.get_calendar("XKRX")
    today = datetime.today().astimezone(timezone('Asia/Seoul'))
    if XKRX.is_session(today.strftime("%Y-%m-%d")):
        print("Open")
        Main()

    else:
        print("Closed")
        print(XKRX.previous_close(today.strftime("%Y-%m-%d")).astimezone(timezone('Asia/Seoul')).strftime("%Y%m%d"))
        last_open_day = XKRX.previous_close(today.strftime("%Y-%m-%d")).astimezone(timezone('Asia/Seoul'))
        last_open_day_string = XKRX.previous_close(today.strftime("%Y-%m-%d")).astimezone(
            timezone('Asia/Seoul')).strftime("%Y%m%d")
        if os.path.exists('files/hold/current_' + last_open_day.strftime("%Y%m%d") + '.csv'):
            print("Aleady Exists!!")
        else:
            analysis.checkBuySellList.check_buy_sell_list()
