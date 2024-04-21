import os
from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.bar import BarGenerator

symbol = "btc_usdt"
# symbol = "bnb_usdt"
# symbol = "eth_usdt"
# symbol = "eth_btc"
# symbol = "btc_usdt"
# symbol = "eth_usdt"
# symbol = "xrp_usdt"
# symbol = "uni_btc"
# symbol = "ltc_usdt"
# symbol = "ltc_btc"
# symbol = "dash_usdt"
# symbol = "bnb_usdt"
# symbol = "bnb_btc"
# symbol = "bch_usdt"
# symbol = "ada_usdt"
# interval = Interval.MINUTE.value
# interval = Interval.MINUTE.value
interval = Interval.HOUR.value
# interval = Interval.DAY.value
window = 4

out_file = None


# out_file = open(output_file_path, "w")
# out_file.write("symbol,exchange,datetime,open,high,low,close,volume\n")


def on_bar(bar):
    print(bar.__dict__)


def on_window_bar(bar):
    arr = bar.get_arr()
    arr = [str(x) for x in arr]
    out_file.write(','.join(arr) + "\n")


def merge_bar_date(filename, window, interval):
    bg = BarGenerator(on_bar, window=window, on_window_bar=on_window_bar, interval=interval, quick_minute=0)
    ret = []
    f = open(filename, "r")
    for line in f:
        try:
            arr = line.strip().split(',')
            arr = arr[0:1] + arr[2:]
            bar = BarData.init_from_mysql_db(arr)
            bg.merge_bar_not_minute(bar)
        except Exception as ex:
            print("not ok, ex:{}".format(ex))

    f.close()
    return ret


def run():
    global symbol, interval, window, out_file
    # interval = Interval.HOUR.value
    windows = []
    period = "min"
    if interval == Interval.MINUTE.value:
        period = "min"
        windows = [1, 5, 10, 15, 30]
    elif interval == Interval.HOUR.value:
        period = "hour"
        windows = [1, 2, 4, 6, 12, 24]
    elif interval == Interval.DAY.value:
        period = "day"
        windows = [1, 3, 5, 10]

    for window in windows:
        # for window in [4]:
        input_file_path = ".tumbler/fix_{}_{}1.csv".format(symbol, period)
        if os.path.exists(input_file_path):
            output_file_path = ".tumbler/{}_{}_{}.csv".format(symbol, interval, window)

            print(output_file_path)

            out_file = open(output_file_path, "w")
            out_file.write("symbol,exchange,datetime,open,high,low,close,volume\n")

            print(merge_bar_date(input_file_path, window=window, interval=interval))

            out_file.close()


def loop1():
    global symbol
    from tumbler.service.mysql_service import MysqlService

    mysql_service_manager = MysqlService()

    day_symbols = mysql_service_manager.get_mysql_distinct_symbol(table='kline_1day')
    print(day_symbols)
    for symbol in day_symbols:
        if symbol.endswith("usdt"):
            print(symbol)
            run()


# loop1()

run()
