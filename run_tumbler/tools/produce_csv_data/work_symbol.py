# coding=utf-8
from datetime import datetime

from tumbler.service.mysql_service import MysqlService
from tumbler.data.download.download_manager import DownloadManager
from tumbler.data.binance_data import BinanceClient
from tumbler.constant import Interval
from tumbler.data.download.download_manager import get_spot_symbols, get_binance_symbols

from datetime import datetime, timedelta
import os

from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.bar import BarGenerator
import tumbler.data.local_data_produce as ldp


def minute_replace(d):
    d = d.replace(second=0, microsecond=0)
    return d


def hour_replace(d):
    d = d.replace(minute=0, second=0, microsecond=0)
    return d


def day_replace(d):
    return d


def fix_data(origin_file, to_file, func, period="min1"):
    if period == "min1":
        inc = timedelta(minutes=1)
    elif period == "hour1":
        inc = timedelta(hours=1)
    elif period == "day1":
        inc = timedelta(days=1)

    print("{} -> {}".format(origin_file, to_file))
    pre_close = 0
    compare_d = None
    f_in = open(origin_file, "r")
    f_out = open(to_file, "w")
    for line in f_in:
        arr = line.strip().split(',')
        dtime = arr[2]
        try:
            d = datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
        except Exception as ex:
            d = datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S.%f')

        d = func(d)
        if compare_d is None:
            compare_d = d
        else:
            compare_d = compare_d + inc
            while compare_d < d:
                new_arr = [arr[0], arr[1], compare_d] + [pre_close] * 4 + [0]
                new_arr = [str(x) for x in new_arr]
                f_out.write((','.join(new_arr)) + "\n")
                compare_d = compare_d + inc

                print("go to fix {} {} compare_d:{} d:{}".format(arr[0], period, compare_d, d))
                # break

        arr[2] = d
        pre_close = arr[-2]
        arr = [str(x) for x in arr]
        f_out.write((','.join(arr)) + "\n")

    f_in.close()
    f_out.close()


def fix_all_data(symbol, period):
    origin_file = ".tumbler/{}_{}.csv".format(symbol, period)
    if os.path.exists(origin_file):
        to_file = ".tumbler/fix_{}_{}.csv".format(symbol, period)
        if period == "min1":
            fix_data(origin_file, to_file, minute_replace, period)
        elif period == "hour1":
            fix_data(origin_file, to_file, hour_replace, period)
        elif period == "day1":
            fix_data(origin_file, to_file, day_replace, period)

        os.remove(origin_file)


out_file = None


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


def merge_symbol(symbol, interval):
    global out_file
    # interval = Interval.HOUR.value
    windows = []
    period = "min"
    if interval == Interval.MINUTE.value:
        period = "min"
        windows = [1, 5, 10, 15, 30]
    elif interval == Interval.HOUR.value:
        period = "hour"
        windows = [1, 2, 4, 6, 8, 12, 24]
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


def work_single(symbol, interval=Interval.HOUR.value):
    mysql_service_manager = MysqlService()
    binance_client = BinanceClient()
    manager = DownloadManager(mysql_service_manager, binance_client)

    spot_symbols = [symbol]
    if interval == Interval.HOUR.value:
        manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
                                   periods=[Interval.HOUR.value],
                                   start_datetime=datetime(2021, 7, 1), end_datetime=datetime.now())

        ldp.get_hour_bar_from_mysql(symbol=symbol)

        fix_all_data(symbol, "hour1")

        merge_symbol(symbol, Interval.HOUR.value)
    elif interval == Interval.MINUTE.value:
        manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
                                   periods=[Interval.MINUTE.value],
                                   start_datetime=datetime(2021, 7, 1), end_datetime=datetime.now())

        ldp.get_minute_bar_from_mysql(symbol=symbol)

        fix_all_data(symbol, "min1")

        merge_symbol(symbol, Interval.MINUTE.value)
    else:
        pass


def filter_symbols(arr, suffix="_usdt"):
    return [symbol for symbol in arr if symbol.endswith(suffix) and not symbol.endswith("down_usdt")
            and not symbol.endswith("up_usdt") and not symbol.endswith("bear_usdt")
            and not symbol.endswith("bull_usdt")]


def work_all():
    mysql_service = MysqlService()

    symbols = mysql_service.get_mysql_distinct_symbol(table='kline_1day')
    symbols = filter_symbols(symbols, suffix="_usdt")

    for symbol in symbols:
        if symbol.endswith("usdt"):
            print(symbol)
            work_single(symbol)


if __name__ == "__main__":
    symbol = "btc_usdt"
    symbol = "eth_usdt"
    symbol = "sol_usdt"
    symbol = "bnb_usdt"
    # merge_symbol(symbol, Interval.MINUTE.value)
    # merge_symbol(symbol, Interval.HOUR.value)

    work_single(symbol, interval=Interval.HOUR.value)
    # work_single(symbol, interval=Interval.MINUTE.value)

    # work_all()
