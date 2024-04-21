# coding=utf-8
import time

from datetime import datetime, timedelta

from tumbler.object import BarData
from tumbler.constant import Exchange, Interval
from tumbler.function import get_vt_key
from tumbler.service import mongo_service_manager

from tumbler.data.binance_data import BinanceClient
from tumbler.function.bar import BarGenerator

from tumbler.service import log_service_manager

from fix_bar_date import fix_all_data


out_file = None


def on_bar(bar):
    print(bar.__dict__)


def on_window_bar(bar):
    global out_file
    arr = bar.get_arr()
    arr = [str(x) for x in arr]
    out_file.write(','.join(arr) + "\n")


def get_bar(symbol, interval, end_dt):
    if interval == Interval.MINUTE.value:
        if symbol in ["btc_usdt"]:
            data = mongo_service_manager.load_bar_data(symbol, "BITMEX", interval, datetime(2017, 1, 1), end_dt)
        else:
            data = mongo_service_manager.load_bar_data(symbol, "BINANCE", interval, datetime(2017, 1, 1), end_dt)
        filepath = "./.tumbler/{}_{}.csv".format(symbol, "min1")
    else:
        data = mongo_service_manager.load_bar_data(symbol, "BINANCE", interval, datetime(2017, 1, 1), end_dt)
        filepath = "./.tumbler/{}_{}.csv".format(symbol, "hour1")

    log_service_manager.write_log("[get_bar] symbol:{} interval:{} end_dt:{} filepath:{}"
                                  .format(symbol, interval, end_dt, filepath))
    f = open(filepath, "w")
    for bar in data:
        arr = [bar.symbol, bar.exchange, bar.datetime,
               bar.open_price, bar.high_price, bar.low_price, bar.close_price, bar.volume]
        arr = [str(x) for x in arr]
        f.write(','.join(arr) + "\n")
    f.close()


def merge_bar_date(symbol, interval, window=24):
    global out_file
    if interval == Interval.DAY.value:
        period = "day"
    elif interval == Interval.MINUTE.value:
        period = "min"
    elif interval == Interval.HOUR.value:
        period = "hour"
    else:
        period = "min"

    input_file_path = ".tumbler/fix_{}_{}1.csv".format(symbol, period)
    output_file_path = ".tumbler/{}_{}_{}.csv".format(symbol, interval, window)
    log_service_manager.write_log("[merge_bar_date] {}->{}".format(input_file_path, output_file_path))
    out_file = open(output_file_path, "w")
    out_file.write("symbol,exchange,datetime,open,high,low,close,volume\n")
    bg = BarGenerator(on_bar, window=window, on_window_bar=on_window_bar, interval=interval, quick_minute=1)
    f = open(input_file_path, "r")
    for line in f:
        try:
            arr = line.strip().split(',')
            arr = arr[0:1] + arr[2:]
            bar = BarData.init_from_mysql_db(arr)
            bg.merge_bar_not_minute(bar)
        except Exception as ex:
            log_service_manager.write_log("not ok, ex:{}".format(ex))

    f.close()
    out_file.close()


def daily_run():
    while True:
        try:
            log_service_manager.write_log("[daily_run] now go to run download!")
            for symbol in ["btc_usdt", "eth_usdt", "bnb_usdt", "bnb_usdt",
                           "ltc_usdt", "bch_usdt", "dash_usdt", "xrp_usdt", "ada_usdt"]:
                for interval in [Interval.HOUR.value]:
                    now = datetime.now()
                    before = now - timedelta(days=3)
                    b = BinanceClient()
                    b.download_save_mongodb(symbol=symbol, _start_datetime=before, _end_datetime=now, interval=interval)
                    get_bar(symbol=symbol, interval=interval, end_dt=now)
                    fix_all_data(symbol, "hour1")

                    merge_bar_date(symbol=symbol, interval=interval, window=1)
                    merge_bar_date(symbol=symbol, interval=interval, window=2)
                    merge_bar_date(symbol=symbol, interval=interval, window=4)
                    merge_bar_date(symbol=symbol, interval=interval, window=6)
                    merge_bar_date(symbol=symbol, interval=interval, window=12)
                    merge_bar_date(symbol=symbol, interval=interval, window=24)

            log_service_manager.write_log("[daily_run] finished one day!")
        except Exception as ex:
            log_service_manager.write_log("[daily_run] ex:{}".format(ex))

        time.sleep(3600 * 24)


if __name__ == '__main__':
    daily_run()

