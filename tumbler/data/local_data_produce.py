# coding=utf-8

from datetime import datetime, timedelta

from tumbler.constant import Exchange, Interval, Status, Direction
from tumbler.service import mongo_service_manager, mysql_service_manager
from tumbler.service.mysql_service import MysqlService
from tumbler.function.order_math import my_str
from tumbler.data.binance_data import BinanceClient
from tumbler.service import log_service_manager


def get_bar_from_mongo(symbol, interval, file_path):
    data = mongo_service_manager.load_bar_data(symbol, "BINANCE", interval, datetime(2017, 1, 1),
                                               datetime(2022, 12, 20))

    f = open("./.tumbler/{}_{}.csv".format(symbol, file_path), "w")
    for bar in data:
        arr = bar.get_arr()
        arr = [str(x) for x in arr]
        f.write(','.join(arr) + "\n")
    f.close()


def get_bar_from_mysql(symbol, interval, file_path):
    mysql_service_manager = MysqlService()
    data = mysql_service_manager.get_bars(symbols=[symbol], period=interval,
                                          start_datetime=datetime(2010, 1, 1),
                                          end_datetime=datetime.now() + timedelta(hours=3))

    filename = "./.tumbler/{}_{}.csv".format(symbol, file_path)
    print(filename, len(data))
    f = open(filename, "w")
    for bar in data:
        arr = bar.get_arr()
        arr = [str(x) for x in arr]
        f.write(','.join(arr) + "\n")

    f.close()


def get_minute_bar_from_mongo(symbol="eth_usdt"):
    return get_bar_from_mongo(symbol=symbol, interval=Interval.MINUTE.value, file_path="min1")


def get_hour_bar_from_mongo(symbol="btc_usdt"):
    return get_bar_from_mongo(symbol=symbol, interval=Interval.HOUR.value, file_path="hour1")


def get_minute_bar_from_mysql(symbol="eth_usdt"):
    return get_bar_from_mysql(symbol=symbol, interval=Interval.MINUTE.value, file_path="min1")


def get_hour_bar_from_mysql(symbol="btc_usdt"):
    return get_bar_from_mysql(symbol=symbol, interval=Interval.HOUR.value, file_path="hour1")


def get_day_bar(symbol="btc_usdt"):
    mysql_service_manager = MysqlService()
    data = mysql_service_manager.get_bars(symbols=["btc_usdt"], period=Interval.DAY.value,
                                          start_datetime=datetime(2010, 1, 1), end_datetime=datetime.now())
    f = open("./.tumbler/{}_day1.csv".format(symbol), "w")
    # f.write("symbol,exchange,datetime,open,high,low,close,volume\n")
    for bar in data:
        arr = [bar.symbol, Exchange.BINANCE.value, bar.datetime, bar.open_price, bar.high_price,
               bar.low_price, bar.close_price,
               bar.volume]
        arr = [my_str(x) for x in arr]
        f.write(','.join(arr) + "\n")
    f.close()


def get_all_day_bar():
    mysql_service_manager = MysqlService()
    data = mysql_service_manager.get_bars(symbols=[], period=Interval.DAY.value,
                                          start_datetime=datetime(2010, 1, 1), end_datetime=datetime.now())
    f = open("./.tumbler/symbols_day.csv", "w")
    for bar in data:
        arr = [bar.symbol, bar.datetime, bar.open_price, bar.high_price, bar.low_price, bar.close_price,
               bar.volume]
        arr = [my_str(x) for x in arr]
        f.write(','.join(arr) + "\n")
    f.close()


def go_to_fix_mongodb(symbol, interval, start_time=datetime(2017, 1, 1), end_time=datetime(2022, 12, 20)):
    bar_arr = mongo_service_manager.load_bar_data(symbol, "BINANCE", interval, start_time, end_time)
    inc = timedelta(minutes=1)
    if interval == Interval.MINUTE.value:
        inc = timedelta(minutes=1)
    elif interval == Interval.HOUR.value:
        inc = timedelta(hours=1)
    elif interval == Interval.DAY.value:
        inc = timedelta(days=1)

    if len(bar_arr) == 0:
        log_service_manager.write_log("[go_to_fix_mongodb] symbol:{} interval:{} bar_arr zero!"
                                      .format(symbol, interval))
        return

    need_fix_period = []
    pre_datetime = bar_arr[0].datetime
    for bar in bar_arr:
        now_datetime = bar.datetime

        if pre_datetime and pre_datetime + inc < now_datetime:
            need_fix_period.append([pre_datetime, now_datetime])
            log_service_manager.write_log("[go_to_fix_mongodb] pre_datetime:{} now_datetime:{}"
                                          .format(pre_datetime, now_datetime))
        pre_datetime = bar.datetime

    for before, now in need_fix_period:
        client = BinanceClient()
        n = client.download_save_mongodb(symbol=symbol, _start_datetime=before, _end_datetime=now, interval=interval)

        log_service_manager.write_log("[go_to_fix_mongodb] finished compare before, now, num:{}".format(n))
