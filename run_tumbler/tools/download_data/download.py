# coding=utf-8

from datetime import datetime, timedelta
import time
import traceback
import requests

from tumbler.function import get_vt_key
from tumbler.object import BarData
from tumbler.constant import Exchange, Interval

from tumbler.service import mongo_service_manager

from tumbler.data.binance_data import BinanceClient
from tumbler.data.bitmex_data import BitmexClient


def download_binance_data():
    b = BinanceClient()

    #all_symbols = ["eth_usdt", "bch_usdt", "ltc_usdt"]
    all_symbols = ["btc_usdt"]
    #all_symbols = ["bnb_usdt", "eth_usdt"]
    #all_symbols = ["ltc_usdt", "bch_usdt", "dash_usdt", "xrp_usdt", "ada_usdt"]
    #all_symbols = ["ltc_usdt", "bch_usdt", "dash_usdt", "xrp_usdt"]
    #all_symbols = ["eth_usdt", "btc_usdt"]
    #all_symbols = ["eth_btc"]

    #periods = [Interval.MINUTE.value, Interval.HOUR.value, Interval.DAY.value]
    periods = [Interval.MINUTE.value]
    #periods = [Interval.HOUR.value, Interval.DAY.value]
    #periods = [Interval.DAY.value]
    #periods = [Interval.DAY.value]
    #periods = [Interval.HOUR.value]
    begin_start_dt = datetime(2017, 6, 1)
    #begin_start_dt = datetime(2020, 10, 1)
    begin_end_dt = datetime(2024, 12, 31)

    for symbol in all_symbols:
        for period in periods:
            start_dt = begin_start_dt
            end_dt = begin_end_dt
            while start_dt < end_dt:
                try:
                    if period in [Interval.DAY.value]:
                        tday = start_dt + timedelta(days=1000)
                    else:
                        tday = start_dt + timedelta(days=5)
                    ret = b.get_kline(symbol=symbol, period=period, start_datetime=start_dt, end_datetime=tday)
                    # for bar in ret:
                    #     print(bar.datetime, bar.interval, bar.symbol)

                    print("go to save db len:{}".format(len(ret)))
                    mongo_service_manager.save_bar_data(ret)

                    start_dt = tday
                    print("why start_dt:{}".format(start_dt))
                except Exception as ex:
                    print(ex)
                    continue


def download_bitmex_data():
    b = BitmexClient()

    all_symbols = ["XBTUSD"]
    #periods = [Interval.MINUTE.value, Interval.HOUR.value, Interval.DAY.value]
    periods = [Interval.MINUTE.value]
    start_dt = datetime(2013, 1, 1)
    end_dt = datetime(2020, 12, 31)

    for symbol in all_symbols:
        for period in periods:
            while start_dt < end_dt:
                try:
                    tday = start_dt + timedelta(days=5)
                    print("start_dt:{}, end_dt:{}".format(start_dt, tday))
                    ret = b.get_kline(symbol=symbol, interval=period, start_dt=start_dt, end_dt=tday)
                    mongo_service_manager.save_bar_data(ret)

                    start_dt = tday
                except Exception as ex:
                    print(ex)
                    continue


def test():
    b = BitmexClient()
    data = b.get_kline("XBTUSD", Interval.HOUR.value, start_dt=datetime(2014, 1, 1), end_dt=datetime(2016, 12, 2))
    print(data)


if __name__ == "__main__":
    download_binance_data()
    # download_bitmex_data()
    # test()
