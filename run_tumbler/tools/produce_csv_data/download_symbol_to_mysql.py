# coding=utf-8
from datetime import datetime

from tumbler.service.mysql_service import MysqlService
from tumbler.data.download.download_manager import DownloadManager
from tumbler.data.binance_data import BinanceClient
from tumbler.constant import Interval
from tumbler.data.download.download_manager import get_spot_symbols, get_binance_symbols

if __name__ == "__main__":
    mysql_service_manager = MysqlService()
    binance_client = BinanceClient()
    manager = DownloadManager(mysql_service_manager, binance_client)
    # manager.recovery_all_kline(include_symbols=["bnb_usdt"], periods=[Interval.DAY.value])
    # manager.recovery_all_kline(suffix="usdt")
    # manager.recovery_all_kline(suffix="_usdt", periods=[Interval.MINUTE.value])

    # spot_symbols = ["etc_usdt", "eth_usdt", "btc_usdt", "bnb_usdt", "link_usdt"]
    # spot_symbols = ["etc_usdt"]
    spot_symbols = ["btc_usdt","eth_usdt"]
    spot_symbols = ["btc_usdt"]
    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="",
    #                            periods=[Interval.DAY.value, Interval.HOUR.value],
    #
    #                            start_datetime=datetime(2021, 1, 1), end_datetime=datetime.now())
    # intervals = [Interval.DAY.value, Interval.HOUR.value, Interval.HOUR4.value]
    #
    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
    #                            periods=intervals,
    #                            start_datetime=datetime(2017, 1, 1),
    #                            end_datetime=datetime.now())

    # spot_symbols = get_binance_symbols()
    # intervals = [Interval.DAY.value, Interval.HOUR.value]
    # intervals = [Interval.DAY.value]
    intervals = [Interval.MINUTE.value, Interval.HOUR.value, Interval.DAY.value]
    intervals = [Interval.MINUTE.value]
    manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
                               periods=intervals,
                               start_datetime=datetime(2018, 3, 30),
                               end_datetime=datetime.now())

    # intervals = [Interval.HOUR4.value]
    # spot_symbols = get_binance_symbols()
    # manager.check_all_kline(include_symbols=spot_symbols, suffix="_usdt",
    #                         periods=intervals,
    #                         start_datetime=datetime(2017, 1, 1),
    #                         end_datetime=datetime.now())

    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="",
    #                            periods=[Interval.DAY.value, Interval.HOUR.value])

    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
    #                            periods=[Interval.MINUTE.value, Interval.HOUR.value])
    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_btc",
    #                            periods=[Interval.MINUTE.value, Interval.HOUR.value])
    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_eth",
    #                            periods=[Interval.MINUTE.value, Interval.HOUR.value])

    # manager.recovery_all_kline(suffix="_usdt", periods=[Interval.DAY.value])
