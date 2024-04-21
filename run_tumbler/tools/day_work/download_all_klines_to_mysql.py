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

    spot_symbols = get_binance_symbols()
    # periods = [Interval.MINUTE.value]
    # periods = [Interval.DAY.value, Interval.HOUR.value]
    # spot_symbols = ["eth_usdt", "bnb_usdt"]
    # periods = [Interval.MINUTE.value]

    manager.check_all_kline(include_symbols=spot_symbols, suffix="_usdt",
                            periods=[Interval.DAY.value],
                            start_datetime=datetime(2017, 1, 1), end_datetime=datetime.now())

    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
    #                            periods=[Interval.DAY.value],
    #                            start_datetime=datetime(2017, 1, 1), end_datetime=datetime.now())

    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="",
    #                            periods=[Interval.DAY.value, Interval.HOUR.value])

    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_usdt",
    #                            periods=[Interval.MINUTE.value, Interval.HOUR.value])
    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_btc",
    #                            periods=[Interval.MINUTE.value, Interval.HOUR.value])
    # manager.recovery_all_kline(include_symbols=spot_symbols, suffix="_eth",
    #                            periods=[Interval.MINUTE.value, Interval.HOUR.value])

    # manager.recovery_all_kline(suffix="_usdt", periods=[Interval.DAY.value])
