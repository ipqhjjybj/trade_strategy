# coding=utf-8
import time
from datetime import datetime, timedelta

from tumbler.service.mysql_service import MysqlService
from tumbler.data.download.download_manager import DownloadManager
from tumbler.data.binance_data import BinanceClient
from tumbler.constant import Interval

if __name__ == "__main__":
    mysql_service_manager = MysqlService()
    binance_client = BinanceClient()
    manager = DownloadManager(mysql_service_manager, binance_client)
    manager.check_all_kline(include_symbols=[], not_include_symbols=[], suffix="_usdt",
                            start_datetime=datetime.now() - timedelta(days=90), end_datetime=None,
                            periods=[Interval.DAY.value, Interval.HOUR.value])
