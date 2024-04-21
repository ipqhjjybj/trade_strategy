# coding=utf-8
import time
from datetime import datetime, timedelta

from tumbler.service.mysql_service import MysqlService
from tumbler.data.download.download_manager import DownloadManager, get_binance_symbols
from tumbler.data.binance_data import BinanceClient
from tumbler.service import log_service_manager
from tumbler.constant import Interval


def run_loop():
    mysql_service_manager = MysqlService()
    binance_client = BinanceClient()
    manager = DownloadManager(mysql_service_manager, binance_client)

    while True:
        try:
            manager.check_all_kline(include_symbols=get_binance_symbols(), not_include_symbols=[], suffix="_usdt",
                                    start_datetime=datetime.now() - timedelta(days=3),
                                    end_datetime=datetime.now() + timedelta(days=1),
                                    periods=[Interval.DAY.value])
        except Exception as ex:
            log_service_manager.write_log(f"[run_loop] ex:{ex}")

        time.sleep(3600 * 12)


if __name__ == "__main__":
    run_loop()
