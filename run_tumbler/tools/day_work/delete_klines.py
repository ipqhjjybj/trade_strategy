# coding=utf-8

from tumbler.service.mysql_service import MysqlService
from tumbler.data.download.download_manager import DownloadManager
from tumbler.data.binance_data import BinanceClient

if __name__ == "__main__":
    mysql_service_manager = MysqlService()
    binance_client = BinanceClient()
    manager = DownloadManager(mysql_service_manager, binance_client)
    manager.delete_all_klines(suffix="_eth")
    #manager.delete_all_klines(suffix="_btc")

    #mysql_service_manager.delete_bars(symbols=["bnt_eth"])
