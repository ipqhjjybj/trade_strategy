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
    symbols = get_binance_symbols()
    log_service_manager.write_log(f"[symbols]:{symbols}!")
    end_dt = "2021-12-31 00:00:00"
    conn = mysql_service_manager.get_conn()
    cur = conn.cursor()
    for symbol in symbols:
        if symbol.endswith("_usdt"):
            try:
                sqll = f"delete from `tumbler`.`kline_1min` where symbol = '{symbol}' and datetime <= '{end_dt}'"
                log_service_manager.write_log(f"sqll:{sqll}")
                cur.execute(sqll)
                conn.commit()
            except Exception as ex:
                log_service_manager.write_log(f"[run_loop] ex:{ex}!")
            time.sleep(3)

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_loop()
