# coding=utf-8

from datetime import datetime, timedelta
import time

from tumbler.record.binance_client import BinanceClient
from tumbler.record.binancef_client import BinancefClient
from tumbler.record.huobis_client import HuobisClient
from tumbler.record.okexs_client import OkexsClient

from tumbler.constant import Interval
from tumbler.data.binance_data import BinanceClient
from tumbler.service import mongo_service_manager
from tumbler.scheduler.daily_figure_scheduler import DailyCoinsScheduler
from tumbler.service import log_service_manager


def run():
    """
    dailyCoinScheduler 是处理 所有期货USDT合约的
    :return:
    """
    s = DailyCoinsScheduler()
    while True:
        log_service_manager.write_log("Daily download future Binance Future USDT")
        try:
            s.run()
        except Exception as ex:
            log_service_manager.write_log("ex:{}".format(ex))
        time.sleep(3600 * 24)


run()

