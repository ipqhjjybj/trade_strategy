# coding=utf-8

from datetime import datetime

from tumbler.apps.monitor.base import (
    APP_NAME
)
from tumbler.constant import Exchange
from tumbler.engine import BaseEngine
from tumbler.event import EVENT_TRADE_LOG, EVENT_STRATEGY_VARIABLES_LOG
from tumbler.function import FilePrint


class MonitorEngine(BaseEngine):
    """ 监控引擎，输出不同的策略报告，以及每日仓位净值等"""
    default_settings = {
        "record_assets": ["smt", "usdt", "ruff"],
        "exchanges": [Exchange.HUOBI.value]
        # "exchanges":[Exchange.HUOBI.value, Exchange.BINANCE.value, Exchange.GATEIO.value]
    }

    def __init__(self, main_engine, event_engine):
        super(MonitorEngine, self).__init__(main_engine, event_engine, APP_NAME)
        self.setting = self.default_settings

        self.bef_hour = 0
        self.bef_day = 0

        self.account_file_day = FilePrint("account.log", "day")
        self.account_file_hour = FilePrint("account_h.log", "hour")

        self.strategy_log_file = FilePrint("monitor.log", "strategy")

        self.trade_file_dict = {}  # 打印日志输出的映射 strategy_name:print_file
        self.register_event()

    def load_setting(self, i_setting):
        self.setting = i_setting

    def register_event(self):
        self.event_engine.register(EVENT_TRADE_LOG, self.process_trade_log)
        self.event_engine.register(EVENT_STRATEGY_VARIABLES_LOG, self.process_strategy_log)

    def process_strategy_log(self, event):
        strategy_log = event.data
        t = datetime.now()
        self.strategy_log_file.write('{}:{}\n'.format(t.strftime("%Y-%m-%d %H:%M:%S"), strategy_log))

    def process_trade_log(self, event):
        trade_log = event.data

        fp = self.trade_file_dict.get(trade_log.strategy_name)
        if fp is None:
            fp = FilePrint('{}.csv'.format(trade_log.strategy_name), "trade")
            self.trade_file_dict[trade_log.strategy_name] = fp

        fp.write(trade_log.get_txt_msg())

    def close(self):
        for key, fp in self.trade_file_dict.items():
            fp.close()
