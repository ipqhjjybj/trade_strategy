# coding=utf-8

from enum import Enum
from itertools import product
from tumbler.object import Direction
from tumbler.service.log_service import log_service_manager

APP_NAME = "BacktestingEngine"

EVENT_CS_LOG = "backLog"
EVENT_CS_STRATEGY = "csStrategy"
STOPORDER_PREFIX = "8btc_stop"


class BacktestingMode(Enum):
    """
    Direction of order/trade/position.
    """
    BAR = "BAR"  # bar级别回测
    TICK = "TICK"  # tick级别回测


class OptimizationSetting:
    """
    Setting for runnning optimization.
    """

    def __init__(self):
        """"""
        self.params = {}
        self.target_name = ""

    def add_parameter(self, name, start, end=None, step=None):
        if not end and not step:
            self.params[name] = [start]
            return

        if start >= end:
            log_service_manager.write_log("start need less than end")
            return

        if step <= 0:
            log_service_manager.write_log("step need bigger than 0")
            return

        value = start
        value_list = []

        while value <= end:
            value_list.append(value)
            value += step

        self.params[name] = value_list

    def set_target(self, target_name):
        self.target_name = target_name

    def generate_setting(self):
        """
        产生 暴力循环的 参数迭代配置
        """
        keys = self.params.keys()
        values = self.params.values()
        products = list(product(*values))

        settings = []
        for p in products:
            setting = dict(zip(keys, p))
            settings.append(setting)

        return settings

    def generate_setting_ga(self):
        """
        产生 遗传学习算法的 参数迭代配置
        """
        settings_ga = []
        settings = self.generate_setting()
        for d in settings:
            param = [tuple(i) for i in d.items()]
            settings_ga.append(param)
        return settings_ga


class DailyResult:
    """"""

    def __init__(self, date, close_price):
        """"""
        self.date = date
        self.close_price = close_price
        self.pre_close = 0

        self.trades = []
        self.trade_count = 0

        self.start_pos = 0
        self.end_pos = 0

        self.turnover = 0
        self.commission = 0
        self.slippage = 0

        self.trading_pnl = 0
        self.holding_pnl = 0
        self.total_pnl = 0
        self.net_pnl = 0

    def add_trade(self, trade):
        self.trades.append(trade)

    def calculate_pnl(self, pre_close, start_pos, size, rate, slippage):
        self.pre_close = pre_close

        # Holding pnl is the pnl from holding position at day start
        self.start_pos = start_pos
        self.end_pos = start_pos
        self.holding_pnl = self.start_pos * (self.close_price - self.pre_close) * size

        # Trading pnl is the pnl from new trade during the day
        self.trade_count = len(self.trades)

        for trade in self.trades:
            if trade.direction == Direction.LONG.value:
                pos_change = trade.volume
            else:
                pos_change = -trade.volume

            turnover = trade.price * trade.volume * size

            self.trading_pnl += pos_change * (self.close_price - trade.price) * size
            self.end_pos += pos_change
            self.turnover += turnover
            self.commission += turnover * rate
            self.slippage += trade.volume * size * slippage

        # Net pnl takes account of commission and slippage cost
        self.total_pnl = self.trading_pnl + self.holding_pnl
        self.net_pnl = self.total_pnl - self.commission - self.slippage

    def get_string(self):
        arr = []
        for trade in self.trades:
            arr.append((trade.direction, trade.volume, trade.price))
        msg = "{},{},{},{},{},{},{},{},{},{},{},{},{}".format(self.date, self.close_price, self.pre_close,
                                                              self.start_pos, self.end_pos, self.turnover,
                                                              self.commission, self.slippage, \
                                                              self.trading_pnl, self.holding_pnl, self.total_pnl,
                                                              self.net_pnl, arr)
        return msg
