# coding=utf-8

from copy import copy
from datetime import timedelta, datetime

import pandas as pd

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.object import BarData, TickData
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator, PandasDeal
from tumbler.constant import Direction, Status, Interval, Exchange
from tumbler.apps.cta_strategy.template import OrderSendModule
from tumbler.function.technique import PD_Technique
import tumbler.function.risk as risk
from tumbler.apps.cta_strategy.template import NewOrderSendModule
from tumbler.constant import TradeOrderSendType, CheckTradeAccountType


class FreqtradeRealStrategy(CtaTemplate):
    """
    FreqtradeRealStrategy
    """
    author = "ipqhjjybj"
    class_name = "FreqtradeRealStrategy"

    day_window = 1
    minute_window = 5
    minute_interval = Interval.MINUTE5.value

    init_days = 30

    vt_symbol = "btm_usdt.OKEX5"

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'vt_symbol',  # 交易的代码
        'exchange',  # 交易所
        'day_window',  # 日周期
        'minute_window',  # 分钟周期
        'minute_interval',  # 分钟间隔
        'min_func',  # 分钟func
        'day_func',
        'fixed',
        "pos",
        "init_days"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(FreqtradeRealStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.day_bg = BarGenerator(None, window=self.day_window, on_window_bar=self.on_day_bar,
                                   interval=Interval.DAY.value, quick_minute=2)
        self.min_bg = BarGenerator(self.on_bar, window=self.minute_window, on_window_bar=self.on_min_bar,
                                   interval=Interval.MINUTE.value, quick_minute=2)

        self.order_module = None

        self.target_pos = 0
        self.day_pos = 0
        self.day_flag = True
        self.am_day = ArrayManager(30)
        self.am_minute_period = ArrayManager(300)

        self.tick_run = risk.TimeWork(1)

    def on_init(self):
        self.write_log("[on_init] now go to load bars!")

        # day_bars = self.load_bar_online(vt_symbol=self.vt_symbol,
        #                                 days=self.init_days,
        #                                 interval=Interval.DAY.value)
        #

        min_bars = CtaTemplate.load_bar_online(vt_symbol=self.vt_symbol,
                                               days=self.init_days,
                                               interval=self.minute_interval)
        for bar in min_bars:
            self.on_min_bar(bar)

        # see init bar_data
        df = self.am_minute_period.to_pandas_data()
        df.to_csv("freqtrade.csv")

    def update_contracts(self):
        self.write_log("[update_contracts]")
        contract = self.get_contract(self.vt_symbol)
        if contract and not self.order_module:
            self.order_module = NewOrderSendModule(
                self, contract, 0, wait_seconds=5,
                send_order_type=TradeOrderSendType.POST_ONLY.value,
                check_account_type=CheckTradeAccountType.NOT_CHECK_ACCOUNT.value)

    def on_start(self):
        self.write_log("[on_start]")

    def on_stop(self):
        self.write_log("[on_stop]")

    def on_tick(self, tick):
        if self.tick_run.can_work():
            self.min_bg.update_tick(tick)
            if not self.order_module:
                self.update_contracts()
            else:
                self.order_module.on_tick(tick)

    def on_bar(self, bar: BarData):
        # self.write_log(f"[on_bar] "
        #                f" datetime:{bar.datetime}"
        #                f" open:{bar.open_price}, "
        #                f" high:{bar.high_price}, "
        #                f" low:{bar.low_price}, "
        #                f" close:{bar.close_price}")
        self.min_bg.update_bar(bar)
        # self.day_bg.update_bar(bar)

    def on_min_bar(self, bar: BarData):
        self.write_log(f"[on_min_bar] "
                       f" datetime:{bar.datetime}"
                       f" open:{bar.open_price}, "
                       f" high:{bar.high_price}, "
                       f" low:{bar.low_price}, "
                       f" close:{bar.close_price}")
        self.am_minute_period.update_bar(bar)

        if self.am_minute_period.inited:
            self.target_pos = self.min_func(self.am_minute_period)
            if self.trading:
                self.write_log(f"[on_min_bar] target_pos:{self.target_pos} now_pos:{self.order_module.get_now_pos()}!")
                self.order_module.set_to_target_pos(self.target_pos)

    def on_day_bar(self, bar: BarData):
        self.am_day.update_bar(bar)

        if self.am_day.inited:
            self.day_flag = self.day_func(self.am_day)

    def on_order(self, order):
        msg = "[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded)
        self.write_log(msg)

        self.order_module.on_order(order)

    def on_trade(self, trade):
        self.write_important_log("[on_trade info] trade:{},{},{},{}\n"
                                 .format(trade.vt_symbol, trade.order_id, trade.direction, trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
