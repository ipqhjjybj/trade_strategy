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


class FreqTradeStrategy(CtaTemplate):
    """
    调用 FreqTrade策略
    """
    author = "ipqhjjybj"
    class_name = "FreqTradeStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    day_window = 1
    minute_window = 5

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'symbol_pair',  # 交易对
        'exchange',  # 交易所
        'day_window',  # 日周期
        'minute_window',  # 分钟周期
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
        super(FreqTradeStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.day_bg = BarGenerator(None, window=self.day_window, on_window_bar=self.on_day_bar,
                                   interval=Interval.DAY.value, quick_minute=2)
        self.min_bg = BarGenerator(None, window=self.minute_window, on_window_bar=self.on_min_bar,
                                   interval=Interval.MINUTE.value, quick_minute=2)

        self.target_pos = 0
        self.day_pos = 0
        self.day_flag = False
        self.am_day = ArrayManager(30)
        self.am_minute_period = ArrayManager(300)

    def on_init(self):
        self.write_log("on_init")

    def update_contracts(self):
        pass

    def on_start(self):
        self.write_log("[on_start]")

    def on_stop(self):
        self.write_log("[on_stop]")

    def on_tick(self, tick):
        pass

    def on_bar(self, bar: BarData):
        self.min_bg.update_bar(bar)
        self.day_bg.update_bar(bar)

        if self.am_day.inited and self.am_minute_period.inited:

            if self.pos < self.target_pos:
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, self.target_pos - self.pos)
            elif self.pos > self.target_pos:
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, self.pos - self.target_pos)

    def on_min_bar(self, bar: BarData):
        self.am_minute_period.update_bar(bar)

        if self.am_minute_period.inited:
            self.target_pos = self.min_func(self.am_minute_period)
            if not self.day_flag:
                self.target_pos = 0

    def on_day_bar(self, bar: BarData):
        self.am_day.update_bar(bar)

        if self.am_day.inited:
            self.day_flag = self.day_func(self.am_day)

    def on_order(self, order):
        msg = "[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded)
        self.write_log(msg)

    def on_trade(self, trade):
        self.write_important_log("[on_trade info] trade:{},{},{},{}\n"
                                 .format(trade.vt_symbol, trade.order_id, trade.direction, trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
