# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

import talib

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class EmaRsiV1Strategy(CtaTemplate):
    """
    金叉有信号，然后 atr 背离入场, 死叉出
    """
    author = "ipqhjjybj"
    class_name = "EmaRsiV1Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    bar_window = 1

    pos = 0

    fast_length = 5
    slow_length = 60

    rsi_length = 14

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'fast_length',  # 快线长度
                  'slow_length',  # 慢线长度
                  'rsi_length',   # rsi长度
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(EmaRsiV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value, quick_minute=2)
        self.am = ArrayManager(100)

        self.high_list = []
        self.low_list = []

        self.pos = 0

        self.cross_over = False
        self.cross_down = False

        self.limit_order_dict = {}

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def cancel_all_limit_order(self):
        need_cancel_sets = []
        for vt_order_id, order in self.limit_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_bar(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

    def on_window_bar(self, bar):
        # self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        short_arr = talib.EMA(am.close, timeperiod=self.fast_length)
        long_arr = talib.EMA(am.close, timeperiod=self.slow_length)

        atr_arr = talib.RSI(am.close, timeperiod=self.rsi_length)

        if short_arr[-2] < long_arr[-2] and short_arr[-1] > long_arr[-1]:
            self.cross_over = True
            self.cross_down = False

        if short_arr[-2] > long_arr[-2] and short_arr[-1] < long_arr[-1]:
            self.cross_down = True
            self.cross_over = False

        entry_long_signal = False
        entry_short_signal = False

        if atr_arr[-3] <= min(atr_arr[-5:-1]):
            entry_long_signal = True

        if atr_arr[-3] >= max(atr_arr[-5:-1]):
            entry_short_signal = True

        if self.pos == 0:
            if entry_long_signal and self.cross_over:
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos) + 1)
            elif entry_short_signal and self.cross_down:
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos) + 1)

        elif self.pos > 0:
            if self.cross_down:
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))
        elif self.pos < 0:
            if self.cross_over:
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))

    def on_stop_order(self, stop_order):
        pass

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

    def on_trade(self, trade):
        if trade.direction == Direction.LONG.value:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
