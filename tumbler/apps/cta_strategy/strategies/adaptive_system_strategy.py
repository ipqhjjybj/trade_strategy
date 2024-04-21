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


class AdaptiveSystemV1Strategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    dmi 决定趋势, ema介入
    """
    author = "ipqhjjybj"
    class_name = "AdaptiveSystemV1Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    bar_window = 1

    period = 10
    apower = 0.75
    highthreshold = 0.5
    lowthreshold = 1.05

    pos = 0

    parameters = [
                  'strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'period',
                  'apower',
                  'highthreshold',
                  'lowthreshold'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AdaptiveSystemV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value, quick_minute=2)
        self.am = ArrayManager(300)

        self.nhr_array = []
        self.nlr_array = []

        self.pos = 0

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

        atr_arr = talib.ATR(am.high, am.low, am.close, timeperiod=self.period)

        highrange = am.high_array[-1] - am.low_array[-self.period]
        lowrange = am.low_array[-1] - am.high_array[-self.period]

        nhr = highrange / pow(self.period, self.apower) / atr_arr[-1]
        nlr = lowrange / pow(self.period, self.apower) / atr_arr[-1]

        self.nhr_array.append(nhr)
        self.nlr_array.append(nlr)

        if len(self.nhr_array) > self.period:
            self.nhr_array.pop(0)

        if len(self.nlr_array) > self.period:
            self.nlr_array.pop(0)

        if len(self.nhr_array) >= self.period:
            hrmax = max(self.nhr_array)
            lrmax = min(self.nlr_array)

            if hrmax > self.highthreshold and lrmax < self.lowthreshold:
                if self.pos <= 0:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos) + 1)

            elif hrmax < self.highthreshold and lrmax > self.lowthreshold:
                if self.pos >= 0:
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos) + 1)

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
