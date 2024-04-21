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


class MacdAtrV2Strategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    """
    author = "ipqhjjybj"
    class_name = "MacdAtrV2Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    bar_window = 1

    pos = 0

    n_bar_macd = 4
    atr_period = 10
    atr_xishu = 0.5

    n_bar_max_leave = 20

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'n_bar_macd',  # 多少根macd bar线
                  'atr_xishu',  # 系数
                  'n_bar_max_leave',  # 离场信号
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MacdAtrV2Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value, quick_minute=2)
        self.am = ArrayManager(60)

        self.high_list = []
        self.low_list = []

        self.pos = 0

        self.slow_ma = 0
        self.sell_line = 0
        self.buy_line = 0

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

        macd, macdsignal, macdhist = talib.MACD(am.close, fastperiod=12, slowperiod=26, signalperiod=9)
        atr_arr = talib.ATR(am.high, am.low, am.close, timeperiod=14)
        _, max_arr = talib.MINMAX(am.high_array, timeperiod=self.n_bar_max_leave)
        min_arr, _ = talib.MINMAX(am.low_array, timeperiod=self.n_bar_max_leave)

        cross_over = False
        cross_down = False
        if macdhist[-3] <= min(macdhist[-5:-1]):
            cross_over = True

        if macdhist[-3] >= max(macdhist[-5:-1]):
            cross_down = True

        if cross_over:
            self.low_list.append(am.low_array[-3])
            if len(self.low_list) > self.n_bar_macd:
                self.low_list.pop(0)

        if cross_down:
            self.high_list.append(am.high_array[-3])
            if len(self.high_list) > self.n_bar_macd:
                self.high_list.pop(0)

        if self.high_list:
            up_line = max(self.high_list) + atr_arr[-1] * self.atr_xishu
        else:
            up_line = bar.close_price * 100

        if self.low_list:
            down_line = max(self.low_list) + atr_arr[-1] * self.atr_xishu
        else:
            down_line = 0

        cover_long_signal = False
        cover_short_signal = False
        if bar.high_price >= max_arr[-2]:
            cover_short_signal = True
        if bar.low_price <= min_arr[-2]:
            cover_long_signal = True

        if self.pos == 0:
            if bar.close_price > up_line:
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos) + 1)
            elif bar.close_price < down_line:
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos) + 1)
        elif self.pos > 0:
            if cover_long_signal:
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))
        elif self.pos < 0:
            if cover_short_signal:
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
