# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class HaitunStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "HaitunStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    fast_window = 12
    slow_window = 26
    macd_window = 9

    bar_window = 4

    fixed_size = 1

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'fast_window',  # 快线
                  'slow_window',  # 慢线
                  'macd_window',  # macd window
                  'bar_window'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(HaitunStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value)
        self.am = ArrayManager(50)

        self.pos = 0
        self.fixed_size = 1

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

    def on_window_bar(self, bar):
        # self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        ma_short_value = Technique.x_average(am.close_array, self.fast_window)
        ma_long_value = Technique.x_average(am.close_array, self.slow_window)
        (macd_value, avg_macd, macd_diff) = Technique.macd(am.close_array, self.fast_window, self.slow_window,
                                                           self.macd_window)

        cond = 0
        if am.close_array[-1] > ma_long_value[-1] and macd_value[-1] > avg_macd[-1] and avg_macd[-1] > 0:
            cond = 1
        elif am.close_array[-1] > ma_long_value[-1] and macd_value[-1] < avg_macd[-1] and macd_value[-1] > 0:
            cond = 0
        elif am.close_array[-1] < ma_long_value[-1] and macd_value[-1] < avg_macd[-1] and avg_macd[-1] < 0:
            cond = -1
        elif am.close_array[-1] < ma_long_value[-1] and macd_value[-1] > avg_macd[-1] and macd_value[-1] < 0:
            cond = 0

        if self.pos == 0:
            if cond > 0:
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))
            elif cond < 0:
                self.short(self.symbol_pair, self.exchange, bar.close_price - 100, self.fixed_size)
        elif self.pos > 0:
            if cond <= 0:
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))
            if cond < 0:
                self.short(self.symbol_pair, self.exchange, bar.close_price - 100, self.fixed_size)
        elif self.pos < 0:
            if cond >= 0:
                self.cover(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))
            if cond > 0:
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, self.fixed_size)

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
