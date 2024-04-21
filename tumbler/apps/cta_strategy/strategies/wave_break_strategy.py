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


class WaveBreakStrategy(CtaTemplate):
    """
    这个策略是可以用的
    技术交易系统新概念里的策略
    ATR 7日交易ATR
    c -- 常数，2.8 到 3.1 之间的值
    ARC -- ATR值与常数C的乘积
    SIC -- 重要收盘点位，交易开始好后最有利的收盘价位
    SAR -- 止损反转点， SIC 的价位加上(或减去) ARC值即可

    规则：
    1.当价格收盘方向与SAR方向相反，就在收盘价处建仓
    2.止损反转点（SAR）
        （1）从多头到空头：如果收盘价低于交易以来的最高价减去ARC后的价位，即收盘价低于SAR，就在收盘价将多头交易转为空头交易。
        （2）从空头到多头：如果收盘价高于交易以来的最低价加上ARC后的价位，即收盘价低于SAR，就在收盘价将空头交易转为多头交易。
    """
    author = "ipqhjjybj"
    class_name = "WaveBreakStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    c = 3
    n_atr = 7

    interval = Interval.DAY.value
    bar_window = 1

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'c',
                  'n_atr',
                  'interval',
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(WaveBreakStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)
        self.am = ArrayManager(30)

        self.pos = 0
        self.long_line = 0
        self.short_line = 0

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

        if self.long_line > 0:
            if self.pos <= 0:
                if bar.close_price > self.long_line:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos) + 1)
        if self.short_line > 0:
            if self.pos >= 0:
                if bar.close_price < self.short_line:
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos) + 1)

    def on_window_bar(self, bar):
        # self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        atr_arr = talib.ATR(am.high, am.low, am.close, self.n_atr)
        val_atr = atr_arr[-1]
        arc = val_atr * self.c

        max_sic_close = talib.MAX(am.close, 7)[-1]
        min_sic_close = talib.MIN(am.close, 7)[-1]

        self.long_line = min_sic_close + arc
        self.short_line = max_sic_close - arc

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
