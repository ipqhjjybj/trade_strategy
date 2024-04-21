# coding=utf-8

import time
from datetime import datetime
from copy import copy, deepcopy
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


class TsmMiddleSupportAndResistanceStrategy(CtaTemplate):
    """
    TSM 中午支撑突破系统
    """
    author = "ipqhjjybj"
    class_name = "TsmMiddleSupportAndResistanceStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    bar_window = 12

    pos = 0

    break_out_only = False

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'break_out_only',  # input_only
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(TsmMiddleSupportAndResistanceStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value, quick_minute=2)

        self.today_high = 0
        self.today_low = 0
        self.today_high_close = 0
        self.today_low_close = 0

        self.pos = 0
        self.signal = 0
        self.zone = 0.25

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
        is_new_day = self.bg.is_new_day(bar)
        is_new_afternoon = self.bg.is_new_afternoon(bar)
        self.bg.update_bar(bar)

        if is_new_day:
            self.today_high = bar.high_price
            self.today_low = bar.low_price
            self.today_high_close = bar.close_price
            self.today_low_close = bar.close_price

        self.today_high = max(self.today_high, bar.high_price)
        self.today_high_close = max(self.today_high_close, bar.close_price)
        self.today_low = min(self.today_low, bar.low_price)
        self.today_low_close = min(self.today_low_close, bar.close_price)

        if bar.after_middle_time():
            if self.pos >= 0:
                if bar.low_price <= self.today_low:
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 200, abs(self.pos) + 1)
            elif self.pos <= 0:
                if bar.high_price >= self.today_high:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 200, abs(self.pos) + 1)

            if self.break_out_only is False and self.pos >= 0 and self.pos == self.signal and \
                    bar.close_price > self.today_high - self.zone * (self.today_high - self.today_low):
                self.sell(self.symbol_pair, self.exchange, bar.close_price - 200, abs(self.pos) + 1)

            elif self.break_out_only is False and self.pos <= 0 and self.pos == self.signal and \
                    bar.close_price < self.today_low + self.zone * (self.today_high - self.today_low):
                self.buy(self.symbol_pair, self.exchange, bar.close_price + 200, abs(self.pos) + 1)

        self.signal = self.pos

    def on_window_bar(self, bar):
        pass

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
