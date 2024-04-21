# coding=utf-8

from datetime import datetime, time
from copy import copy
from collections import defaultdict
from threading import Lock
from enum import Enum

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData, BarData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class RBreakerStrategy(CtaTemplate):
    """
    经典策略 r-breaker
    """
    author = "ipqhjjybj"
    class_name = "RBreakerStrategy"

    symbol_pair = "btc_usd"
    exchange = "BITMEX"

    setup_coef = 0.35
    break_coef = 0.25
    enter_coef_1 = 1.07
    enter_coef_2 = 0.07

    fixed_size = 1

    bar_window = 1

    multiplier = 3

    buy_break = 0  # 突破买入价
    sell_setup = 0  # 观察卖出价
    sell_enter = 0  # 反转卖出价
    buy_enter = 0  # 反转买入价
    buy_setup = 0  # 观察买入价
    sell_break = 0  # 突破卖出价

    last_day_bar = None

    exit_time = time(hour=23, minute=55)

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'setup_coef',  #
                  'break_coef',  #
                  'enter_coef_1',  #
                  'enter_coef_2'  #

                  ]

    # 需要保存的运行时变量
    variables = ['inited', 'trading']

    def __init__(self, mm_engine, strategy_name, settings):
        super(RBreakerStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.DAY.value)
        self.am = ArrayManager(50)

        self.stop_order_dict = {}

        self.bars = []

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
        self.bg.update_bar(bar)
        self.am.update_bar(bar)

        if not self.am.inited:
            return

        if self.last_day_bar is None:
            return

        all_stop_ids = self.stop_order_dict.keys()
        for stop_id in all_stop_ids:
            self.cancel_order(stop_id)

        self.stop_order_dict.clear()

        if bar.datetime.time() < self.exit_time:
            if self.pos == 0:

                if bar.close_price > self.sell_setup:
                    stop_lists = self.buy(self.symbol_pair, self.exchange, self.buy_break, self.fixed_size, stop=True)
                    for stop_order_id, stop_order in stop_lists:
                        self.stop_order_dict[stop_order_id] = stop_order

                    # stop_lists = self.short(self.symbol_pair, self.exchange, self.sell_enter, self.multiplier * self.fixed_size, stop=True)
                    # for stop_order_id, stop_order in stop_lists:
                    #     self.stop_order_dict[stop_order_id] = stop_order

                elif bar.close_price < self.buy_setup:

                    stop_lists = self.short(self.symbol_pair, self.exchange, self.sell_break, self.fixed_size,
                                            stop=True)
                    for stop_order_id, stop_order in stop_lists:
                        self.stop_order_dict[stop_order_id] = stop_order

                    # stop_lists = self.buy(self.symbol_pair, self.exchange, self.buy_enter, self.multiplier * self.fixed_size, stop=True)
                    # for stop_order_id, stop_order in stop_lists:
                    #     self.stop_order_dict[stop_order_id] = stop_order

        else:
            if self.pos > 0:
                order_list = self.sell(self.symbol_pair, self.exchange, bar.close_price * 0.99, abs(self.pos))

            elif self.pos < 0:
                order_list = self.cover(self.symbol_pair, self.exchange, bar.close_price * 1.01, abs(self.pos))

    def on_window_bar(self, bar):
        print("on_window_bar,{},{},{},{},{}".format(bar.datetime, bar.open_price, bar.high_price, bar.low_price,
                                                    bar.close_price))
        self.last_day_bar = copy(bar)

        self.buy_setup = self.last_day_bar.low_price - self.setup_coef * (
                    self.last_day_bar.high_price - self.last_day_bar.close_price)
        self.sell_setup = self.last_day_bar.high_price + self.setup_coef * (
                    self.last_day_bar.close_price - self.last_day_bar.low_price)

        self.buy_enter = (self.enter_coef_1 / 2.0) * (
                    self.last_day_bar.high_price + self.last_day_bar.low_price) - self.enter_coef_2 * self.last_day_bar.high_price
        self.sell_enter = (self.enter_coef_1 / 2.0) * (
                    self.last_day_bar.high_price + self.last_day_bar.low_price) - self.enter_coef_2 * self.last_day_bar.low_price

        self.buy_break = self.sell_setup + self.break_coef * (self.sell_setup - self.buy_setup)
        self.sell_break = self.buy_setup - self.break_coef * (self.sell_setup - self.buy_setup)

        # print("buy_break:{},sell_setup:{},sell_enter:{}".format(self.buy_break,self.sell_setup,self.sell_enter))
        # print("buy_enter:{},buy_setup:{},sell_break:{}".format(self.buy_enter,self.buy_setup,self.sell_break))

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
