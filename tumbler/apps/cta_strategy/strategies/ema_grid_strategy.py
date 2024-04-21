# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock
from enum import Enum
from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)

from tumbler.constant import Direction, Interval, Offset
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class GridCondition(Enum):
    INIT = ""
    PUT_ORDER = "put_order"
    COVER_ORDER = "cover_order"


class EmaGridStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢（EMA）

    网格挂单策略，围绕中枢进行挂单
    1、EMA突破中枢的某个倍数，确定新中枢，原先的止损掉
    2、原先单子如果发现成交了，就放置相反的平多平空单
    3、如果平多平空单成交了，则重新挂网格单
    """
    author = "ipqhjjybj"
    class_name = "EmaGridStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    slow_window = 20

    spread_rate = 0.05
    start_put_rate = 0.01
    put_order_num = 10

    bar_window = 1
    fixed = 1
    interval = Interval.HOUR.value

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'fixed',  # 固定尺寸
                  'put_order_num',  # 放置的单子数
                  'slow_window',  # 慢线
                  'start_put_rate',    # 偏离比率
                  'spread_rate',    # 价差比率
                  'interval',   # 周期
                  'bar_window'  # bar线
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(EmaGridStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)
        self.am = ArrayManager(30)

        self.pos = 0

        self.slow_ma = 0

        self.center = 0

        self.condition_long_orders_arr = []
        self.condition_short_orders_arr = []
        self.long_orders_arr = []
        self.cover_long_orders_arr = []
        self.short_orders_arr = []
        self.cover_short_orders_arr = []

        for i in range(self.put_order_num):
            self.condition_long_orders_arr.append(GridCondition.INIT.value)
            self.condition_short_orders_arr.append(GridCondition.INIT.value)
            self.long_orders_arr.append([])
            self.cover_long_orders_arr.append([])
            self.short_orders_arr.append([])
            self.cover_short_orders_arr.append([])

        self.need_clear_order_ids = set([])

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def cancel_need_clear_orders(self):
        tmp_list_order_ids = list(self.need_clear_order_ids)
        for vt_order_id in tmp_list_order_ids:
            self.cancel_order(vt_order_id)

    def cancel_all_limit_orders(self):
        for i in range(self.put_order_num):
            if self.long_orders_arr[i]:
                order_list = self.long_orders_arr[i]
                for order in order_list:
                    if order.is_active():
                        self.need_clear_order_ids.add(order.vt_order_id)
                self.long_orders_arr[i] = []

        for i in range(self.put_order_num):
            if self.short_orders_arr[i]:
                order_list = self.short_orders_arr[i]
                for order in order_list:
                    if order.is_active():
                        self.need_clear_order_ids.add(order.vt_order_id)
                self.short_orders_arr[i] = []

        for i in range(self.put_order_num):
            if self.cover_long_orders_arr[i]:
                order_list = self.cover_long_orders_arr[i]
                for order in order_list:
                    if order.is_active():
                        self.need_clear_order_ids.add(order.vt_order_id)
                self.cover_long_orders_arr[i] = []

        for i in range(self.put_order_num):
            if self.cover_short_orders_arr[i]:
                order_list = self.cover_short_orders_arr[i]
                for order in order_list:
                    if order.is_active():
                        self.need_clear_order_ids.add(order.vt_order_id)
                self.cover_short_orders_arr[i] = []

        self.cancel_need_clear_orders()

    def check_stop_loss(self, ticker_price):
        center_now = self.slow_ma
        zhisun_val = self.put_order_num * self.spread_rate * self.center
        if center_now > self.center + zhisun_val or center_now < self.center - zhisun_val:
            # 触发止损
            self.cancel_all_limit_orders()

            if center_now < self.center:
                price = ticker_price * 0.99
                volume = self.put_order_num * self.fixed
                self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume)
            elif center_now > self.center:
                price = ticker_price * 1.01
                volume = self.put_order_num * self.fixed
                self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value, Offset.OPEN.value, price, volume)

            for i in range(self.put_order_num):
                self.condition_long_orders_arr[i] = GridCondition.INIT.value
                self.condition_short_orders_arr[i] = GridCondition.INIT.value

            self.center = center_now

    def on_bar(self, bar):
        self.bg.update_bar(bar)

        self.check_stop_loss(bar.close_price)
        self.put_grid_orders()
        self.put_cover_orders()

    def put_cover_orders(self):
        if self.center:
            self.write_log("[put_cover_orders] self.center:{} self.pos:{}".format(self.center, self.pos))
            for i in range(self.put_order_num):
                if self.condition_long_orders_arr[i] in [GridCondition.COVER_ORDER.value]:
                    if not self.cover_long_orders_arr[i]:
                        price = self.center - (self.start_put_rate + self.spread_rate * (i - 1)) * self.center
                        volume = self.fixed
                        list_orders = self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value,
                                                      Offset.OPEN.value, price, volume)

                        self.write_log("[put_cover_orders] short price:{} volume:{}".format(price, volume))
                        self.cover_long_orders_arr[i] = []
                        for vt_order_id, order in list_orders:
                            self.cover_long_orders_arr[i].append(copy(order))

                if self.condition_short_orders_arr[i] in [GridCondition.COVER_ORDER.value]:
                    if not self.cover_short_orders_arr[i]:
                        price = self.center + (self.start_put_rate + self.spread_rate * (i - 1)) * self.center
                        volume = self.fixed
                        list_orders = self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value,
                                                      Offset.OPEN.value, price, volume)

                        self.write_log("[put_cover_orders] long price:{} volume:{}".format(price, volume))
                        self.cover_short_orders_arr[i] = []
                        for vt_order_id, order in list_orders:
                            self.cover_short_orders_arr[i].append(copy(order))

    def put_grid_orders(self):
        if self.center:
            self.write_log("[put_grid_orders] self.center:{} self.pos:{}".format(self.center, self.pos))
            for i in range(self.put_order_num):
                if self.condition_long_orders_arr[i] in [GridCondition.INIT.value, GridCondition.PUT_ORDER.value]:
                    if not self.long_orders_arr[i]:
                        # symbol, exchange, direction, offset, price, volume
                        price = self.center - (self.start_put_rate + self.spread_rate * i) * self.center
                        volume = self.fixed
                        list_orders = self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value,
                                                       Offset.OPEN.value, price, volume)

                        self.write_log("[put_grid_orders] long price:{} volume:{}".format(price, volume))
                        self.long_orders_arr[i] = []
                        for vt_order_id, order in list_orders:
                            self.long_orders_arr[i].append(copy(order))

                if self.condition_short_orders_arr[i] in [GridCondition.INIT.value, GridCondition.PUT_ORDER.value]:
                    if not self.short_orders_arr[i]:
                        price = self.center + (self.start_put_rate + self.spread_rate * i) * self.center
                        volume = self.fixed
                        list_orders = self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value,
                                                      Offset.CLOSE.value, price, volume)

                        self.write_log("[put_grid_orders] short price:{} volume:{}".format(price, volume))
                        self.short_orders_arr[i] = []
                        for vt_order_id, order in list_orders:
                            self.short_orders_arr[i].append(copy(order))

    def on_window_bar(self, bar):
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        slow_ma = Technique.x_average(am.close_array, self.slow_window)
        self.slow_ma = slow_ma[-1]

        self.write_log("[on_window_bar] slow_ma:{}".format(self.slow_ma))

        if not self.center:
            self.center = self.slow_ma
            self.put_grid_orders()

    def on_stop_order(self, stop_order):
        pass

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.vt_order_id in self.need_clear_order_ids:
            if not order.is_active():
                self.need_clear_order_ids.remove(order.vt_order_id)
                self.write_log(f"[on_order] need_clear_order_ids remove {order.vt_order_id}")
            else:
                self.write_log(f"[on_order] ????? {order.vt_order_id}")
        else:
            find_flag = 0
            for i in range(self.put_order_num):
                order_list = self.long_orders_arr[i]
                for j in range(len(order_list)):
                    if order_list[j].vt_order_id == order.vt_order_id:
                        find_flag = 1
                        self.write_log(f"[on_order] find long_orders_arr {order.vt_order_id}")
                        order_list[j] = copy(order)
                        if not order.is_active():
                            order_list.pop(j)
                            self.condition_long_orders_arr[i] = GridCondition.COVER_ORDER.value
                            self.write_log(f"[on_order] long_orders_arr, condition_long_orders_arr[{i}]"
                                           f" {self.condition_long_orders_arr[i]}")
                        break

            for i in range(self.put_order_num):
                order_list = self.short_orders_arr[i]
                for j in range(len(order_list)):
                    if order_list[j].vt_order_id == order.vt_order_id:
                        find_flag = 2
                        self.write_log(f"[on_order] find short_orders_arr {order.vt_order_id}")
                        order_list[j] = copy(order)
                        if not order.is_active():
                            order_list.pop(j)
                            self.condition_short_orders_arr[i] = GridCondition.COVER_ORDER.value
                            self.write_log(f"[on_order] short_orders_arr, condition_short_orders_arr[{i}]"
                                           f" {self.condition_short_orders_arr[i]}")
                        break

            for i in range(self.put_order_num):
                order_list = self.cover_long_orders_arr[i]
                for j in range(len(order_list)):
                    if order_list[j].vt_order_id == order.vt_order_id:
                        find_flag = 3
                        self.write_log(f"[on_order] find cover_long_orders_arr {order.vt_order_id}")
                        order_list[j] = copy(order)
                        if not order.is_active():
                            order_list.pop(j)
                            self.condition_long_orders_arr[i] = GridCondition.PUT_ORDER.value
                            self.write_log(f"[on_order] cover_long_orders, condition_long_orders_arr[{i}]"
                                           f" {self.condition_long_orders_arr[i]}")
                        break

            for i in range(self.put_order_num):
                order_list = self.cover_short_orders_arr[i]
                for j in range(len(order_list)):
                    if order_list[j].vt_order_id == order.vt_order_id:
                        find_flag = 4
                        self.write_log(f"[on_order] find cover_short_orders_arr {order.vt_order_id}")
                        order_list[j] = copy(order)
                        if not order.is_active():
                            order_list.pop(j)
                            self.condition_short_orders_arr[i] = GridCondition.PUT_ORDER.value
                            self.write_log(f"[on_order] cover_short_orders, condition_long_orders_arr[{i}]"
                                           f" {self.condition_long_orders_arr[i]}")
                        break

            self.write_log(f"[on_order] end! {find_flag} {order.vt_order_id}")

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
