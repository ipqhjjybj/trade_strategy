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


class NetGridStrategy(CtaTemplate):
    # TODO
    """
    网格挂单策略
    突破网格后，下一个市价单止损，盈亏平衡后全部平仓，然后重新布网

    先放弃吧，网格很难做出赚钱的策略
    """
    author = "ipqhjjybj"
    class_name = "NetGridStrategy"

    bar_window = 1
    interval = Interval.HOUR.value

    # 策略参数
    side_mode = Direction.SHORT.value       # 选择做多/做空模式
    first_hold_mode = False                 # 选择启动时是否买入底仓
    symbol = "btc_usdt"                     # 选择交易品种，Fmex仅支持BTC永续合约
    exchange = "BINANCE"                    # 交易所
    first_hold = 100                        # 买入底仓数量
    step = 20                               # 设置网格间距
    one_hand = 10                           # 设置单个网格下单量

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol',  # 交易对
                  'exchange',  # 交易所
                  'first_hold_mode',    # 选择做多/做空模式
                  'first_hold',  # 买入底仓数量
                  'step',   # 设置网格间距
                  'one_hand',   # 设置单个网格下单量
                  'interval',  # 周期
                  'bar_window'  # bar线
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(NetGridStrategy, self).__init__(mm_engine, strategy_name, settings)
        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)

        self.net_num = self.first_hold / self.one_hand  # 设置网格数量
        self.hold_limit = self.net_num * self.one_hand + self.first_hold  # 计算最大持仓量量

        self.last_price = 0

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

        self.put_grid_orders(bar.close_price)

    def on_window_bar(self, bar):
        pass

    def first_do(self, recent_price):
        if abs(self.pos) <= self.first_hold and self.first_hold_mode:
            if self.side_mode == Direction.LONG.value:
                price = recent_price + self.step
                volume = self.first_hold - self.pos
                if volume >= 0:
                    self.send_order(self.symbol, self.exchange, Direction.LONG.value,
                                    Offset.OPEN.value, price, volume)
                else:
                    self.write_log(f"[put_grid_orders] long, volume error: {volume}")
            else:
                price = recent_price - self.step
                volume = self.first_hold + self.pos
                if volume >= 0:
                    self.send_order(self.symbol, self.exchange, Direction.SHORT.value,
                                    Offset.CLOSE.value, price, volume)
                else:
                    self.write_log(f"[put_grid_orders] short, volume error: {volume}")

    def put_grid_orders(self, recent_price):
        if recent_price < self.last_price - self.step:
            # 如果可以做多
            if self.pos < self.hold_limit:
                price = recent_price + self.step
                volume = self.one_hand
                self.send_order(self.symbol, self.exchange, Direction.LONG.value, Offset.OPEN.value, price, volume)
                self.write_log(f"[put_grid_orders] price:{price}, volume:{volume}, hold_limit:{self.hold_limit}")

                self.last_price = recent_price
            # 如果不能做多
            elif self.pos >= self.hold_limit:
                price = recent_price - self.step
                volume = self.pos * 2
                self.send_order(self.symbol, self.exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume)
                self.write_log(f"[put_grid_orders] reverse short!")

                # 之后进入对冲锁仓状态，直到盈亏平衡点， 或者回到网格区间


        elif recent_price > self.last_price + self.step:
            # 如果可以做空
            if self.pos > self.hold_limit:
                price = recent_price - self.step
                volume = self.one_hand
                self.send_order(self.symbol, self.exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume)
                self.write_log(f"[put_grid_orders] price:{price}, volume:{volume}, hold_limit:{self.hold_limit}")

                self.last_price = recent_price
            elif self.pos <= self.hold_limit:
                price = recent_price + self.step
                volume = abs(self.pos) * 2
                self.send_order(self.symbol, self.exchange, Direction.LONG.value, Offset.OPEN.value, price, volume)
                self.write_log(f"[put_grid_orders] reverse long!")

                # 之后进入对冲锁仓状态，直到盈亏平衡点， 或者回到网格区间

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

