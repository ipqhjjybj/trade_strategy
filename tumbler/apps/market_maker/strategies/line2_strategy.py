# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus


class Line2Strategy(MarketMakerTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "Line2Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"
    line_rate = 10
    cut_rate = 3

    pos = 0

    parameters = ['strategy_name',              # 策略加载的唯一性名字
                  'class_name',                 # 类的名字
                  'author',                     # 作者
                  'symbol_pair',                # 交易对
                  'line_rate',                  # 多少涨幅或者跌幅确定线
                  'cut_rate'                    # 下单后止盈止损间距 
                ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]


    def __init__(self, mm_engine, strategy_name, settings):
        super(Line2Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.line_point = None
        self.line_direction = None

        self.pos = 0

        self.pre_high = 0
        self.pre_high_bar = None
        self.pre_low = 99999999
        self.pre_low_bar = None

        self.pre_bar = None

        self.send_order_price = 0

    def on_init(self):
        pass

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_tick(self):
        pass

    def compute_line(self, bar):
        if self.line_point is None:
            if self.pre_high < bar.high_price:
                self.pre_high_bar = copy(bar)
                self.pre_high = bar.high_price

            if self.pre_low > bar.low_price:
                self.pre_low_bar = copy(bar)
                self.pre_low = bar.low_price

            if self.pre_high > self.pre_low * (1 + self.line_rate * 2 / 100.0):
                self.line_point = (self.pre_high + self.pre_low) / 2.0

                if self.pre_high_bar.datetime > self.pre_low_bar.datetime:
                    self.line_direction = Direction.LONG.value
                else:
                    self.line_direction = Direction.SHORT.value
        else:
            if self.line_direction == Direction.LONG.value:
                self.pre_high = max(self.pre_high, bar.high_price)
                self.pre_low = bar.low_price

                if bar.close_price < self.line_point:
                    if self.pre_high > self.pre_low * (1 + self.line_rate * 2 / 100.0):
                        self.line_direction = Direction.SHORT.value
                        self.line_point = self.pre_low * (1 + self.line_rate / 100.0)
                    else:
                        self.line_point = self.pre_high * (1 - self.line_rate / 100.0)
            else:
                self.pre_high = bar.high_price
                self.pre_low = min(self.pre_low, bar.low_price)

                if bar.close_price > self.line_point:
                    if self.pre_high > self.pre_low * (1 + self.line_rate * 2 / 100.0):
                        self.line_direction = Direction.LONG.value
                        self.line_point = self.pre_high * (1 - self.line_rate  / 100.0)
                    else:
                        self.line_point = self.pre_low * (1 + self.line_rate / 100.0)

        self.write_log("bar.high:{},bar.low:{},pre_high:{}, pre_low:{} line_point:{} line_direction:{}".format(bar.high_price ,bar.low_price ,self.pre_high, self.pre_low, self.line_point, self.line_direction))
        self.pre_bar = copy(bar)

    def on_bar(self, bar):
        #self.write_log("[bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        if self.line_point:
            if self.line_direction == Direction.LONG.value:
                if self.pos == 0:
                    self.short(self.symbol_pair, self.exchange, bar.close_price - 100, 1)
                    self.send_order_price = bar.close_price 
                elif self.pos > 0:
                    self.short(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))
                elif self.pos < 0:
                    cover_price = self.send_order_price * (1 - self.cut_rate / 100.0)
                    cut_price = self.send_order_price * (1 + self.cut_rate / 100.0)
                    if bar.close_price > cut_price or bar.close_price < cover_price:
                        self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))

            else:
                if self.pos == 0:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, 1)
                    self.send_order_price = bar.close_price

                elif self.pos > 0:
                    sell_price = self.send_order_price * (1 + self.cut_rate / 100.0)
                    cut_price = self.send_order_price * (1 - self.cut_rate / 100.0)

                    if bar.close_price > sell_price or bar.close_price < cut_price:
                        self.short(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))

                elif self.pos < 0:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))

        self.compute_line(bar)

    def on_stop_order(self, stop_order):
        self.write_log("[on_stop_order] {},{},{},{},{},{},{},{}".format(stop_order.vt_symbol, stop_order.direction, stop_order.offset,
                stop_order.price, stop_order.volume, stop_order.vt_order_id, stop_order.vt_order_ids, stop_order.status))
        

    def on_order(self, order):
        if order.status == Status.SUBMITTING.value:
            return

    def on_trade(self, trade):

        if trade.direction == Direction.LONG.value:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume

        self.write_trade(trade)


    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.datetime, trade.vt_symbol, trade.vt_trade_id, trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)



