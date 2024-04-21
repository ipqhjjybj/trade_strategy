# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.market_maker.template import (
    MarketMakerSignal
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager


class LineSignal(MarketMakerSignal):

    def __init__(self, _line_rate):
        self.line_rate = _line_rate
        self.line_point = None
        self.line_direction = None
        
        self.pre_high = 0
        self.pre_high_bar = None
        self.pre_low = 9999999
        self.pre_low_bar = None

        self.pre_bar = None

    def get_line_direction(self):
        return self.line_direction

    def get_line_point(self):
        return self.line_point

    def on_tick(self, tick):
        pass

    def on_bar(self, bar):
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

        self.pre_bar = copy(bar)
        