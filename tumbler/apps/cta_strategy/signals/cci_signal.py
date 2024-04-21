# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
    CtaSignal
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class CciSignal(CtaSignal):

    def __init__(self, _cci_window, _cci_level):
        self.cci_window = _cci_window
        self.cci_level = _cci_level

        self.cci_long = self.cci_level
        self.cci_short = -self.cci_level

        self.am = ArrayManager(40)

    def on_bar(self, bar):
        self.am.update_bar(bar)

        if not self.am.inited:
            self.set_signal_pos(0)

        cci_value = self.am.cci(self.cci_window)

        if cci_value >= self.cci_long:
            self.set_signal_pos(1)
        elif cci_value <= self.cci_short:
            self.set_signal_pos(-1)
        else:
            self.set_signal_pos(0)
