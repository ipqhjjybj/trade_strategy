# coding=utf-8

import talib

from tumbler.apps.cta_strategy.template import (
    CtaSignal
)
from tumbler.function.bar import ArrayManager


class DmiSignal(CtaSignal):

    def __init__(self, length):
        super(DmiSignal, self).__init__()

        self.length_window = length

        self.am = ArrayManager(self.length_window + 30)

    def on_bar(self, bar):
        self.am.update_bar(bar)

        if not self.am.inited:
            self.set_signal_pos(0)

        plus_di = talib.PLUS_DI(self.am.high_array, self.am.low_array,
                                self.am.close_array, timeperiod=self.length_window)
        minus_di = talib.MINUS_DI(self.am.high_array, self.am.low_array,
                                  self.am.close_array, timeperiod=self.length_window)

        cross_over = plus_di[-1] > minus_di[-1]
        cross_below = plus_di[-1] < minus_di[-1]

        if cross_over:
            self.set_signal_pos(1)
        elif cross_below:
            self.set_signal_pos(-1)
