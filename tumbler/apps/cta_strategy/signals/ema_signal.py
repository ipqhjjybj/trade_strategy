# coding=utf-8

from tumbler.apps.cta_strategy.template import (
    CtaSignal
)
from tumbler.function.bar import ArrayManager
from tumbler.function.technique import Technique


class EmaSignal(CtaSignal):

    def __init__(self, _fast_window, _slow_window):
        super(EmaSignal, self).__init__()

        self.fast_window = _fast_window
        self.slow_window = _slow_window

        self.fast_ma0 = 0
        self.slow_ma0 = 0

        self.am = ArrayManager(max(self.fast_window, self.slow_window) + 30)

    def on_bar(self, bar):
        self.am.update_bar(bar)

        if not self.am.inited:
            self.set_signal_pos(0)

        fast_ma = Technique.x_average(self.am.close_array, self.fast_window)
        self.fast_ma0 = fast_ma[-1]

        slow_ma = Technique.x_average(self.am.close_array, self.slow_window)
        self.slow_ma0 = slow_ma[-1]

        cross_over = self.fast_ma0 > self.slow_ma0
        cross_below = self.fast_ma0 < self.slow_ma0

        if cross_over:
            self.set_signal_pos(1)
        elif cross_below:
            self.set_signal_pos(-1)
