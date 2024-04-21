# coding=utf-8

from tumbler.apps.cta_strategy.template import (
    CtaSignal
)

from tumbler.function.bar import ArrayManager
from tumbler.function.technique import Technique


class HaitunSignal(CtaSignal):

    def __init__(self, _fast_window, _slow_window, _macd_window):
        super(HaitunSignal, self).__init__()

        self.fast_window = _fast_window
        self.slow_window = _slow_window
        self.macd_window = _macd_window

        self.am = ArrayManager(max(self.fast_window, self.slow_window, self.macd_window) + 40)

    def on_bar(self, bar):
        self.am.update_bar(bar)

        if not self.am.inited:
            self.set_signal_pos(0)

        ma_short_value = Technique.x_average(self.am.close_array, self.fast_window)
        ma_long_value = Technique.x_average(self.am.close_array, self.slow_window)
        (macd_value, avg_macd , macd_diff) = Technique.macd(self.am.close_array, self.fast_window, self.slow_window, self.macd_window)

        cond = 0
        if self.am.close_array[-1] > ma_long_value[-1] and macd_value[-1] > avg_macd[-1] and avg_macd[-1] > 0:
            cond = 1
        elif self.am.close_array[-1] > ma_long_value[-1] and macd_value[-1] < avg_macd[-1] and macd_value[-1] > 0:
            cond = 0
        elif self.am.close_array[-1] < ma_long_value[-1] and macd_value[-1] < avg_macd[-1] and avg_macd[-1] < 0:
            cond = -1
        elif self.am.close_array[-1] < ma_long_value[-1] and macd_value[-1] > avg_macd[-1] and macd_value[-1] < 0:
            cond = 0

        self.set_signal_pos(cond)
