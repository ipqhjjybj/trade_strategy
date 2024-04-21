# coding=utf-8

from tumbler.apps.cta_strategy.template import (
    CtaSignal
)

from tumbler.function.bar import ArrayManager


class RsiSignal(CtaSignal):

    def __init__(self, _rsi_window, _rsi_level):
        super(RsiSignal, self).__init__()

        self.rsi_window = _rsi_window
        self.rsi_level = _rsi_level

        self.rsi_long = 50 + self.rsi_level
        self.rsi_short = 50 - self.rsi_level

        self.am = ArrayManager(self.rsi_window + 30)

    def on_bar(self, bar):
        self.am.update_bar(bar)

        if not self.am.inited:
            self.set_signal_pos(0)

        rsi_value = self.am.rsi(self.rsi_window)

        if rsi_value >= self.rsi_long:
            self.set_signal_pos(1)
        elif rsi_value <= self.rsi_short:
            self.set_signal_pos(-1)
        else:
            self.set_signal_pos(0)
