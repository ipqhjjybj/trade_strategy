# coding=utf-8
from copy import copy


class StrategyStatistic(object):
    def __init__(self):
        self.bar_since_entry_num = 0
        self.pos = 0

        self.last_bar = None

    def new_pos(self, t_pos):
        if t_pos and t_pos == self.pos:
            self.bar_since_entry_num += 1
        if not t_pos:
            self.bar_since_entry_num = 0
        self.pos = t_pos

    def new_bar(self, bar):
        self.last_bar = copy(bar)

    @property
    def bar_since_entry(self):
        return self.bar_since_entry_num



