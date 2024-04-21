# encoding: UTF-8

from collections import defaultdict

from tumbler.constant import Direction


class StrategyPnlStat(object):
    """
    策略运行时的结果盈亏状态，用于优化策略
    """
    total_profit = 0  # 策略总体盈亏
    inner_now_pos = 0  # 当前仓位
    inner_avg_price = 0  # 当前均价

    max_total_profit = 0  # 最大盈亏
    total_trades = 0  # 总共on_trade的数量
    total_win_num = 0  # 总共盈利次数
    total_loss_num = 0  # 总共亏损次数

    con_win_num = 0  # 连续赚钱次数
    con_loss_num = 0  # 连续亏钱次数
    max_con_win_num = 0  # 最大连续赚钱次数
    max_con_loss_num = 0  # 最大连续亏钱次数

    max_position = 0  # 最大持仓

    down_profit = 0  # 利润回撤幅度
    down_profit_rate = 0  # 利润回撤百分比

    def __init__(self, _vt_symbol):
        self.vt_symbol = _vt_symbol

        self.win_rate_frequency = defaultdict(int)
        self.loss_rate_frequency = defaultdict(int)

        self.position_frequency = defaultdict(int)

        self.now_close_price = 0
        self.pre_win_or_loss = 0

        self.ta = 0
        self.tb = 0
        self.tc = 0
        self.td = 0
        self.te = 0
        self.tf = 0
        self.tg = 0
        self.th = 0
        self.tu = 0
        self.to = 0

        self.start_loss_trade_time = ""
        self.end_loss_trade_time = ""

        self.start_win_trade_time = ""
        self.end_win_trade_time = ""

        self.period_max_continue_win_time = ("", "")
        self.period_max_continue_loss_time = ("", "")

        self.save_ticker = None

    def get_down_profit_rate(self):
        return self.down_profit_rate

    def get_down_profit(self):
        return self.down_profit

    def get_debug(self):
        return {"ta": self.ta, "tb": self.tb, "tc": self.tc, "td": self.td, "te": self.te, "tf": self.tf, "tg": self.tg,
                "th": self.th, "tu": self.tu, "to": self.to}

    def on_trade(self, trade):
        self.position_frequency[self.inner_now_pos] += 1
        new_trade = 0
        if trade.direction == Direction.LONG.value:
            if self.inner_now_pos > 0:
                self.inner_avg_price = (
                                               self.inner_avg_price * self.inner_now_pos + trade.volume * trade.price)\
                                       * 1.0 / (trade.volume + self.inner_now_pos)
                self.ta += 1

            elif self.inner_now_pos < 0:
                if abs(self.inner_now_pos) > trade.volume:
                    self.total_profit += -1 * (trade.price - self.inner_avg_price) * trade.volume
                    new_trade = -1 * (trade.price - self.inner_avg_price) * trade.volume
                    self.tb += 1

                elif abs(self.inner_now_pos) < trade.volume:
                    self.total_profit += -1 * (trade.price - self.inner_avg_price) * abs(self.inner_now_pos)
                    new_trade = -1 * (trade.price - self.inner_avg_price) * abs(self.inner_now_pos)
                    self.inner_avg_price = trade.price

                    self.tc += 1
                else:
                    self.total_profit += -1 * (trade.price - self.inner_avg_price) * trade.volume
                    new_trade = -1 * (trade.price - self.inner_avg_price) * trade.volume
                    self.inner_avg_price = 0

                    self.td += 1
            else:
                self.inner_avg_price = trade.price

                self.te += 1

            self.inner_now_pos += trade.volume
        else:
            if self.inner_now_pos < 0:
                self.inner_avg_price = (self.inner_avg_price * abs(
                    self.inner_now_pos) + trade.volume * trade.price) * 1.0 / (trade.volume + abs(self.inner_now_pos))
                self.tf += 1
            elif self.inner_now_pos > 0:
                if self.inner_now_pos > trade.volume:
                    self.total_profit += (trade.price - self.inner_avg_price) * trade.volume
                    new_trade = (trade.price - self.inner_avg_price) * trade.volume

                    self.tg += 1
                elif self.inner_now_pos < trade.volume:
                    self.total_profit += (trade.price - self.inner_avg_price) * self.inner_now_pos
                    new_trade = (trade.price - self.inner_avg_price) * self.inner_now_pos
                    self.inner_avg_price = trade.price

                    self.th += 1
                else:
                    self.total_profit += (trade.price - self.inner_avg_price) * self.inner_now_pos
                    new_trade = (trade.price - self.inner_avg_price) * self.inner_now_pos
                    self.inner_avg_price = 0

                    self.tu += 1
            else:
                self.inner_avg_price = trade.price
                self.to += 1

            self.inner_now_pos -= trade.volume

        self.total_trades += 1

        win_or_loss = 0
        if new_trade > 0:
            win_or_loss = 1
        elif new_trade < 0:
            win_or_loss = -1

        if win_or_loss > 0:
            self.total_win_num += 1
            self.con_win_num += 1
            self.con_loss_num = 0

            self.win_rate_frequency[self.con_win_num] += 1

            if self.con_win_num == 1:
                self.start_win_trade_time = trade.trade_time

        if win_or_loss < 0:
            self.total_loss_num += 1
            self.con_win_num = 0
            self.con_loss_num += 1

            self.loss_rate_frequency[self.con_loss_num] += 1

            if self.con_loss_num == 1:
                self.start_loss_trade_time = trade.trade_time

        if self.max_con_win_num < self.con_win_num:
            self.max_con_win_num = self.con_win_num
            self.end_win_trade_time = trade.trade_time
            self.period_max_continue_win_time = (self.start_win_trade_time, self.end_win_trade_time)

        if self.max_con_loss_num < self.con_loss_num:
            self.max_con_loss_num = self.con_loss_num
            self.end_loss_trade_time = trade.trade_time
            self.period_max_continue_loss_time = (self.start_loss_trade_time, self.end_loss_trade_time)

        self.pre_win_or_loss = win_or_loss

        self.max_total_profit = max(self.max_total_profit, self.total_profit)
        if self.max_total_profit > 0:
            self.down_profit = (self.max_total_profit - self.total_profit)
            self.down_profit_rate = self.down_profit * 1.0 / self.max_total_profit

        self.max_position = max(self.max_position, abs(self.inner_now_pos))

    def on_tick(self, tick):
        self.now_close_price = tick.last_price

    def on_bar(self, bar):
        self.now_close_price = bar.close_price

    def get_realtime_pnl(self):
        return self.inner_now_pos * (self.now_close_price - self.inner_avg_price)

    def show_win_num_state(self):
        return str(self.win_rate_frequency.items())

    def show_loss_num_state(self):
        return str(self.loss_rate_frequency.items())

    def get_max_continue_win_time(self):
        return self.period_max_continue_win_time

    def get_max_continue_loss_time(self):
        return self.period_max_continue_loss_time

    def get_position_layout(self):
        return str(self.position_frequency.items())
