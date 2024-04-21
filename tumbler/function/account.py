# encoding: UTF-8

from copy import copy
from collections import defaultdict

import numpy as np

from tumbler.constant import Direction
from tumbler.function.order_math import get_round_order_price


class TradeManager(object):
    """
    策略运行时的结果盈亏状态，用于优化策略
    """

    inner_now_pos = 0  # 当前仓位
    inner_avg_price = 0  # 当前均价

    def __init__(self):
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

    def get_pos(self):
        return self.inner_now_pos

    def get_avg_price(self):
        return self.inner_avg_price

    def on_trade(self, trade):
        now_profit = 0
        new_trade = 0

        if trade.direction == Direction.LONG.value:
            if self.inner_now_pos > 0:
                self.inner_avg_price = (
                                               self.inner_avg_price * self.inner_now_pos + trade.volume * trade.price) \
                                       * 1.0 / (trade.volume + self.inner_now_pos)
                self.ta += 1

            elif self.inner_now_pos < 0:
                if abs(self.inner_now_pos) > trade.volume:
                    now_profit += -1 * (trade.price - self.inner_avg_price) * trade.volume
                    new_trade = -1 * (trade.price - self.inner_avg_price) * trade.volume
                    self.tb += 1

                elif abs(self.inner_now_pos) < trade.volume:
                    now_profit += -1 * (trade.price - self.inner_avg_price) * abs(self.inner_now_pos)
                    new_trade = -1 * (trade.price - self.inner_avg_price) * abs(self.inner_now_pos)
                    self.inner_avg_price = trade.price

                    self.tc += 1
                else:
                    now_profit += -1 * (trade.price - self.inner_avg_price) * trade.volume
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
                    now_profit += (trade.price - self.inner_avg_price) * trade.volume
                    new_trade = (trade.price - self.inner_avg_price) * trade.volume

                    self.tg += 1
                elif self.inner_now_pos < trade.volume:
                    now_profit += (trade.price - self.inner_avg_price) * self.inner_now_pos
                    new_trade = (trade.price - self.inner_avg_price) * self.inner_now_pos
                    self.inner_avg_price = trade.price

                    self.th += 1
                else:
                    now_profit += (trade.price - self.inner_avg_price) * self.inner_now_pos
                    new_trade = (trade.price - self.inner_avg_price) * self.inner_now_pos
                    self.inner_avg_price = 0

                    self.tu += 1
            else:
                self.inner_avg_price = trade.price
                self.to += 1

            self.inner_now_pos -= trade.volume
        return now_profit, new_trade

    def get_debug(self):
        return {"ta": self.ta, "tb": self.tb, "tc": self.tc, "td": self.td, "te": self.te, "tf": self.tf, "tg": self.tg,
                "th": self.th, "tu": self.tu, "to": self.to}


class StrategyPnlStat(object):
    """
    策略运行时的结果盈亏状态，用于优化策略
    """
    total_profit = 0  # 策略总体盈亏
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

        self.trade_manager = TradeManager()

        self.win_rate_frequency = defaultdict(int)
        self.loss_rate_frequency = defaultdict(int)

        self.position_frequency = defaultdict(int)

        self.now_close_price = 0
        self.pre_win_or_loss = 0

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
        return self.trade_manager.get_debug()

    def on_trade(self, trade):
        self.position_frequency[self.trade_manager.inner_now_pos] += 1

        now_profit, new_trade = self.trade_manager.on_trade(trade)
        self.total_profit += now_profit
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

        self.max_position = max(self.max_position, abs(self.trade_manager.get_pos()))

    def on_tick(self, tick):
        self.now_close_price = tick.last_price

    def on_bar(self, bar):
        self.now_close_price = bar.close_price

    def get_realtime_pnl(self):
        return self.trade_manager.get_pos() * (self.now_close_price - self.trade_manager.get_pos())

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


class CoinAccountManager(object):
    """
    币本位账户持仓计算，并将持仓转化成复利模式+币本位模式

    账户通过信号计算 实际应该导出的仓位
    @input:
        init_account_val 初始BTC当前数量
        init_real_pos 初始持仓数量(持有10张)
        contract_size 每张的持仓大小(100 USD)
        volume_tick 每张合约的最小单位
        leverage 杠杆比例
        max_signal_pos_size 最大的信号持仓

    @output
        输入信号持仓，根据
        real_pos = (1.0 * signal_pos / max_signal_pos) * (account_val * leverage * price / contract_size)
    """
    def __init__(self, init_account_val, max_signal_pos, contract_size, volume_tick,
                 leverage=1, fee_rate=0.0003):
        self.account_val = init_account_val
        self.contract_size = contract_size
        self.max_signal_pos = max_signal_pos
        self.volume_tick = volume_tick
        self.leverage = leverage
        self.signal_pos = 0
        self.ticker = None
        self.last_price = 0

        self.real_pos = 0
        self.fee_rate = fee_rate             # 手续费比率
        self.acc_account_income = 0         # 累计因为交易带来的资金增长

        self.trade_manager = TradeManager()

    def has_inited(self):
        return self.ticker is not None

    def on_tick(self, tick):
        self.ticker = copy(tick)
        self.last_price = (self.ticker.bid_prices[0] + self.ticker.ask_prices[0]) / 2.0

    def compute_real_pos(self):
        self.real_pos = (1.0 * self.signal_pos / self.max_signal_pos) *\
                        (self.account_val * self.leverage * self.last_price / self.contract_size)
        self.real_pos = get_round_order_price(self.real_pos, self.volume_tick)
        # print(self.signal_pos, self.max_signal_pos, self.account_val, self.leverage, self.last_price,
        #       self.contract_size, self.real_pos)
        if abs(self.real_pos - int(self.real_pos)) < 1e-12:
            self.real_pos = int(self.real_pos)
        return self.real_pos

    def on_signal_pos(self, pos):
        """
        传入信号仓位
        """
        if str(pos) == str(np.nan):
            self.signal_pos = 0
        else:
            self.signal_pos = pos
        #print("on_signal_pos pos:{} signal_pos:{}".format(pos, self.signal_pos))
        return self.compute_real_pos()

    def on_trade(self, trade):
        """
        推送交易数据
        """
        now_profit, new_trade = self.trade_manager.on_trade(trade)
        coin_gain = 1.0 * now_profit / trade.price * self.contract_size / trade.price
        self.account_val += coin_gain
        self.account_val -= trade.volume * self.contract_size / trade.price * self.fee_rate
        return self.account_val

    def get_account_val(self):
        return self.account_val

    def get_real_pos(self):
        return self.real_pos




