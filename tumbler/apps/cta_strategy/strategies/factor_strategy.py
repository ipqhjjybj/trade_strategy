# coding=utf-8

from copy import copy

import pandas as pd
import numpy as np

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import BarData
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator
from tumbler.constant import Direction, Status, Interval
from tumbler.function.technique import PD_Technique


class FactorStrategy(CtaTemplate):
    """
    因子策略 ， 利用pandas快速 回测一个因子
    因子值需要处理到 只有 -1 到 1之间，利于合并处理
    1、加载初始数据
    2、每根K线过来，合并，计算新的因子值，因子值 > 0.5 则开仓，< 0.5 则平仓, < -0.5开空仓，> -0.5平空仓
    3、计算收益情况
    """
    author = "ipqhjjybj"
    class_name = "FactorStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    fixed = 1
    pos = 0
    target_pos = 0

    compute_factor = None

    is_backtesting = False
    initDays = 20  # 初始化天数

    # 策略变量
    bar_window = 4
    max_window = 20

    interval = Interval.HOUR.value

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'symbol_pair',  # 交易对
        'exchange',  # 交易所
        'compute_factor',  # 外部因子计算函数
        'bar_window',  # 多少小时周期
        'interval',  #
        'is_backtesting',  # 是实盘还是回测
        'fixed',
        'exchange_info',
        "pos"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(FactorStrategy, self).__init__(mm_engine, strategy_name, settings)
        if self.interval == Interval.HOUR.value:
            self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                                   interval=Interval.HOUR.value, quick_minute=1)
        elif self.interval == Interval.MINUTE.value:
            self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                                   interval=Interval.MINUTE.value, quick_minute=0)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.order_dict = {}

        self.init_array = []
        self.df = None
        self.factor_nums = 1

    def on_init(self):
        self.write_log("on_init")
        if not self.is_backtesting:
            self.write_log("load_bar")
            self.load_bar(60)
            self.compute_df()
            self.df = self.compute_factor(self.df)
            n = self.df.shape[0] - 1
            if self.df["pos"][n] == np.NAN:
                self.target_pos = 0
            else:
                self.target_pos = self.df["pos"][n] * self.fixed
            self.write_log("on_init after")
            self.write_log("self.df:{}".format(self.df))
            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))

    def compute_df(self):
        self.df = pd.DataFrame(self.init_array, columns=BarData.get_columes())
        self.init_factor(self.df)

    def work_df(self, bar: BarData):
        if self.df is not None:
            n = self.df.shape[0]
            self.df.loc[n] = bar.get_np_array(self.factor_nums)
        else:
            self.init_array.append(bar.get_dict())
            self.compute_df()
            n = self.df.shape[0] - 1
        self.df = self.compute_factor(self.df)
        self.target_pos = self.df["pos"][n] * self.fixed

    def init_factor(self, df):
        df["pos"] = None

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.tick_send_order(tick)
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.write_log("bar:{}".format(bar.get_dict()))
        self.bg.update_bar(bar)
        if self.trading:
            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))
            self.to_target_pos(bar)

    def on_window_bar(self, bar: BarData):
        self.write_log("on_window_bar:{}".format(bar.get_dict()))
        if not self.is_backtesting and self.trading:
            self.write_log("[on_window_bar] df:{}".format(self.df))
        if not self.trading:
            self.init_array.append(bar.get_dict())
        else:
            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))
            self.work_df(bar)
            self.to_target_pos(bar)

    def get_already_send_volume(self):
        buy_volume, sell_volume = 0, 0
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                if order.direction == Direction.LONG.value:
                    buy_volume += order.volume
                else:
                    sell_volume += order.volume
        return buy_volume, sell_volume

    def cancel_all_orders(self):
        need_cancel_sets = []
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def to_target_pos(self, bar):
        if self.trading:
            buy_volume, sell_volume = self.get_already_send_volume()
            chazhi = self.target_pos - self.pos

            if chazhi > 0:
                uu_volume = chazhi - buy_volume
                if uu_volume > 0:
                    price = bar.close_price * 1.005
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.buy(self.symbol_pair, self.exchange, price, uu_volume)
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order
            elif chazhi < 0:
                uu_volume = chazhi + sell_volume
                if uu_volume < 0:
                    price = bar.close_price * 0.995
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(uu_volume))
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos += new_traded
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)
            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

    def on_trade(self, trade):
        self.write_log("[on_trade info] trade:{},{},{},{}\n".format(trade.vt_symbol, trade.order_id, trade.direction,
                                                                    trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
