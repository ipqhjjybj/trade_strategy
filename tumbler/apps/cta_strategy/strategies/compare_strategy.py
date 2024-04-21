# coding=utf-8

from copy import copy

import pandas as pd

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import BarData
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.function.bar import BarGenerator, PandasDeal
from tumbler.constant import Direction, Status, Interval


class CompareStrategy(CtaTemplate):
    """
    撮合部分对拍代码
    """
    author = "ipqhjjybj"
    class_name = "CompareStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    fixed = 1
    pos = 0
    target_pos = 0

    bar_period_factor = []     # func, window, interval,

    is_backtesting = False
    initDays = 20  # 初始化天数

    # 策略变量
    max_window = 20

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'symbol_pair',  # 交易对
        'exchange',  # 交易所
        'func',
        'fixed',
        'exchange_info',
        "pos"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(CompareStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)
        self.order_dict = {}
        self.is_backtesting = True

        self.df = None
        self.init_array = []

        self.ii = 0

    def on_init(self):
        pass

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        pass

    def compute_df(self):
        self.df = pd.DataFrame(self.init_array, columns=BarData.get_columns())
        self.df["pos"] = None

    def on_bar(self, bar: BarData):
        if self.df is not None:
            n = self.df.shape[0]
            self.df.loc[n] = bar.get_np_array(1)
        else:
            self.init_array.append(bar.get_dict())
            self.compute_df()

        self.df = self.func(self.df)
        self.ii += 1
        if self.ii == 10000:
            self.df.to_csv("c.log")

        self.target_pos = self.df["pos"][self.df.shape[0] - 1]
        if self.trading:
            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))
            self.to_target_pos(bar.close_price)

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

    def to_target_pos(self, order_price):
        if self.trading:
            buy_volume, sell_volume = self.get_already_send_volume()
            chazhi = self.target_pos - self.pos

            if len(self.order_dict.keys()) > 0:
                for vt_order_id, order in self.order_dict.items():
                    self.write_log("[to_target_pos not empty] vt_order_id:{} traded:{} volume:{} status:{}"
                                   .format(vt_order_id, order.traded, order.volume, order.status))
            if chazhi > 0:
                uu_volume = chazhi - buy_volume
                if uu_volume > 0:
                    if self.is_backtesting:
                        price = order_price * 1.2
                    else:
                        price = order_price * 1.005
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.buy(self.symbol_pair, self.exchange, price, uu_volume)
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order
            elif chazhi < 0:
                uu_volume = chazhi + sell_volume
                if uu_volume < 0:
                    if self.is_backtesting:
                        price = order_price * 0.8
                    else:
                        price = order_price * 0.995
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(uu_volume))
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order

    def on_order(self, order):
        msg = "[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded)
        self.write_log(msg)

        if order.traded > 0:
            self.write_important_log(msg)

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
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
