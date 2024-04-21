# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class Auto1MarketStrategy(CtaTemplate):
    """
    这个策略 机械做市，每分钟发买单跟卖单， 每天8点平掉
    """
    author = "ipqhjjybj"
    class_name = "Auto1MarketStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    fixed_spread = 0.1
    fixed_volume = 1
    price_tick = 0.01

    avg_price = 0

    # 策略参数
    bar_window = 5

    parameters = [
        'strategy_name',
        'class_name',
        'author',
        'vt_symbols_subscribe',
        'pos',
        'fixed_spread',
        'fixed_volume',
        'price_tick'
    ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading',
                 'avg_price'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(Auto1MarketStrategy, self).__init__(mm_engine, strategy_name, settings)
        self.bg = BarGenerator(self.on_bar, window=self.bar_window,
                               on_window_bar=self.on_window_bar, interval=Interval.MINUTE.value)

        self.am = ArrayManager(40)

        self.fixed_spread_price = 0
        self.limit_order_dict = {}
        self.pre_pos = -1111111111

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def cancel_all_orders(self):
        need_cancel_sets = []
        for vt_order_id, order in self.limit_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_bar(self, bar):
        self.bg.update_bar(bar)
        self.fixed_spread_price = get_round_order_price(bar.close_price * self.fixed_spread / 100.0, self.price_tick)
        if bar.datetime.hour == 8 and 0 <= bar.datetime.minute <= 5:
            self.write_log("[on_bar] end day bar:{}".format(bar.__dict__))
            self.cancel_all_orders()
            if self.pos > 0:
                list_orders = self.sell(self.symbol_pair, self.exchange, bar.close_price - self.fixed_spread_price,
                                        abs(self.pos))
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order
            elif self.pos < 0:
                list_orders = self.buy(self.symbol_pair, self.exchange, bar.close_price + self.fixed_spread_price,
                                       abs(self.pos))
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order
        else:
            if self.pre_pos != self.pos:
                list_orders = self.buy(self.symbol_pair, self.exchange, bar.close_price - self.fixed_spread_price,
                                       self.fixed_volume)
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order

                list_orders = self.sell(self.symbol_pair, self.exchange, bar.close_price + self.fixed_spread_price,
                                        self.fixed_volume)
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order

            self.pre_pos = self.pos

    def on_window_bar(self, bar):
        am = self.am
        am.update_bar(bar)

    def compute_avg_price(self, new_trade_price, new_trade_volume, new_trade_direction):
        if new_trade_direction == Direction.LONG.value:
            if self.pos >= 0:
                self.avg_price = (self.avg_price * self.pos + new_trade_price * new_trade_volume) / (
                        self.pos + new_trade_volume)
                self.pos += new_trade_volume
            else:
                if abs(self.pos) < new_trade_volume:
                    self.avg_price = new_trade_price
                self.pos += new_trade_volume

        else:
            if self.pos > 0:
                if self.pos < new_trade_volume:
                    self.avg_price = new_trade_price
                self.pos -= new_trade_volume
            else:
                self.avg_price = (self.avg_price * abs(self.pos) + new_trade_price * new_trade_volume) / (
                        abs(self.pos) + new_trade_volume)
                self.pos -= new_trade_volume

        self.write_log("pos:{}, avg_price:{}".format(self.pos, self.avg_price))

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.limit_order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.limit_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.compute_avg_price(order.price, new_traded, order.direction)
                self.limit_order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.limit_order_dict.pop(order.vt_order_id)
            else:
                bef_order = self.limit_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.compute_avg_price(order.price, new_traded, order.direction)
                self.limit_order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.limit_order_dict.pop(order.vt_order_id)

    def on_trade(self, trade):
        self.write_log("[on_trade info] trade:{},{},{},{}\n".format(trade.vt_symbol, trade.order_id, trade.direction,
                                                                    trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
