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


class MartinStrategy(CtaTemplate):
    """
    这个策略 回测蓝猫改版那个策略

    """
    author = "ipqhjjybj"
    class_name = "MartinStrategy"

    symbol_pair = "btc_usdt"
    exchange = "COINEXS"

    # 策略参数
    bar_window = 5

    max_price = 999999
    min_price = 1000

    int_direction = 1

    fixed = 100
    second_fix_size = 120

    loss_space_buy = 7
    loss_space_sell = 7

    put_order_num = 2

    max_loss_buy_num = 28
    max_loss_sell_num = 28

    profit_space_buy = 3
    profit_space_sell = 3

    # cut_price_long = 1000
    # cut_price_short = 1000

    support_long = True
    support_short = True

    avg_price = 0
    pos = 0

    exchange_info = {"exchange_name": "COINEXS", "account_key": "COINEXS.BTC", "price_tick": 0.5, "volume_tick": 1}

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'vt_symbols_subscribe',  # vt_symbol_subscribe
                  "pos",  # 仓位
                  'avg_price',  # 持仓均价
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'max_price',  # 最大价格, 低于这个价格才挂单
                  'min_price',  # 最小价格, 高于这个价格才挂单
                  'int_direction',  # 挂单的方向
                  'fixed',  # 第一单大小
                  'second_fix_size',  # 补仓大小
                  'put_order_num',  # 补单挂单数量
                  'loss_space_buy',  # 多头补仓间距
                  'loss_space_sell',  # 空头补仓间距
                  'max_loss_buy_num',  # 多头最大补仓次数
                  'max_loss_sell_num',  # 空头最大补仓次数
                  'profit_space_buy',  # 多头盈利间距
                  'profit_space_sell',  # 空头盈利间距
                  'cut_price_long',   # 多头砍仓间距
                  'cut_price_short',   # 空头砍仓间距
                  'support_long',      # 支持多头
                  'support_short',     # 支持空头
                  'bar_window'  # 窗口
                  'exchange_info'  # 交易所信息
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading',
                 'avg_price'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MartinStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                                   interval=Interval.MINUTE.value)
        self.am = ArrayManager(40)

        self.limit_order_dict = {}

        self.now_direction = None
        self.last_tick = None

        if self.int_direction < 0:
            self.run_direction = Direction.SHORT.value
        elif self.int_direction > 0:
            self.run_direction = Direction.LONG.value

        self.max_long_pos_size = self.fixed + self.max_loss_buy_num * self.second_fix_size
        self.max_short_pos_size = self.fixed + self.max_loss_sell_num * self.second_fix_size

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def cancel_all_order(self):
        vt_order_ids = list(self.limit_order_dict.keys())
        for vt_order_id in vt_order_ids:
            self.cancel_order(vt_order_id)

    def check_put_order_condition(self, price):
        if price >= self.min_price and price <= self.max_price:
            return True
        else:
            return False

    def put_order(self, price):
        if not self.am.inited:
            return

        if self.pos == 0:
            if self.run_direction == Direction.LONG.value and self.support_long:
                list_orders = self.buy(self.symbol_pair, self.exchange, price, self.fixed)
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order
            elif self.run_direction == Direction.SHORT.value and self.support_short:
                list_orders = self.sell(self.symbol_pair, self.exchange, price, self.fixed)
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order

        elif self.pos > 0:
            if self.run_direction == Direction.LONG.value:
                for i in range(1, self.put_order_num + 1):
                    if abs(self.pos) + i * self.second_fix_size <= self.max_long_pos_size:
                        list_orders = self.buy(self.symbol_pair, self.exchange, price - self.loss_space_buy,
                                               self.second_fix_size)

                        for vt_order_id, order in list_orders:
                            self.limit_order_dict[vt_order_id] = order

                price = get_round_order_price(self.avg_price + self.profit_space_buy, self.exchange_info["price_tick"])
                list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(self.pos))
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order
        else:
            if self.run_direction == Direction.SHORT.value:
                for i in range(1, self.put_order_num + 1):
                    if abs(self.pos) + i * self.second_fix_size <= self.max_short_pos_size:
                        list_orders = self.sell(self.symbol_pair, self.exchange, price + self.loss_space_sell,
                                                self.second_fix_size)
                        for vt_order_id, order in list_orders:
                            self.limit_order_dict[vt_order_id] = order

                price = get_round_order_price(self.avg_price - self.profit_space_sell, self.exchange_info["price_tick"])
                list_orders = self.buy(self.symbol_pair, self.exchange, price, abs(self.pos))
                for vt_order_id, order in list_orders:
                    self.limit_order_dict[vt_order_id] = order

    def check_to_cut(self, price):
        # if self.pos > 0 and price < self.avg_price - self.cut_price_long:
        #     list_orders = self.sell(self.symbol_pair, self.exchange, price * 0.99, abs(self.pos))
        #     for vt_order_id, order in list_orders:
        #         self.limit_order_dict[vt_order_id] = order
        #     return True
        # if self.pos < 0 and price > self.avg_price + self.cut_price_short:
        #     list_orders = self.buy(self.symbol_pair, self.exchange, price * 1.01, abs(self.pos))
        #     for vt_order_id, order in list_orders:
        #         self.limit_order_dict[vt_order_id] = order
        #     return True
        if self.pos > 0 and abs(self.pos) >= self.max_long_pos_size:
            list_orders = self.sell(self.symbol_pair, self.exchange, price * 0.99, abs(self.pos))
            for vt_order_id, order in list_orders:
                self.limit_order_dict[vt_order_id] = order
            return True
        if self.pos < 0 and abs(self.pos) >= self.max_short_pos_size:
            list_orders = self.buy(self.symbol_pair, self.exchange, price * 1.01, abs(self.pos))
            for vt_order_id, order in list_orders:
                self.limit_order_dict[vt_order_id] = order
            return True
        return False

    def on_tick(self, tick):
        self.bg.update_tick(tick)
        if self.last_tick is None:
            self.last_tick = copy(tick)
        else:
            self.last_tick = copy(tick)

    def on_bar(self, bar):
        self.bg.update_bar(bar)

        self.cancel_all_order()
        flag = self.check_to_cut(bar.close_price)
        if not flag and self.check_put_order_condition(bar.close_price):
            self.put_order(bar.close_price)

    def on_window_bar(self, bar):
        am = self.am
        am.update_bar(bar)

        self.write_log(
            "[on_window_bar] [{}] high_price:{},low_price:{},pos:{},direction:{}".format(
                bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                bar.high_price, bar.low_price, self.pos, self.run_direction))
        if not am.inited:
            return

        fast_ma = Technique.x_average(self.am.close_array, 5)
        fast_ma0 = fast_ma[-1]

        slow_ma = Technique.x_average(self.am.close_array, 20)
        slow_ma0 = slow_ma[-1]

        if fast_ma0 > slow_ma0:
            self.run_direction = Direction.LONG.value
        else:
            self.run_direction = Direction.SHORT.value

    def on_stop_order(self, stop_order):
        pass

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
