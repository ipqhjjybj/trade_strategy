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


class BollingerBanditStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "BollingerBanditStrategy"

    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"

    # 策略参数
    bollinger_lengths = 50  # 布林通道参数
    offset = 1.25  # 布林通道参数
    roc_calc_length = 30  # 过滤器参数
    liq_length = 50  # 自适应出场均线参数

    interval = Interval.HOUR.value

    bar_window = 4

    fixed = 1

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'vt_symbols_subscribe',  # vt_symbol_subscribe
                  "pos",  # 仓位
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bollinger_lengths',  # 布林通道参数
                  'offset',  # 布林通道参数
                  'roc_calc_length',  # 过滤器参数
                  'liq_length',  # 自适应出场均线参数
                  'fixed',  #
                  'bar_window',
                  'interval'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(BollingerBanditStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval)
        self.am = ArrayManager(70)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.liq_days = self.liq_length

        self.stop_order_dict = {}
        self.limit_order_dict = {}

        self.order_reject_cache_dict = {}
        self.send_times_count_dict = defaultdict(int)

    def on_init(self):
        self.write_log("on_init")
        self.load_bar(30)
        self.write_log("on_init after")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.tick_send_order(tick)
        self.bg.update_tick(tick)

    def on_bar(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

        self.cancel_all_limit_order()

    def cancel_all_limit_order(self):
        need_cancel_sets = []
        for vt_order_id, order in self.limit_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_window_bar(self, bar):
        self.write_log(
            "[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format(bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                                            bar.high_price, bar.low_price, self.pos))
        to_cancel_order_ids = list(self.stop_order_dict.keys())
        for vt_order_id in to_cancel_order_ids:
            self.cancel_order(vt_order_id)

        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        upband, dnband = am.boll(self.bollinger_lengths, self.offset)

        roc_calc = am.close_array[-1] - am.close_array[-self.roc_calc_length]

        if self.pos == 0:
            self.liq_days = self.liq_length
        else:
            self.liq_days = max(self.liq_days - 1, 10)

        liq_point = am.ma(self.liq_days)

        if self.pos <= 0 and roc_calc > 0:
            if self.pos < 0:
                list_orders = self.buy(self.symbol_pair, self.exchange, upband, abs(self.pos), stop=True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

            list_orders = self.buy(self.symbol_pair, self.exchange, upband, self.fixed, stop=True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

        if self.pos > 0 and liq_point < upband:
            list_orders = self.sell(self.symbol_pair, self.exchange, liq_point, abs(self.pos), stop=True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

        if self.pos >= 0 and roc_calc < 0:
            if self.pos > 0:
                list_orders = self.sell(self.symbol_pair, self.exchange, dnband, abs(self.pos), stop=True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

            list_orders = self.sell(self.symbol_pair, self.exchange, dnband, self.fixed, stop=True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

        if self.pos < 0 and liq_point > dnband:
            list_orders = self.buy(self.symbol_pair, self.exchange, liq_point, self.fixed, stop=True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

    def on_stop_order(self, stop_order):
        if stop_order.vt_order_id in self.stop_order_dict.keys():
            self.stop_order_dict[stop_order.vt_order_id] = copy(stop_order)

            if not stop_order.is_active():
                self.stop_order_dict.pop(stop_order.vt_order_id)

            for vt_order_id, order in stop_order.vt_order_ids:
                self.limit_order_dict[vt_order_id] = order

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        self.update_order(order)

        if order.status == Status.SUBMITTING.value:
            return

        if order.status == Status.REJECTED.value:
            self.order_reject_cache_dict[order.vt_order_id] = copy(order)
            self.send_times_count_dict[order.vt_order_id] += 1

            self.again_send(order.price)

        if order.vt_order_id in self.limit_order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.limit_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos += new_traded
            else:
                bef_order = self.limit_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded

            self.limit_order_dict[order.vt_order_id] = copy(order)
            if not order.is_active():
                self.limit_order_dict.pop(order.vt_order_id)

    def again_send(self, price):
        self.write_log("[again send] price:{}".format(price))
        reject_order_list = list(set(list(self.order_reject_cache_dict.keys())))
        for vt_order_id in reject_order_list:
            order_info = copy(self.order_reject_cache_dict[vt_order_id])
            num_before = self.send_times_count_dict[vt_order_id]
            self.write_log("[loop] vt_order_id:{}".format(vt_order_id))

            if num_before < 1:
                if order_info.direction == Direction.LONG.value:
                    price = price * 1.001
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.buy(self.symbol_pair, self.exchange, price, abs(order_info.volume))
                    for new_vt_order_id, order in list_orders:
                        self.limit_order_dict[new_vt_order_id] = order
                        self.send_times_count_dict[new_vt_order_id] = num_before + 1
                else:
                    price = price * 0.999
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(order_info.volume))
                    for new_vt_order_id, order in list_orders:
                        self.limit_order_dict[new_vt_order_id] = order
                        self.send_times_count_dict[new_vt_order_id] = num_before + 1
            else:
                self.cache_need_send_order(order_info.direction, abs(order_info.volume))

            if vt_order_id in self.order_reject_cache_dict.keys():
                self.order_reject_cache_dict.pop(vt_order_id)
                self.write_log("[order_reject_cache_dict] pop:{}".format(vt_order_id))

            if vt_order_id in self.send_times_count_dict.keys():
                self.send_times_count_dict.pop(vt_order_id)
                self.write_log("[send_times_count_dict] pop:{}".format(vt_order_id))

    def on_trade(self, trade):
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
