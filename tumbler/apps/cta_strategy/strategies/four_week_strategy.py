# coding=utf-8

from copy import copy
from collections import defaultdict

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.constant import Direction, Status, Interval
from tumbler.function.technique import Technique


class FourWeekStrategy(CtaTemplate):
    """
    鳄鱼线交易法则
    """
    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"

    fixed = 1
    pos = 0

    # 策略参数
    CF = 5

    initDays = 20  # 初始化天数

    # 策略变量
    bar_window = 4

    max_window = 20

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'symbol_pair',  # 交易对
        'exchange',    # 交易所
        'bar_window',  # 多少小时周期
        'max_window',  # 多少根K线的最高点，最低点
        'fixed',
        'exchange_info',
        "pos"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(FourWeekStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value, quick_minute=1)
        self.am = ArrayManager(10 + self.max_window)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

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

    def cancel_all_limit_order(self):
        need_cancel_sets = []
        for vt_order_id, order in self.limit_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_bar(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}"
        # .format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

        self.cancel_all_limit_order()

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

        break_up = Technique.highest(am.high_array, self.max_window)
        break_down = Technique.lowest(am.low_array, self.max_window)

        self.write_log("[run canshu] break_up:{}, break_down:{}".format(break_up, break_down))

        if self.pos == 0:
            if break_up > 0.0 and bar.close_price < break_up:
                list_orders = self.buy(self.symbol_pair, self.exchange, break_up + self.exchange_info["price_tick"],
                                       self.fixed, stop=True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

            if break_down > 0.0 and bar.close_price > break_down:
                list_orders = self.sell(self.symbol_pair, self.exchange, break_down - self.exchange_info["price_tick"],
                                        self.fixed, stop=True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

        elif self.pos > 0:
            if bar.close_price > break_down:
                list_orders = self.sell(self.symbol_pair, self.exchange, break_down, self.fixed + abs(self.pos),
                                        stop=True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

        elif self.pos < 0:
            if bar.close_price < break_up:
                list_orders = self.buy(self.symbol_pair, self.exchange, break_up, self.fixed + abs(self.pos), stop=True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

    def on_stop_order(self, stop_order):
        self.write_log(
            "[stop_order info] vt_order_id:{}, order.status:{}, vt_order_ids:{}\n".format(stop_order.vt_order_id,
                                                                                          stop_order.status,
                                                                                          stop_order.vt_order_ids))

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
        self.write_log("[on_trade info] trade:{},{},{},{}\n".format(trade.vt_symbol, trade.order_id, trade.direction,
                                                                    trade.volume))

        # if trade.direction == Direction.LONG.value:
        #     self.pos += trade.volume
        # else:
        #     self.pos -= trade.volume
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
