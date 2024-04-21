# coding=utf-8

from copy import copy
import time
from collections import defaultdict
import pandas as pd
import numpy as np

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import BarData, TickData
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator
from tumbler.constant import Direction, Status, Interval
from tumbler.function.bar import BarGenerator, NewPandasDeal


class MachineLearningStrategy(CtaTemplate):
    """
    机器学习策略，调用机器学习算法实现:
    1、加载初始数据
    2、构建多周期数据
    3、传递进model训练，一边fit，一边 predict
    4、对交易产生的label 生成仓位
    5、分钟级别产生信号并下单
    """

    author = "ipqhjjybj"
    class_name = "MachineLeaningStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    fixed = 1
    pos = 0
    target_pos = 0

    per_time = 10  # 每隔5买发一次买单

    bar_period_factor = []  # func, window, interval,

    is_backtesting = False
    initDays = 20  # 初始化天数

    retry_cancel_send_num = 20  # 撤单失败重试次数

    # 策略变量
    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # 订阅的交易对
        'symbol_pair',  # 交易对
        'exchange',  # 交易所
        'bar_period_factor',  # BarFactor
        'is_backtesting',  # 是实盘还是回测
        'fixed',
        'exchange_info',
        "pos"
    ]

    # 变量列表，保存了变量的名称
    varList = [
        'inited',
        'trading'
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MachineLearningStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, interval=Interval.MINUTE.value, quick_minute=1)
        self.bg_pandas_array = []
        for func, window, interval in self.bar_period_factor:
            self.write_log("window:{} interval:{}".format(window, interval))
            pandas_deal = NewPandasDeal(func, window, interval)
            self.bg_pandas_array.append(pandas_deal)

        self.ticker = None
        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.order_dict = {}
        self.pre_use_time = time.time() - self.per_time
        self.cancel_dict_times_count = defaultdict(int)  # 对一个 vt_order_id 的撤单次数
        self.config_right = False

        self.dirty_order_dict = {}

    def check_config(self):
        self.config_right = True
        if "price_tick" not in self.exchange_info.keys():
            self.config_right = False
            self.write_important_log("[check_config] price_tick is not set!")
            return

        if "volume_tick" not in self.exchange_info.keys():
            self.config_right = False
            self.write_important_log("[check_config] volume_tick is not set!")
            return

        self.write_log("[check_config] all right!")

    def on_init(self):
        self.check_config()

        if not self.is_backtesting:
            self.write_log("[on_init] load_bar")
            self.load_bar(60)

            self.target_pos = 0
            self.write_log("[on_init] len bg_pandas_array:{}".format(len(self.bg_pandas_array)))
            i = 0
            for bg_pandas in self.bg_pandas_array:
                self.write_log("[on_init] bg_pandas init!")
                bg_pandas.on_init()
                pos = bg_pandas.get_pos()
                self.write_log("[on_init] window:{} i:{} pos:{}".format(bg_pandas.window, i, pos))
                self.target_pos += pos

            self.target_pos = self.target_pos * self.fixed
            self.trading = True
            self.write_log("[on_init] target_pos:{}, now pos:{}".format(self.target_pos, self.pos))

        for bg_pandas in self.bg_pandas_array:
            bg_pandas.start_trading()

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def fit(self, x, y):
        pass

    def predict(self, x, y):
        return 1

    def on_tick(self, tick):
        if tick.has_depth():
            self.ticker = copy(tick)
            if not self.config_right:
                return

            self.bg.update_tick(tick)

            now = time.time()
            if now > self.pre_use_time + self.per_time:
                self.go_to_order()
                self.pre_use_time = now

    def on_bar(self, bar: BarData):
        if self.is_backtesting:
            self.ticker = TickData.make_ticker(bar.close_price)
        self.write_log("[on_bar] datetime:{} close_price:{}".format(bar.datetime, bar.close_price))
        for bg_pandas in self.bg_pandas_array:
            bg_pandas.on_bar(bar)

        dic = {}
        for bg_pandas in self.bg_pandas_array:
            gd = bg_pandas.get_last_items()
            dic.update(gd)

        # 这里需要构造
        # x, y 来让模型训练
        self.fit([], [])
        val = self.predict(1, 2)
        if val > 1:
            self.target_pos = 1
        elif val < -1:
            self.target_pos = -1
        else:
            self.target_pos = 0
        if self.trading:
            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))
            self.to_target_pos()

    def get_already_send_volume(self):
        buy_volume, sell_volume = 0, 0
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                if order.direction == Direction.LONG.value:
                    buy_volume += order.volume - order.traded
                else:
                    sell_volume += order.volume - order.traded
        return buy_volume, sell_volume

    def cancel_all_orders(self):
        need_cancel_sets = []
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def cancel_not_cover_order(self):
        """
        撤销不工作的订单
        """
        now = time.time()
        need_cancel_sets = set([])
        for vt_order_id, order in self.order_dict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            if now - order_time > 60 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                need_cancel_sets.add(order.vt_order_id)
                self.write_log('[prepare cover_order] vt_order_id:{}, order.order_time:{}, old_order_time:{}'
                               .format(vt_order_id, order_time, order.order_time))

        if need_cancel_sets:
            self.cancel_sets_order(need_cancel_sets)

    def cancel_sets_order(self, need_cancel_sets):
        """
        发出撤单
        """
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.order_dict[vt_order_id]
                    del self.order_dict[vt_order_id]

                    self.dirty_order_dict[vt_order_id] = copy(order)

    def go_to_order(self):
        self.cancel_not_cover_order()
        self.to_target_pos()

    def to_target_pos(self):
        if self.trading:
            buy_volume, sell_volume = self.get_already_send_volume()
            chazhi = self.target_pos - self.pos

            self.write_log("[to_target_pos] buy_volume:{} sell_volume:{} chazhi:{}"
                           .format(buy_volume, sell_volume, chazhi))
            if len(self.order_dict.keys()) > 0:
                for vt_order_id, order in self.order_dict.items():
                    self.write_log("[to_target_pos not empty] vt_order_id:{} traded:{} volume:{} status:{}"
                                   .format(vt_order_id, order.traded, order.volume, order.status))
            if chazhi > 0:
                volume = chazhi - buy_volume
                depth_volume = self.ticker.get_sum_depth_sell_volume()
                volume = min(volume, depth_volume)
                volume = get_round_order_price(volume, self.exchange_info["volume_tick"])

                price = self.ticker.ask_prices[0] * 1.003
                price = get_round_order_price(price, self.exchange_info["price_tick"])

                if volume > 0 and price > 0:
                    list_orders = self.buy(self.symbol_pair, self.exchange, price, volume)
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order

            elif chazhi < 0:
                volume = abs(chazhi) - sell_volume
                depth_volume = self.ticker.get_sum_depth_buy_volume()
                volume = min(volume, depth_volume)
                volume = get_round_order_price(volume, self.exchange_info["volume_tick"])

                price = self.ticker.bid_prices[0] * 0.997
                price = get_round_order_price(price, self.exchange_info["price_tick"])

                if volume > 0 and price > 0:
                    list_orders = self.sell(self.symbol_pair, self.exchange, price, volume)
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

        elif order.vt_order_id in self.dirty_order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.dirty_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos += new_traded
                self.dirty_order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.dirty_order_dict[order.vt_order_id] = copy(order)
            else:
                bef_order = self.dirty_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded
                self.dirty_order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.dirty_order_dict[order.vt_order_id] = copy(order)

    def on_trade(self, trade):
        self.write_important_log("[on_trade info] trade:{},{},{},{}\n"
                                 .format(trade.vt_symbol, trade.order_id, trade.direction, trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
