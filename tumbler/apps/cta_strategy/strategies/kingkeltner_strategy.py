# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

import talib

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class KingKeltnerStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "KingKeltnerStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"
    
    # 策略参数
    avg_length = 40                             # 布林通道参数
    atr_length = 40                             # 真实波幅参数

    bar_window = 4

    fixed = 1

    pos = 0

    parameters = ['strategy_name',              # 策略加载的唯一性名字
                  'class_name',                 # 类的名字
                  'author',                     # 作者
                  'vt_symbols_subscribe',       # vt_symbol_subscribe
                  "pos",                        # 仓位
                  'symbol_pair',                # 交易对
                  'exchange',                   # 交易所
                  'avg_length',                 # 布林通道参数
                  'atr_length',                 # 真实波幅参数
                  'fixed',                      # 
                  'bar_window'        
                ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(KingKeltnerStrategy, self).__init__(mm_engine, strategy_name, settings)

        print(settings)
        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar, interval=Interval.HOUR.value)
        self.am = ArrayManager(50)

        self.stop_order_dict = {}
        self.limit_order_dict = {}

        self.pos = 0
        self.fixed = 1

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar):
        #self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

    def on_window_bar(self, bar):
        #self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        to_cancel_order_ids = list(self.stop_order_dict.keys())
        for vt_order_id in to_cancel_order_ids:
            self.cancel_order(vt_order_id)

        am = self.am 
        am.update_bar(bar)
        
        if not am.inited:
            return

        mov_avg_array = talib.MA( (am.high + am.low + am.close)/3.0, self.avg_length)
        atr_val = am.atr(self.atr_length)

        upband = mov_avg_array[-1] + atr_val
        dnband = mov_avg_array[-1] + atr_val

        liq_point = mov_avg_array[-1]

        # 三价均线向上，并且价格上破通道上轨，开多单
        if self.pos <= 0 and mov_avg_array[-1] > mov_avg_array[-2]:
            if self.pos < 0:
                list_orders = self.buy(self.symbol_pair, self.exchange, upband, abs(self.pos), stop = True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

            list_orders = self.buy(self.symbol_pair, self.exchange, upband, self.fixed, stop = True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

        if self.pos > 0:
            list_orders = self.sell(self.symbol_pair, self.exchange, liq_point, abs(self.pos), stop = True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

        # 三价均线向下，并且价格下破通道下轨，开空单
        if self.pos < 0 and mov_avg_array[-1] < mov_avg_array[-2]:
            if self.pos > 0:
                list_orders = self.sell(self.symbol_pair, self.exchange, dnband, abs(self.pos), stop = True)
                for vt_order_id, order in list_orders:
                    self.stop_order_dict[vt_order_id] = order

            list_orders = self.sell(self.symbol_pair, self.exchange, dnband, self.fixed, stop = True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

        if self.pos < 0:
            list_orders = self.buy(self.symbol_pair, self.exchange, liq_point, abs(self.pos), stop = True)
            for vt_order_id, order in list_orders:
                self.stop_order_dict[vt_order_id] = order

    def on_stop_order(self, stop_order):
        if stop_order.vt_order_id in self.stop_order_dict.keys():
            self.stop_order_dict[stop_order.vt_order_id] = copy(stop_order)

            if not stop_order.is_active():
                self.stop_order_dict.pop(stop_order.vt_order_id)

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction ,order.price ,order.volume ,order.traded))
        
    def on_trade(self, trade):
        if trade.direction == Direction.LONG.value:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume

        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id, trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)




