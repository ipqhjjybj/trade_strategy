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


class EmaDmiV2Strategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    dmi 决定趋势, ema介入

    dmi 用的长周期趋势

    """
    author = "ipqhjjybj"
    class_name = "EmaDmiV2Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    ema_short = 5
    ema_long = 10

    adx_length = 18

    bar_window1 = 1
    bar_window2 = 12

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window1',  # bar线 1
                  'bar_window2',  # bar线 2
                  'adx_length',   # adx 的长度线
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(EmaDmiV2Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg1 = BarGenerator(self.on_bar1, window=self.bar_window1, on_window_bar=self.on_window_bar1,
                                interval=Interval.HOUR.value, quick_minute=2)
        self.bg2 = BarGenerator(self.on_bar2, window=self.bar_window2, on_window_bar=self.on_window_bar2,
                                interval=Interval.HOUR.value, quick_minute=2)
        self.am1 = ArrayManager(60)
        self.am2 = ArrayManager(60)

        self.pos = 0

        self.limit_order_dict = {}

        self.adx_flag = False

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg1.update_tick(tick)
        self.bg2.update_tick(tick)

    def cancel_all_limit_order(self):
        need_cancel_sets = []
        for vt_order_id, order in self.limit_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_bar(self, bar):
        '''
        用于回测时的数据提供
        '''
        self.on_bar1(copy(bar))
        self.on_bar2(copy(bar))

    def on_bar1(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg1.update_bar(bar)

    def on_bar2(self, bar):
        self.bg2.update_bar(bar)

    def on_window_bar1(self, bar):
        self.write_log("[on_window_bar1] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am1
        am.update_bar(bar)

        if not am.inited:
            return

        ema_short_arr = talib.EMA(am.close, timeperiod=self.ema_short)
        ema_long_arr = talib.EMA(am.close, timeperiod=self.ema_long)

        if self.adx_flag:
            if ema_short_arr[-1] > ema_long_arr[-1]:
                if self.pos <= 0:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos) + 1)
            elif ema_short_arr[-1] < ema_long_arr[-1]:
                if self.pos >= 0:
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos) + 1)
        else:
            if self.pos > 0:
                if ema_short_arr[-1] < ema_long_arr[-1]:
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))
            if self.pos < 0:
                if ema_short_arr[-1] > ema_long_arr[-1]:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))

    def on_window_bar2(self, bar):
        self.write_log("[on_window_bar2] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am2
        am.update_bar(bar)

        if not am.inited:
            return

        adx_arr = talib.ADX(am.high_array, am.low_array, am.close_array, timeperiod=self.adx_length)
        adxr_arr = talib.ADXR(am.high_array, am.low_array, am.close_array, timeperiod=self.adx_length)
        # self.write_log(f"adx_arr:{adx_arr} {am.high_array} {am.low_array} {am.close_array}")
        # self.write_log(f"adxr_arr:{adxr_arr}")

        #if adx_arr[-1] > adxr_arr[-1]:
        if adx_arr[-1] > max(adx_arr[-5:-2]):
            self.adx_flag = True
        else:
            self.adx_flag = False
        self.write_log(f"[on_window_bar2] adx_flag:{self.adx_flag} adx:{adx_arr[-1]} adxr:{adxr_arr[-1]} ")

    def on_stop_order(self, stop_order):
        pass

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

    def on_trade(self, trade):
        if trade.direction == Direction.LONG.value:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
