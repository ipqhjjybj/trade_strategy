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


class TBPStrategy(CtaTemplate):
    """
    好像回测很差。。。
    tbp 趋势平衡点交易系统
    MF: 动量，当日的收盘价与两个交易日前的收盘价之差
    avg(x): 平均价格，当日的最高价、最低价和收盘价的平均值
    止损点 非反转点
        (1) 如果处于多头状态，止损点为 avg(x) - TR
        (2) 如果处于空头状态，止损点为 avg(x) + TR
    目标价位 了结仓位，不等反转
        (1) 如果处于多头状态，目标价为 2 * avg(x) - L
        (2) 如果处于空头状态，目标价为 2 * avg(x) - H
    建仓:
        （1）收盘价高于TBP时，在收盘价入市，建立多头仓位
         (2) 收盘价低于TBP时，在收盘价入市，建立空头仓位
    反转入市：
         (1) 收盘价高于TBP时，在收盘价从空头转为多头
         (2) 收盘价低于TBP时，在收盘价从多头转为空头
    离场:
        (1) 在目标价获利离场，不等着反转
        (2) 在止损点了解离场，不等着反转
    计算次日交易日的TBP
        (1) 如果处于多头状态，则用前两个交易日中较小的MF值加上前一个交易日的收盘价即可
        (2) 如果处于空头状态，则用前两个交易日中较大的MF值加上前一个交易日的收盘价即可
    """
    author = "ipqhjjybj"
    class_name = "TBPStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    interval = Interval.DAY.value
    bar_window = 1

    pos = 0
    fixed = 1

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'fixed',      # 下单参数大小
                  'interval',
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(TBPStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)
        self.am = ArrayManager(30)

        self.pos = 0

        self.zhisun_long_val = 0
        self.zhisun_short_val = 0
        self.cover_long_val = 0
        self.cover_short_val = 0

        self.update_flag = False

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

        if not self.update_flag:
            if self.pos > 0:
                if bar.close_price >= self.cover_long_val or bar.close_price < self.zhisun_long_val:
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos))
            elif self.pos < 0:
                if bar.close_price <= self.cover_short_val or bar.close_price > self.zhisun_short_val:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))

        self.update_flag = True

    def on_window_bar(self, bar):
        # self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return
        tr = max(am.high_array[-1] - am.low_array[-1],
                 abs(am.high_array[-1] - am.close_array[-2]),
                 abs(am.low_array[-1] - am.close_array[-2]))

        mf_2 = am.close_array[-3] - am.close_array[-5]
        mf_1 = am.close_array[-2] - am.close_array[-4]
        mf = am.close_array[-1] - am.close_array[-3]
        avg_x = (bar.high_price + bar.low_price + bar.close_price) / 3.0

        long_tbp = am.close_array[-3] - max(mf_1, mf_2)
        short_tbp = am.close_array[-3] - min(mf_1, mf_2)

        self.zhisun_long_val = avg_x - tr
        self.zhisun_short_val = avg_x + tr
        self.cover_long_val = 2 * avg_x - am.low_array[-1]
        self.cover_short_val = 2 * avg_x - am.high_array[-1]

        if mf > max(mf_1, mf_2):
            if self.pos < self.fixed:
                self.update_flag = True
                self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value,
                                Offset.OPEN.value, bar.close_price + 100, abs(self.pos) + self.fixed)
        elif mf < min(mf_1, mf_2):
            if self.pos > -1 * self.fixed:
                self.update_flag = True
                self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value,
                                Offset.OPEN.value, bar.close_price - 100, abs(self.pos) + self.fixed)

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
