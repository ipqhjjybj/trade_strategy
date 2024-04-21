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
from tumbler.function import get_max_and_index, get_min_and_index


class TradingRulerAStrategy(CtaTemplate):
    '''
    对交易规则A进行代码编写:
    交易规则A

    交易策略:
        划分浪形，选择操作浪的B浪介入，根据一浪预估出运行时间及运行空间。
        估计策略:
            点数空间 = 一浪运行长度 * 2.5
            运行天数 = 一浪时间 * 2.5
        退出策略:
            1. 达到目标价退出
            2. 分析浪形，已经跌破1浪与2浪低点(高点)连线退出
            3. 止损退出

    必须满足条件
    1、盈亏比，盈利空间>亏损空间*2 ，止损用20根bar最低或最高点
    2、持有天数 >= 5天， 也就是 4小时bar * 30根
    3、操作周期 4h 级别

    满足以下条件信号加分
    1、在重要支撑位附近 (成交量前期堆叠附近)
    2、Macd处于底背离状态或者 macd 处于增加上升的状态

    代码实现思路:
    考虑做空
    1、计算N天最高价(不包含最近M天) M天内最低价 ( N > M > 1)
    2、假设今天收盘价为CD， M天内最低价是 MLL，N天内最高价是NHH

    目标盈利位是 MLL， 目标止损位是 NHH,  目标盈利为 C - MLL， 目标亏损为 NHH - C
    要 C - MLL > 2 * (NHH - C) > 0

    附加实现的 参照指标
        a.今日最低价低于昨日最低价
        b.macd 处于下降状态

    考虑做多
    1、计算N天最低价(不包含最近M天) M天内最高价 ( N > M > 1)
    2、假设今天收盘价为CD， M天内最高价是 MHH, N天内最低价是NLL

    目标盈利位是 MHH， 目标止损位是 NLL,  目标盈利为 MHH - C， 目标亏损为 C - NLL
    要 MHH - C > 2 * (C - NLL) > 0

    附加实现的 参照指标
        a.今日最低价高于昨日最高价
        b.macd 处于上升状态

    3、在目标盈利 > 2*目标亏损入场后， 如果跌破止损位，则出场
    4、当达到止盈位置时
        a. 止盈一半
        b. 剩余部分仓位进行 StopLoss 跟踪止盈止损来做，跌破stoploss 则退出
    '''
    author = "ipqhjjybj"
    class_name = "TradingRulerStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    interval = Interval.HOUR.value

    bar_window = 1
    M = 15
    N = 30

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bar_window',  # bar线
                  'fixed',  # 下单参数大小
                  'M',     # 最近 M天
                  'N',     # 最近 N天
                  'interval',
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(TradingRulerAStrategy, self).__init__(mm_engine, strategy_name, settings)
        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)
        self.am = ArrayManager(100)

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar):
        self.bg.update_bar(bar)

    def on_window_bar(self, bar):
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        print("on_window_bar", bar.datetime, bar.close_price)
        macd, macdsignal, macdhist = talib.MACD(am.close, fastperiod=12, slowperiod=26, signalperiod=9)

        if self.pos == 0:
            if bar.high_price > am.high_array[-3] and am.high_array[-2] > am.high_array[-3] \
                    and macdhist[-1] > min(macdhist[-2], macdhist[-3]):
                # 处于新高状态，考虑做多
                for i in range(1, self.M):
                    mhh, ind_mhh = get_max_and_index(am.high_array, -(i+1), -1)
                    mll, ind_mll = get_min_and_index(am.low_array, -(i + 1), -1)
                    nll, ind_nll = get_min_and_index(am.low_array, -self.N, ind_mhh-1)

                    if mhh - bar.close_price > 2 * (bar.close_price - nll) > 0 and abs(ind_mhh) >= 4 \
                            and abs(ind_mhh - ind_nll) >= 4 and mll > nll:
                        print(bar.datetime, "go to buy", am.datetime_array[ind_mhh], am.datetime_array[ind_nll],
                              ind_mhh, ind_nll, "price", mhh, bar.close_price, nll,
                              macdhist[-1] > macdhist[-2] > macdhist[-3], macdhist[-1], macdhist[-2], macdhist[-3])

            elif bar.low_price < am.low_array[-3] and am.low_array[-2] < am.low_array[-3] \
                    and macdhist[-1] < max(macdhist[-2], macdhist[-3]):
                # 处于新低状态，考虑做空
                for i in range(1, self.M):
                    mll, ind_mll = get_min_and_index(am.low_array, -(i+1), -1)
                    mhh, ind_mhh = get_max_and_index(am.high_array, -(i+1), -1)
                    nhh, ind_nhh = get_max_and_index(am.high_array, -self.N, ind_mll-1)
                    if bar.close_price - mll > 2 * (nhh - bar.close_price) > 0 and abs(ind_mll) >= 4 \
                            and abs(ind_mll - ind_nhh) >= 4 and mhh < nhh:
                        print(bar.datetime, "go to sell", am.datetime_array[ind_mll], am.datetime_array[ind_nhh],
                              ind_mll, ind_nhh, "price", nhh, bar.close_price, mll,
                              macdhist[-1] < macdhist[-2] < macdhist[-3], macdhist[-1], macdhist[-2], macdhist[-3])

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

