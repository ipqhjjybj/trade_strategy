# coding=utf-8

import time
import numpy as np
from copy import copy
import os
from collections import defaultdict

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.technique import Technique


class AmaStrategy(CtaTemplate):
    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"

    # 初始 1个BTC，10000 U
    x = 1
    y = 10000

    profit_rate = 0.3

    pos = 0

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'symbol_pair',  # 交易对
        'exchange',  # 交易所
        'x',
        'y',
        'profit_rate',
        'exchange_info',
        "pos"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AmaStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.order_dict = {}

    def on_init(self):
        self.write_log("on_init")
        self.load_bar(30)
        self.write_log("on_init after")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        pass

    def on_order(self, order):
        pass

    def on_trade(self, trade):
        pass







