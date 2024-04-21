# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.calendar_spread.template import (
    CalendarSpreadTemplate
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.constant import Direction, Status, Offset


class FutureCrossV2Strategy(CalendarSpreadTemplate):
    """
    COINEXS 与 OKEX 数值相减 > 某个数值
    """
    author = "ipqhjjybj"
    class_name = "FutureCrossV2Strategy"
    target_exchange_info = {}
    base_exchange_info = {}
    profit_spread = 0.18
    base_spread = 0.2
    put_order_num = 1
    inc_spread = 0

    need_cover_buy_volume = 0
    need_cover_sell_volume = 0

    """
    默认参数
    """
    symbol_pair = "bch_usd"

    # 参数列表
    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # 订阅的品种
        'target_exchange_info',  # 目标市场的价格
        'base_exchange_info',  # 反向市场的价格
        'profit_spread',  # 需要保证盈利的价差
        'base_spread',  # 刚开始挂的价差
        'put_order_num',  # 挂单的数量
        'inc_spread',  # 挂单差值迭代
    ]

    def __init__(self, cs_engine, strategy_name, settings):
        super(FutureCrossV2Strategy, self).__init__(cs_engine, strategy_name, settings)

        self.update_failed = False  # 初始化是否成功
        self.base_exchange_updated = False  # 回补场所数据

        self.target_exchange_info["pos"] = 0

        self.base_bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
        self.base_asks = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
        self.target_bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
        self.target_asks = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
