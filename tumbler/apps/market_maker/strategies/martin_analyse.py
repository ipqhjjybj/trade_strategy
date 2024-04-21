# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus


class MartinAnalyseStrategy(MarketMakerTemplate):
    """
    """
    author = "ipqhjjybj"
    class_name = "MartinAnalyseStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    fixed_size = 1
    pos = 0

    avg_pos_price = 0                           # 当前仓位均价

    loss_space_buy = 0.6                        # 多头补仓间距 百分之多少
    max_loss_buy_num = 14                       # 最大补仓次数
    profit_rate = 0.6                           # 盈利率

    parameters = ['strategy_name',              # 策略加载的唯一性名字
                  'class_name',                 # 类的名字
                  'author',                     # 作者
                  'symbol_pair',                # 交易对
                  'loss_space_buy',             # 多头补仓间距
                  'max_loss_buy_num',           # 最大补仓次数
                  'profit_rate'                 # 盈利率
                ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]


    def __init__(self, mm_engine, strategy_name, settings):
        super(MartinAnalyseStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.zs_buy_order_dict = {}
        self.cover_order_dict = {}

        self.now_loss_buy_num = 0

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_tick(self):
        pass

    def is_nosent_order(self):
        return len(self.zs_buy_order_dict.keys()) == 0

    def cancel_dict(self, dic):
        list_orders = list(dic.keys())
        for vt_order_id in list_orders:
            self.cancel_order(vt_order_id)

    def on_bar(self, bar):
        if self.pos == 0:
            ret_orders = self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, self.fixed_size)
            for vt_order_id, order in ret_orders:
                self.zs_buy_order_dict[vt_order_id] = order

        elif self.pos > 0:
            if self.is_nosent_order():
                if self.now_loss_buy_num < self.max_loss_buy_num:
                    price = bar.close_price * (1 - self.loss_space_buy / 100.0)
                    ret_orders = self.buy(self.symbol_pair, self.exchange, price, self.fixed_size)
                    for vt_order_id, order in ret_orders:
                        self.zs_buy_order_dict[vt_order_id] = order

            sell_price = self.avg_pos_price * (1 + self.profit_rate / 100.0)

            self.cancel_dict(self.cover_order_dict)
            ret_orders = self.sell(self.symbol_pair, self.exchange, sell_price, abs(self.pos))
            for vt_order_id, order in ret_orders:
                self.cover_order_dict[vt_order_id] = order
        else:
            self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, self.fixed_size)
        
    def on_stop_order(self, stop_order):
        self.write_log("[on_stop_order] {},{},{},{},{},{},{},{}".format(stop_order.vt_symbol, stop_order.direction, stop_order.offset,
                stop_order.price, stop_order.volume, stop_order.vt_order_id, stop_order.vt_order_ids, stop_order.status))

    def on_order(self, order):
        if order.vt_order_id in self.zs_buy_order_dict.keys():
            bef_order = self.zs_buy_order_dict.get(order.vt_order_id)
            traded = order.traded - bef_order.traded
            if traded > 0:
                self.now_loss_buy_num += 1

                self.avg_pos_price = (self.avg_pos_price * self.pos + order.price * traded) / (traded + self.pos)
                self.pos += traded 
                
            self.zs_buy_order_dict[order.vt_order_id] = copy(order)
            if not order.is_active():
                self.zs_buy_order_dict.pop(order.vt_order_id)

        if order.vt_order_id in self.cover_order_dict.keys():
            self.now_loss_buy_num = 0

            bef_order = self.cover_order_dict.get(order.vt_order_id)
            traded = order.traded - bef_order.traded 
            if traded > 0:
                self.pos -= traded

            self.cover_order_dict[order.vt_order_id] = copy(order)
            if not order.is_active():
                self.cover_order_dict.pop(order.vt_order_id)

    def on_trade(self, trade):
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.datetime, trade.vt_symbol, trade.vt_trade_id, trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

