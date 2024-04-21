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


class GridAnalyseStrategy(MarketMakerTemplate):
    """
    先只研究多头
    在只有多头止盈的情况下，年化回报多少，无限资金，每单止盈，不止损

    """
    author = "ipqhjjybj"
    class_name = "GridAnalyseStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    line_rate = 10
    cut_rate = 3

    fixed_size = 1

    pos = 0


    parameters = ['strategy_name',              # 策略加载的唯一性名字
                  'class_name',                 # 类的名字
                  'author',                     # 作者
                  'symbol_pair',                # 交易对
                  'put_nums',                   # 一共下多少订单
                  'profit_rate',                # 每单止盈间距
                  'put_rate'                    # 每单间隔 , 用固定点数，如 1,2,3,4
                ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]


    def __init__(self, mm_engine, strategy_name, settings):
        super(GridAnalyseStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.pos = 0

        self.zs_buy_order_dict = {}

    def on_init(self):
        pass

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_tick(self):
        pass

    def cancel_all(self):
        arr = []
        for vt_order_id in self.zs_buy_order_dict.keys():
            arr.append(vt_order_id)

        for vt_order_id in arr:
            self.cancel_order( vt_order_id )

    def is_empty(self):
        return len(self.zs_buy_order_dict.keys()) is 0

    def on_bar(self, bar):
        #self.write_log("[bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        if self.pos == 0:
            if self.is_empty():
                price_volume_list = []
                for i in range(self.put_nums):
                    price = bar.close_price * (1 - self.put_rate/100.0 * i)
                    volume = self.fixed_size
                    price_volume_list.append( (bar.close_price , volume) )

                for price, volume in price_volume_list:
                    ret_orders = self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value, 
                    Offset.OPEN.value, price, volume)

                    for vt_order_id, order in ret_orders:
                        if vt_order_id:
                            self.zs_buy_order_dict[vt_order_id] = order
                        else:
                            self.write_log("[put_long_orders] vt_order_id is None")
            else:
                self.cancel_all()
        elif self.pos > 0:
            pass
        elif self.pos < 0:
            self.cover(self.symbol_pair, self.exchange, bar.close_price + 100, abs(self.pos))

    def on_stop_order(self, stop_order):
        self.write_log("[on_stop_order] {},{},{},{},{},{},{},{}".format(stop_order.vt_symbol, stop_order.direction, stop_order.offset,
                stop_order.price, stop_order.volume, stop_order.vt_order_id, stop_order.vt_order_ids, stop_order.status))

    def on_order(self, order):
        if order.vt_order_id in self.zs_buy_order_dict.keys():
            bef_order = self.zs_buy_order_dict.get(order.vt_order_id)
            traded = order.traded - bef_order.traded
            if traded > 0:
                sell_price = order.price * (1 + self.profit_rate / 100.0)
                self.sell(self.symbol_pair, self.exchange, sell_price, abs(traded))
                
            self.zs_buy_order_dict[order.vt_order_id] = copy(order)
            if not order.is_active():
                self.zs_buy_order_dict.pop(order.vt_order_id)

    def on_trade(self, trade):
        if trade.direction == Direction.LONG.value:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume

        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.datetime, trade.vt_symbol, trade.vt_trade_id, trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

