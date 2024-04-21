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
from tumbler.apps.market_maker.signals.line_signal import LineSignal


class LineStrategy(MarketMakerTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "GridMakerV1Strategy"

    symbol_pair = "btc_usdt"
    exchange = "OKEX"
    line_rate = 10
    cut_rate = 3

    pos = 0

    parameters = ['strategy_name',              # 策略加载的唯一性名字
                  'class_name',                 # 类的名字
                  'author',                     # 作者
                  'symbol_pair',                # 交易对
                  'line_rate',                  # 多少涨幅或者跌幅确定线
                  'cut_rate'                    # 下单后止盈止损间距 
                ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]


    def __init__(self, mm_engine, strategy_name, settings):
        super(LineStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.pos = 0

        self.line_signal = LineSignal(self.line_rate)

        self.stop_order_dict = {}               # 止损单
        self.limit_order_dict = {}              # 止盈单
        self.send_order_dict = {}               # 发送出去的订单

        self.transfer_order_dict = {}           # stop单转化过来的order

    def on_init(self):
        pass

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_tick(self):
        pass

    # def compute_line(self, bar):
    #     if self.line_point is None:
    #         if self.pre_high < bar.high_price:
    #             self.pre_high_bar = copy(bar)
    #             self.pre_high = bar.high_price

    #         if self.pre_low > bar.low_price:
    #             self.pre_low_bar = copy(bar)
    #             self.pre_low = bar.low_price

    #         if self.pre_high > self.pre_low * (1 + self.line_rate * 2 / 100.0):
    #             self.line_point = (self.pre_high + self.pre_low) / 2.0

    #             if self.pre_high_bar.datetime > self.pre_low_bar.datetime:
    #                 self.line_direction = Direction.LONG.value
    #             else:
    #                 self.line_direction = Direction.SHORT.value
    #     else:
    #         if self.line_direction == Direction.LONG.value:
    #             self.pre_high = max(self.pre_high, bar.high_price)
    #             self.pre_low = bar.low_price

    #             if bar.close_price < self.line_point:
    #                 if self.pre_high > self.pre_low * (1 + self.line_rate * 2 / 100.0):
    #                     self.line_direction = Direction.SHORT.value
    #                     self.line_point = self.pre_low * (1 + self.line_rate / 100.0)
    #                 else:
    #                     self.line_point = self.pre_high * (1 - self.line_rate / 100.0)
    #         else:
    #             self.pre_high = bar.high_price
    #             self.pre_low = min(self.pre_low, bar.low_price)

    #             if bar.close_price > self.line_point:
    #                 if self.pre_high > self.pre_low * (1 + self.line_rate * 2 / 100.0):
    #                     self.line_direction = Direction.LONG.value
    #                     self.line_point = self.pre_high * (1 - self.line_rate  / 100.0)
    #                 else:
    #                     self.line_point = self.pre_low * (1 + self.line_rate / 100.0)

    #     self.write_log("bar.high:{},bar.low:{},pre_high:{}, pre_low:{} line_point:{} line_direction:{}".format(bar.high_price ,bar.low_price ,self.pre_high, self.pre_low, self.line_point, self.line_direction))
    #     self.pre_bar = copy(bar)

    def cancel_sets_order_ids(self, order_ids):
        for vt_order_id in order_ids:
            self.cancel_order( vt_order_id )

    def cancel_all_limit_order(self):
        #self.write_log("cancel_all_stop_order,self.limit_order_dict:{}".format(self.limit_order_dict.items()))
        need_cancel_sets = []
        for vt_order_id, order in self.limit_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(vt_order_id)
                #self.write_log("cancel_all_limit_order:{}".format(vt_order_id))
        self.cancel_sets_order_ids(need_cancel_sets)

    def cancel_all_stop_order(self):
        #self.write_log("cancel_all_stop_order,self.stop_order_dict:{}".format(self.stop_order_dict.items()))
        need_cancel_sets = []
        for vt_order_id, order in self.stop_order_dict.items():
            #self.write_log("stop_order order.status:{}".format(order.status))
            if order.is_active():
                need_cancel_sets.append(vt_order_id)
                #self.write_log("cancel_all_stop_order:{}".format(vt_order_id))
        self.cancel_sets_order_ids(need_cancel_sets)

    def cancel_all_send_order(self):
        #self.write_log("cancel_all_stop_order,self.send_order_dict:{}".format(self.send_order_dict.items()))
        need_cancel_sets = []
        for vt_order_id, order in self.send_order_dict.items():
            if order.is_active():
                need_cancel_sets.append(vt_order_id)
                #self.write_log("cancel_all_send_order:{}".format(vt_order_id))
        self.cancel_sets_order_ids(need_cancel_sets)

    def is_empty(self):
        return len(self.limit_order_dict.keys()) == 0 \
            and len(self.stop_order_dict.keys()) == 0 \
            and len(self.send_order_dict.keys()) == 0 

    def sell_buy_order(self, price, volume):
        #self.write_log("[sell_buy_order] price:{}, volume:{}".format(price, volume))
        sell_limit_order_price = price * ( 1 + self.cut_rate / 100.0)
        sell_stop_order_price = price * ( 1 - self.cut_rate / 100.0)

        order_list = self.sell(self.symbol_pair, self.exchange, sell_limit_order_price, volume)
        for vt_order_id, order in order_list:
            self.limit_order_dict[vt_order_id] = copy(order)

        order_list = self.sell(self.symbol_pair, self.exchange, sell_stop_order_price, volume, stop=True)
        for vt_order_id, order in order_list:
            self.stop_order_dict[vt_order_id] = copy(order)

    def cover_sell_order(self, price, volume):
        #self.write_log("[cover_sell_order] price:{}, volume:{}".format(price, volume))
        buy_limit_order_price = price * ( 1 - self.cut_rate / 100.0)
        buy_stop_order_price = price * ( 1 + self.cut_rate / 100.0)

        order_list = self.cover(self.symbol_pair, self.exchange, buy_limit_order_price, volume)
        for vt_order_id, order in order_list:
            self.limit_order_dict[vt_order_id] = copy(order)

        order_list = self.cover(self.symbol_pair, self.exchange, buy_stop_order_price, volume, stop=True)
        for vt_order_id, order in order_list:
            self.stop_order_dict[vt_order_id] = copy(order)

    def on_bar(self, bar):
        #self.write_log("[bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        if self.line_signal.get_line_point():
            if self.line_signal.get_line_direction() == Direction.LONG.value:
                if self.pos == 0:
                    self.cancel_all_limit_order()
                    self.cancel_all_stop_order()
                    self.cancel_all_send_order()

                    # self.write_log("[on_bar] line Direction.LONG is_empty:{}, bar.close_price:{}, line_point:{}"\
                    #     .format(self.is_empty(), bar.close_price, self.line_point))

                    if self.is_empty() and bar.close_price > self.line_signal.get_line_point():
                        order_list = self.short(self.symbol_pair, self.exchange, bar.close_price - 10, 1)
                        for vt_order_id, order in order_list:
                            self.send_order_dict[vt_order_id] = copy(order)

                    # if not self.is_empty():
                    #     self.write_log("len(self.limit_order_dict.keys()):{},len(self.stop_order_dict.keys()):{},len(self.send_order_dict.keys()):{}"\
                    #         .format(len(self.limit_order_dict.keys()), len(self.stop_order_dict.keys()), len(self.send_order_dict.keys())))

                elif self.pos > 0:
                    self.cancel_all_limit_order()
                    self.cancel_all_stop_order()
                    self.cancel_all_send_order()

                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 10, abs(self.pos))

                elif self.pos < -1:
                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 10, abs(self.pos))

                # elif self.pos > 0:
                #     self.write_log("self.line_direction == Direction.LONG.value Error condition self.pos > 0")
                # else:
                #     self.write_log("self.line_direction == Direction.LONG.value Error condition self.pos < 0")

            else:
                if self.pos == 0:
                    self.cancel_all_limit_order()
                    self.cancel_all_stop_order()
                    self.cancel_all_send_order()

                    # self.write_log("[on_bar] line Direction.SHORT is_empty:{}, bar.close_price:{}, line_point:{}"\
                    #     .format(self.is_empty(), bar.close_price, self.line_point))
                    if self.is_empty() and bar.close_price < self.line_signal.get_line_point():
                        order_list = self.buy(self.symbol_pair, self.exchange, bar.close_price + 10, 1)
                        for vt_order_id, order in order_list:
                            self.send_order_dict[vt_order_id] = copy(order)

                    # if not self.is_empty():
                    #     self.write_log("len(self.limit_order_dict.keys()):{},len(self.stop_order_dict.keys()):{},len(self.send_order_dict.keys()):{}"\
                    #         .format(len(self.limit_order_dict.keys()), len(self.stop_order_dict.keys()), len(self.send_order_dict.keys())))

                elif self.pos < 0:
                    self.cancel_all_limit_order()
                    self.cancel_all_stop_order()
                    self.cancel_all_send_order()

                    self.buy(self.symbol_pair, self.exchange, bar.close_price + 10, abs(self.pos))

                elif self.pos > 1:
                    
                    self.sell(self.symbol_pair, self.exchange, bar.close_price - 10, abs(self.pos))
                # elif self.pos > 0:
                #     self.write_log("self.line_direction == Direction.SHORT.value Error condition self.pos > 0")
                # else:
                #     self.write_log("self.line_direction == Direction.SHORT.value Error condition self.pos < 0")

        self.line_signal.on_bar(bar)

    def on_stop_order(self, stop_order):
        self.write_log("[on_stop_order] {},{},{},{},{},{},{},{}".format(stop_order.vt_symbol, stop_order.direction, stop_order.offset,
                stop_order.price, stop_order.volume, stop_order.vt_order_id, stop_order.vt_order_ids, stop_order.status))
        if stop_order.status == StopOrderStatus.TRIGGERED.value:
            for vt_order_id, order in stop_order.vt_order_ids:
                self.transfer_order_dict[vt_order_id] = copy(order)

        if stop_order.vt_order_id in self.stop_order_dict.keys():
            if not stop_order.is_active():
                self.stop_order_dict.pop(stop_order.vt_order_id)

    def on_order(self, order):
        if order.status == Status.SUBMITTING.value:
            return

        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction ,order.price ,order.volume ,order.traded))

        if order.direction == Direction.LONG.value:
            #self.write_log( "[on_order] order.direction==LONG {}".format(self.send_order_dict.keys()))
            if order.vt_order_id in self.transfer_order_dict.keys():
                bef_order = self.transfer_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                   self.pos += new_traded

                self.transfer_order_dict[order.vt_order_id] = copy(order)

                if not order.is_active():
                    self.transfer_order_dict.pop(order.vt_order_id)

            elif order.vt_order_id in self.limit_order_dict.keys():
                bef_order = self.limit_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                   self.pos += new_traded
                self.limit_order_dict[order.vt_order_id] = copy(order)

                if not order.is_active():
                    self.limit_order_dict.pop(order.vt_order_id)

            elif order.vt_order_id in self.send_order_dict.keys():
                bef_order = self.send_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                    self.pos += new_traded
                    if abs(self.pos) > 0:
                        self.sell_buy_order( order.price, abs(self.pos))

                self.send_order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.send_order_dict.pop(order.vt_order_id)

        else:
            if order.vt_order_id in self.transfer_order_dict.keys():
                bef_order = self.transfer_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                   self.pos -= new_traded

                self.transfer_order_dict[order.vt_order_id] = copy(order)

                if not order.is_active():
                    self.transfer_order_dict.pop(order.vt_order_id)

            elif order.vt_order_id in self.limit_order_dict.keys():
                bef_order = self.limit_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                   self.pos -= new_traded
                self.limit_order_dict[order.vt_order_id] = copy(order)

                if not order.is_active():
                    self.limit_order_dict.pop(order.vt_order_id)

            elif order.vt_order_id in self.send_order_dict.keys():
                bef_order = self.send_order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                    self.pos -= new_traded
                    if abs(self.pos) > 0:
                        self.cover_sell_order( order.price, abs(self.pos))

                self.send_order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.send_order_dict.pop(order.vt_order_id)
        
    def on_trade(self, trade):
        #self.write_log('[on_trade] start')
        #self.write_log('[trade detail] :{}'.format(trade.__dict__))

        # if trade.direction == Direction.LONG.value:
        #     self.pos += trade.volume
        # else:
        #     self.pos -= trade.volume

        self.write_trade(trade)


    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.datetime, trade.vt_symbol, trade.vt_trade_id, trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)



