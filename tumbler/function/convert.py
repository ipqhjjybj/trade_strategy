# coding=utf-8

import tumbler.config as config
from tumbler.service import MQReceiver
from tumbler.function import get_vt_key
from tumbler.constant import Exchange, Direction, OrderType, Offset
from tumbler.engine import BaseEngine
from tumbler.object import SubscribeRequest, TickData, MergeTickData, OrderRequest


class PositionHolding:
    def __init__(self, contract):
        self.vt_symbol = contract.vt_symbol
        self.exchange = contract.exchange

        self.active_orders = {}

        self.long_pos = 0
        self.short_pos = 0

        self.long_pos_frozen = 0
        self.short_pos_frozen = 0

    def update_position(self, position):
        if position.direction == Direction.NET.value:
            if position.position > 0:
                self.long_pos = position.position
                self.short_pos = 0
            elif position.position < 0:
                self.long_pos = 0
                self.short_pos = abs(position.position)
            else:
                self.long_pos = 0 
                self.short_pos = 0
                
        elif position.direction == Direction.LONG.value:
            self.long_pos = position.position
        else:
            self.short_pos = position.position

    def update_order(self, order):
        if order.is_active():
            self.active_orders[order.vt_order_id] = order
        else:
            if order.vt_order_id in self.active_orders:
                self.active_orders.pop(order.vt_order_id)

        self.calculate_frozen()

    def calculate_frozen(self):
        self.long_pos_frozen = 0
        self.short_pos_frozen = 0

        for order in self.active_orders.values():
            frozen = order.volume - order.traded
            if order.direction == Direction.LONG.value:
                self.long_pos_frozen += frozen
            elif order.direction == Direction.SHORT.value:
                self.short_pos_frozen += frozen

    def get_req_list(self, symbol, exchange, direction, price, volume):
        if self.exchange in [Exchange.BITMEX.value, Exchange.COINEXS.value]:
            ret = []
            req = OrderRequest()
            req.symbol = symbol
            req.exchange = exchange
            req.vt_symbol = self.vt_symbol

            req.price = price
            req.volume = volume

            req.type = OrderType.LIMIT.value
            req.direction = direction
            req.offset = Offset.OPEN.value

            ret.append(req)

            return ret
        else:
            ret = []
            if direction == Direction.LONG.value:
                pos = self.short_pos - self.short_pos_frozen
                if pos >= volume:
                    if volume > 0:
                        req = OrderRequest()
                        req.symbol = symbol
                        req.exchange = exchange
                        req.vt_symbol = self.vt_symbol

                        req.price = price
                        req.volume = volume

                        req.type = OrderType.LIMIT.value
                        req.direction = Direction.LONG.value
                        req.offset = Offset.CLOSE.value

                        ret.append(req)
                else:
                    if pos > 0:
                        req1 = OrderRequest()
                        req1.symbol = symbol
                        req1.exchange = exchange
                        req1.vt_symbol = self.vt_symbol

                        req1.price = price
                        req1.volume = pos

                        req1.type = OrderType.LIMIT.value
                        req1.direction = Direction.LONG.value
                        req1.offset = Offset.CLOSE.value
                        ret.append(req1)

                    new_volume = volume - pos
                    
                    if new_volume > 0:
                        req2 = OrderRequest()
                        req2.symbol = symbol
                        req2.exchange = exchange
                        req2.vt_symbol = self.vt_symbol
                        req2.price = price
                        req2.volume = new_volume
                        req2.type = OrderType.LIMIT.value
                        req2.direction = Direction.LONG.value
                        req2.offset = Offset.OPEN.value 
                        ret.append(req2)
            else:
                pos = self.long_pos - self.long_pos_frozen
                if pos >= volume:
                    if volume > 0:
                        req = OrderRequest()
                        req.symbol = symbol
                        req.exchange = exchange
                        req.vt_symbol = self.vt_symbol

                        req.price = price
                        req.volume = volume

                        req.type = OrderType.LIMIT.value
                        req.direction = Direction.SHORT.value
                        req.offset = Offset.CLOSE.value

                        ret.append(req)
                else:
                    if pos > 0:
                        req1 = OrderRequest()
                        req1.symbol = symbol
                        req1.exchange = exchange
                        req1.vt_symbol = self.vt_symbol

                        req1.price = price
                        req1.volume = pos

                        req1.type = OrderType.LIMIT.value
                        req1.direction = Direction.SHORT.value
                        req1.offset = Offset.CLOSE.value
                        ret.append(req1)

                    new_volume = volume - pos
                    
                    if new_volume > 0:
                        req2 = OrderRequest()
                        req2.symbol = symbol
                        req2.exchange = exchange
                        req2.vt_symbol = self.vt_symbol
                        req2.price = price
                        req2.volume = new_volume
                        req2.type = OrderType.LIMIT.value
                        req2.direction = Direction.SHORT.value
                        req2.offset = Offset.OPEN.value 
                        ret.append(req2)
            return ret