# coding=utf-8
from copy import copy

from tumbler.function import get_vt_key
from tumbler.constant import (
    Exchange
)

from tumbler.object import TradeData, PositionData
from tumbler.function import get_dt_use_timestamp, simplify_tick
from .ws_api_base import NexusApiBase
from tumbler.constant import Direction
from .base import WEBSOCKET_TRADE_HOST
from .base import STATUS_NEXUS2VT


class NexusWsTradeApi(NexusApiBase):
    """
    Nexus WS MARKET API
    """

    def __init__(self, gateway):
        super(NexusWsTradeApi, self).__init__(gateway)

        self.url = WEBSOCKET_TRADE_HOST
        self.order_manager = gateway.order_manager
        self.order_manager.push_data_callback = self.on_data

        self.req_id = 0

        self.set_all_symbols = set([])

    def get_trade_id(self):
        self.req_id = self.req_id + 1
        return str(self.req_id)

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def on_connected(self):
        self.gateway.write_log("NexusWsTradeApi connect success!")
        self.login()

    def on_login(self, packet):
        self.gateway.write_log("NexusWsTradeApi login success!")

    def on_data(self, packet):
        event = packet.get("event")
        if event == "BOOK":
            self.on_market_depth(packet)
        elif event == "TRADE":
            self.on_market_trade(packet)
        elif event == "ORDER_UPDATE":
            self.on_order(packet)
        elif event == "TRANSACTION":
            self.on_trade(packet)
        elif event == "POSITIONS":
            self.on_position(packet)
        else:
            self.gateway.write_log("[on_data] data:{}".format(packet))

    def on_market_depth(self, packet):
        data = packet["data"]
        self.ticker.datetime = get_dt_use_timestamp(packet["timestamp"])
        asks = data["asks"]
        asks = [(x["price"], x["quantity"]) for x in asks]

        bids = data["bids"]
        bids = [(x["price"], x["quantity"]) for x in bids]

        simplify_tick(self.ticker, bids, asks)
        self.gateway.on_ws_tick(copy(self.ticker))

    def on_market_trade(self, data):
        pass

    def on_order(self, packet):
        try:
            data = packet["data"]
            sys_order_id = str(data["order_id"])
            order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
            if not order:
                self.order_manager.add_push_data(sys_order_id, data)
                return

            new_traded_volume = float(data["executed_quantity"])
            # Push order event
            order.deal_price = float(data["executed_price"])
            order.traded = float(data["aggregated_executed_quantity"])
            order.status = STATUS_NEXUS2VT[data["status"]]
            self.order_manager.on_order(copy(order))

            # Push trade event
            if not new_traded_volume > 0:
                return

            trade = TradeData()
            trade.symbol = order.symbol
            trade.exchange = Exchange.NEXUS.value
            trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
            trade.order_id = order.order_id
            trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
            trade.trade_id = self.get_trade_id()
            trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
            trade.direction = order.direction
            trade.type = order.type
            trade.offset = order.offset
            trade.price = float(data["executed_price"])
            trade.volume = float(data["filled-amount"])
            trade.trade_time = get_dt_use_timestamp(packet["timestamp"])
            trade.gateway_name = self.gateway_name

            self.gateway.on_trade(trade)
        except Exception as ex:
            self.gateway.write_log("[on_order] ex:{} data:{}".format(ex, packet))

    def on_trade(self, data):
        pass

    def on_position(self, packet):
        try:
            data = packet["data"]
            for symbol, d in data["positions"].items():
                position = PositionData()
                position.symbol = symbol.lower()
                position.exchange = Exchange.NEXUS.value
                position.vt_symbol = get_vt_key(position.symbol, position.exchange)
                position.position = d["holding"]
                if position.position > 0:
                    position.direction = Direction.LONG.value
                else:
                    position.direction = Direction.SHORT.value
                position.frozen = 0
                position.vt_position_id = get_vt_key(position.symbol, Direction.NET.value)
                self.gateway.on_position(position)
        except Exception as ex:
            self.gateway.write_log("[on_position] ex:{} data:{}".format(ex, packet))