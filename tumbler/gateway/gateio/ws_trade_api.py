# coding=utf-8

import time

from tumbler.function import get_vt_key, get_str_dt_use_timestamp

from tumbler.constant import (
    Exchange,
    Status
)
from tumbler.object import (
    TradeData
)

from .base import WEBSOCKET_TRADE_HOST, change_system_format_to_gateio_format
from .ws_api_base import GateioWsApiBase


class GateioWsTradeApi(GateioWsApiBase):

    def __init__(self, gateway):
        super(GateioWsTradeApi, self).__init__(gateway)

        self.order_manager = gateway.order_manager
        self.order_manager.push_data_callback = self.on_data

        self.req_id = 10
        self.url = WEBSOCKET_TRADE_HOST
        self.set_all_symbols = set([])

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

        self.req_id += 1
        req = {
            "id": self.req_id,
            "method": "order.subscribe",
            "params": [change_system_format_to_gateio_format(x).upper() for x in list(self.set_all_symbols)]
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log("GateioWsTradeApi connect success!")
        self.login()

    def on_login(self):
        self.gateway.write_log("GateioWsTradeApi login success!")

        self.req_id += 1
        req = {
            "id": self.req_id,
            "method": "order.subscribe",
            "params": [change_system_format_to_gateio_format(x).upper() for x in list(self.set_all_symbols)]
        }
        self.send_packet(req)

    def on_data(self, packet):
        method = packet.get("method", None)
        if method is not None and "order.update" == method:
            self.on_order(packet)

    def on_order(self, data):
        int_event, order_info = data["params"]
        sys_order_id = str(order_info["id"])

        order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
        if not order:
            self.order_manager.add_push_data(sys_order_id, data)
            return

        traded_volume = float(order_info["amount"]) - float(order_info["left"])
        new_traded_volume = traded_volume - order.traded

        # Push order event
        order.traded = traded_volume
        if int_event == 1:
            order.status = Status.NOTTRADED.value
        elif int_event == 2:
            order.status = Status.PARTTRADED.value
        elif int_event == 3:
            if float(order_info["left"]) > 0:
                order.status = Status.CANCELLED.value
            else:
                order.status = Status.ALLTRADED.value
        else:
            order.status = Status.REJECTED.value

        self.order_manager.on_order(order)

        # Push trade event
        if not new_traded_volume:
            return

        trade = TradeData()
        trade.symbol = order.symbol
        trade.exchange = Exchange.GATEIO.value
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.order_id = order.order_id
        trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
        trade.trade_id = sys_order_id + "_" + str(time.time())
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = order.direction
        trade.type = order.type
        trade.offset = order.offset
        trade.price = float(order_info["price"])
        trade.volume = new_traded_volume
        trade.trade_time = get_str_dt_use_timestamp(order_info["mtime"], 1)
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)
