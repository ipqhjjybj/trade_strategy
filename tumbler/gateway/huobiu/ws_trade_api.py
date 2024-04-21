# coding=utf-8

from tumbler.function import get_vt_key, get_str_dt_use_timestamp

from tumbler.constant import (
    Exchange
)

from tumbler.object import (
    TradeData,
    SubscribeRequest
)
from tumbler.gateway.huobi.ws_api_base import HuobiWsApiBase, create_signature
from tumbler.gateway.huobis.base import STATUS_HUOBIS2VT

from .base import get_huobi_future_system_format_symbol
from .base import WEBSOCKET_TRADE_HOST


class HuobiuWsTradeApi(HuobiWsApiBase):
    def __init__(self, gateway):
        super(HuobiuWsTradeApi, self).__init__(gateway)

        self.url = WEBSOCKET_TRADE_HOST
        self.order_manager = gateway.order_manager
        self.order_manager.push_data_callback = self.on_data

        self.req_id = 0
        self.set_all_symbols = set([])

        self.trade_id = 0

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

        self.req_id += 1
        req = {
            "op": "sub",
            "cid": str(self.req_id),
            "topic": "orders_cross.{}".format(get_huobi_future_system_format_symbol(req.symbol))
        }
        self.send_packet(req)

    def login(self):
        self.req_id += 1
        params = {"op": "auth", "type": "api", "cid": str(self.req_id)}
        params.update(create_signature(self.key, "GET", self.sign_host, self.path, self.secret))
        return self.send_packet(params)

    def on_connected(self):
        self.gateway.write_log("HuobiuWsTradeApi connect success!")
        self.login()

    def on_login(self):
        self.gateway.write_log("HuobiuWsTradeApi login success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.HUOBIU.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def on_data(self, packet):
        op = packet.get("op", None)
        if op != "notify":
            return

        topic = packet["topic"]
        if "orders" in topic:
            self.on_order(packet)

    def on_order(self, data):
        sys_order_id = str(data["order_id"])

        order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
        if not order:
            self.order_manager.add_push_data(sys_order_id, data)
            return

        # Push order event
        order.traded = float(data["trade_volume"])
        order.status = STATUS_HUOBIS2VT.get(data["status"], None)
        self.order_manager.on_order(order)

        if float(data["trade_volume"]) > 0 and not order.is_active():
            for trade_dic in data["trade"]:
                trade = TradeData()
                trade.symbol = order.symbol
                trade.exchange = Exchange.HUOBIU.value
                trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
                trade.order_id = order.order_id
                trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
                trade.trade_id = str(trade_dic["trade_id"])
                trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
                trade.direction = order.direction
                trade.type = order.type
                trade.offset = order.offset
                trade.price = trade_dic["trade_price"]
                trade.volume = trade_dic["trade_volume"]
                trade.trade_time = get_str_dt_use_timestamp(trade_dic["created_at"])
                trade.gateway_name = self.gateway_name

                self.gateway.on_trade(trade)

