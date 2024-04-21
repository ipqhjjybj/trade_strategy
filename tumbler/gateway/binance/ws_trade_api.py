# coding=utf-8

from tumbler.api.websocket import WebsocketClient
from tumbler.constant import (
    Direction,
    Exchange
)
from tumbler.object import (
    OrderData,
    TradeData,
    AccountData
)
from tumbler.constant import Offset
from tumbler.function import get_vt_key, get_str_dt_use_timestamp
from .base import STATUS_BINANCE2VT, DIRECTION_BINANCE2VT, change_binance_format_to_system_format
from .base import ORDER_TYPE_BINANCE2VT


class BinanceWsTradeApi(WebsocketClient):

    def __init__(self, gateway):
        super(BinanceWsTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.set_all_symbols = set([])

    def connect(self, url, proxy_host="", proxy_port=0):
        self.init(url, proxy_host, proxy_port)
        self.start()

    def on_connected(self):
        self.gateway.write_log("BinanceWsTradeApi connect success!")

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def on_packet(self, packet):
        if packet["e"] == "outboundAccountInfo":
            self.on_account(packet)
        elif packet["e"] == "executionReport":
            self.on_order(packet)

    def on_account(self, packet):
        for d in packet["B"]:
            account = AccountData()
            account.account_id = d["a"].lower()
            account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
            account.balance = float(d["f"]) + float(d["l"])
            account.frozen = float(d["l"])
            account.available = float(d["f"])
            account.gateway_name = self.gateway_name

            if account.balance:
                self.gateway.on_account(account)

    def on_order(self, packet):
        symbol = change_binance_format_to_system_format(packet["s"])

        if packet["C"] in ["null", ""]:
            order_id = packet["c"]
        else:
            order_id = packet["C"]

        order = OrderData()
        order.symbol = symbol
        order.exchange = Exchange.BINANCE.value
        order.vt_symbol = get_vt_key(order.symbol, order.exchange)
        order.order_id = order_id
        order.vt_order_id = get_vt_key(order.order_id, order.exchange)
        order.type = ORDER_TYPE_BINANCE2VT[packet["o"]]
        order.direction = DIRECTION_BINANCE2VT[packet["S"]]
        order.price = float(packet["p"])
        order.volume = float(packet["q"])
        order.traded = float(packet["z"])
        order.status = STATUS_BINANCE2VT[packet["X"]]
        if order.direction == Direction.LONG.value:
            order.offset = Offset.OPEN.value
        else:
            order.offset = Offset.CLOSE.value
        order.time = get_str_dt_use_timestamp(packet["O"])
        order.gateway_name = self.gateway_name

        if not self.order_manager.has_order_id(order_id):
            if self.gateway.rest_trade_api and order.is_active():
                self.gateway.rest_trade_api.cancel_system_order(order_id, symbol)
            return

        self.order_manager.on_order(order)

        # Push trade event
        trade_volume = float(packet["l"])
        if not trade_volume:
            return

        trade_time = get_str_dt_use_timestamp(packet["T"])

        trade = TradeData()
        trade.symbol = order.symbol
        trade.exchange = Exchange.BINANCE.value
        trade.order_id = order.order_id
        trade.vt_order_id = order.vt_order_id
        trade.trade_id = packet["t"]
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = order.direction
        trade.type = order.type
        trade.offset = order.offset
        trade.price = float(packet["L"])
        trade.volume = trade_volume
        trade.trade_time = trade_time
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)
