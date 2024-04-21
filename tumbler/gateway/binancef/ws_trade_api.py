# coding=utf-8

from tumbler.api.websocket import WebsocketClient
from tumbler.function import get_vt_key, get_str_dt_use_timestamp
from tumbler.gateway.binance.base import change_binance_format_to_system_format
from tumbler.gateway.binance.base import ORDER_TYPE_BINANCE2VT
from tumbler.object import AccountData, PositionData, OrderData, TradeData
from tumbler.constant import Direction, Exchange
from .base import DIRECTION_BINANCEF2VT, STATUS_BINANCEF2VT, WEBSOCKET_TRADE_HOST


class BinancefWsTradeApi(WebsocketClient):

    def __init__(self, gateway):
        super(BinancefWsTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.set_all_symbols = set([])

    def connect(self, url="", proxy_host="", proxy_port=0):
        if not url:
            url = WEBSOCKET_TRADE_HOST
        self.init(url, proxy_host, proxy_port)
        self.start()

    def on_connected(self):
        self.gateway.write_log("BinancefWsTradeApi connect success!")

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def on_packet(self, packet):
        if packet["e"] == "ACCOUNT_UPDATE":
            self.on_account(packet)
        elif packet["e"] == "ORDER_TRADE_UPDATE":
            self.on_order(packet)

    def on_account(self, packet):
        for acc_data in packet["a"]["B"]:
            account = AccountData()
            account.account_id = acc_data["a"]
            account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
            account.balance = float(acc_data["wb"])
            account.frozen = float(acc_data["wb"]) - float(acc_data["cw"])
            account.available = account.balance
            account.gateway_name = self.gateway_name

            if account.balance:
                self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            pos = PositionData()
            pos.symbol = pos_data["s"]
            pos.exchange = Exchange.BINANCEF.value
            pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
            pos.direction = Direction.NET.value
            pos.position = float(pos_data["pa"]),
            pos.frozen = 0
            pos.vt_position_id = get_vt_key(pos.vt_symbol, Direction.NET.value)
            pos.price = float(pos_data["ep"])
            pos.gateway_name = self.gateway_name

            self.gateway.on_position(pos)

    def on_order(self, packet):
        ord_data = packet["o"]
        symbol = change_binance_format_to_system_format(ord_data["s"])
        order_id = str(ord_data["c"])

        order = OrderData()
        order.symbol = symbol
        order.exchange = Exchange.BINANCEF.value
        order.vt_symbol = get_vt_key(order.symbol, order.exchange)
        order.order_id = order_id
        order.vt_order_id = get_vt_key(order.order_id, order.exchange)
        order.type = ORDER_TYPE_BINANCE2VT[ord_data["o"]]
        order.direction = DIRECTION_BINANCEF2VT[ord_data["S"]]
        order.price = float(ord_data["p"])
        order.volume = float(ord_data["q"])
        order.traded = float(ord_data["z"])
        order.status = STATUS_BINANCEF2VT[ord_data["X"]]
        order.order_time = get_str_dt_use_timestamp(packet["E"])
        order.gateway_name = self.gateway_name

        self.gateway.on_order(order)

        # Push trade event
        trade_volume = float(ord_data["l"])
        if not trade_volume:
            return

        trade = TradeData()
        trade.symbol = order.symbol
        trade.exchange = Exchange.BINANCEF.value
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.order_id = order.order_id
        trade.vt_order_id = order.vt_order_id
        trade.trade_id = ord_data["t"]
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = order.direction
        trade.type = order.type
        trade.offset = order.offset
        trade.price = float(ord_data["L"])
        trade.volume = trade_volume
        trade.trade_time = get_str_dt_use_timestamp(ord_data["T"])
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)

        if not self.order_manager.has_order_id(order_id):
            if self.gateway.rest_trade_api and order.is_active():
                self.gateway.rest_trade_api.cancel_system_order(order_id, symbol)
