# coding=utf-8

import hashlib
import hmac
import sys
import time
from copy import copy

from tumbler.function import get_vt_key, parse_timestamp
from tumbler.constant import (
    Direction,
    Exchange,
    Product,
    Status
)
from tumbler.object import (
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData
)

from .ws_api_base import BitmexWsApiBase
from .base import ORDER_TYPE_BITMEX2VT, DIRECTION_BITMEX2VT, STATUS_BITMEX2VT
from .base import change_from_system_to_bitmex, change_from_bitmex_to_system, parse_order_info


class BitmexWsTradeApi(BitmexWsApiBase):

    def __init__(self, gateway):
        super(BitmexWsTradeApi, self).__init__(gateway)

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.order_manager = gateway.order_manager
        self.order_manager.push_data_callback = self.on_packet

        self.key = ""
        self.secret = ""

        self.callbacks = {
            "execution": self.on_trade,
            "order": self.on_order,
            "position": self.on_position,
            "margin": self.on_account,
            "instrument": self.on_contract,
        }

        self.accounts = {}
        self.orders = {}
        self.positions = {}
        # self.trades = set()

        self.set_all_symbols = set([])

    def subscribe(self, req):
        """
        Subscribe to tick data upate.
        """
        symbol = req.symbol
        if symbol not in self.set_all_symbols:
            self._subscribe(symbol)
        self.set_all_symbols.add(symbol)

    def _subscribe(self, symbol):
        bitmex_symbol = change_from_system_to_bitmex(symbol)
        req = {
            "op": "subscribe",
            "args": [
                "instrument",
                "execution:{}".format(bitmex_symbol),
                "order:{}".format(bitmex_symbol),
                "position:{}".format(bitmex_symbol),
                "margin"
            ]
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log("BitmexWsTradeApi connect successily!")
        self.authenticate()

    def on_disconnected(self):
        self.gateway.write_log("BitmexWsTradeApi disconnected!")

    def on_packet(self, packet):
        if "error" in packet:
            self.gateway.write_log("Websocket API Error:%s" % packet["error"])

            if "not valid" in packet["error"]:
                self.active = False

        elif "request" in packet:
            req = packet["request"]
            success = packet["success"]

            if success:
                if req["op"] == "authKey":
                    self.gateway.write_log("Websocket API auth success!")
                    self.subscribe_topic()

        elif "table" in packet:
            name = packet["table"]
            callback = self.callbacks[name]

            if isinstance(packet["data"], list):
                for d in packet["data"]:
                    callback(d)
            else:
                callback(packet["data"])

    def on_error(self, exception_type, exception_value, tb):
        msg = "touch error, status_code:{},information:{}".format(exception_type, exception_value)
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb))

    def authenticate(self):
        """
        Authenticate websockey connection to subscribe private topic.
        """
        expires = int(time.time())
        method = "GET"
        path = "/realtime"
        msg = method + path + str(expires)
        signature = hmac.new(self.secret.encode("utf-8"), msg.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()

        req = {"op": "authKey", "args": [self.key, expires, signature]}
        self.send_packet(req)

    def subscribe_topic(self):
        """
        Subscribe to all private topics.
        """
        for symbol in self.set_all_symbols:
            self._subscribe(symbol)

    def on_trade(self, d):
        pass

    def on_order(self, data):
        # Filter order data which cannot be processed properly
        '''
        {"orderID":"079fce75-c2ad-4c12-844a-b8a1254040f8",
        "ordStatus":"Canceled","workingIndicator":false,"leavesQty":0,"text":"Canceled:
        Spam\nSubmission from www.bitmex.com","timestamp":"2020-10-29T02:51:07.298Z","clOrdID":"",
        "account":1474991,"symbol":"XBTUSD"}
        '''
        if "ordStatus" not in data:
            return

        sys_order_id = data["orderID"]
        order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
        if not order:
            self.order_manager.add_push_data(sys_order_id, data)
            return

        self.gateway.write_log("ws on_order:{}".format(data))
        if "cumQty" in data.keys():
            traded_volume = float(data["cumQty"])
        else:
            traded_volume = order.traded
        new_traded_volume = traded_volume - order.traded
        if new_traded_volume > 0:
            order.traded = traded_volume - order.traded

        order.status = STATUS_BITMEX2VT[data["ordStatus"]]
        self.order_manager.on_order(order)

        if new_traded_volume > 0:
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
            trade.price = order.price
            trade.volume = new_traded_volume
            trade.trade_time = parse_timestamp(data["timestamp"])
            trade.gateway_name = self.gateway_name

            self.gateway.on_trade(trade)

    def on_position(self, d):
        symbol = change_from_bitmex_to_system(d["symbol"])

        position = self.positions.get(symbol, None)
        if not position:
            position = PositionData()
            position.symbol = change_from_bitmex_to_system(d["symbol"])
            position.exchange = Exchange.BITMEX.value
            position.vt_symbol = get_vt_key(position.symbol, position.exchange)
            position.direction = Direction.NET.value
            position.gateway_name = self.gateway_name
            position.vt_position_id = get_vt_key(position.vt_symbol, position.direction)

            self.positions[symbol] = position

        volume = d.get("currentQty", None)
        if volume is not None:
            position.position = volume
            if position.position > 0:
                position.vt_position_id = get_vt_key(position.vt_symbol, Direction.LONG.value)
            elif position.position < 0:
                position.vt_position_id = get_vt_key(position.vt_symbol, Direction.SHORT.value)
            else:
                position.vt_position_id = get_vt_key(position.vt_symbol, Direction.NET.value)

        price = d.get("avgEntryPrice", None)
        if price is not None:
            position.price = price

        self.gateway.on_position(copy(position))

    def on_account(self, d):
        account_id = str(d["account"])
        account = self.accounts.get(account_id, None)
        if not account:
            account = AccountData()
            account.account_id = account_id
            account.vt_account_id = get_vt_key(Exchange.BITMEX.value, account.account_id)
            account.gateway_name = self.gateway_name

            self.accounts[account_id] = account

        account.balance = d.get("marginBalance", account.balance)
        account.available = d.get("availableMargin", account.available)
        account.frozen = account.balance - account.available

        self.gateway.on_account(copy(account))

    def on_contract(self, d):
        if "tickSize" not in d:
            return

        if not d["lotSize"]:
            return

        contract = ContractData()
        contract.symbol = change_from_bitmex_to_system(d["symbol"])
        contract.exchange = Exchange.BITMEX.value
        contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
        contract.name = contract.symbol
        contract.size = d["lotSize"]
        contract.price_tick = d["tickSize"]
        contract.volume_tick = d["lotSize"]
        contract.min_volume = d["lotSize"]
        contract.product = Product.FUTURES.value

        self.gateway.on_contract(contract)
