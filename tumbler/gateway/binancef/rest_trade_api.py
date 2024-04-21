# coding=utf-8

import hashlib
import hmac
import time
from datetime import datetime
from threading import Lock

from tumbler.api.rest import RestClient
from tumbler.function import get_vt_key, urlencode
from tumbler.gateway.binance.base import change_system_format_to_binance_format, \
    change_binance_format_to_system_format, Security, asset_from_other_exchanges_to_binance
from tumbler.gateway.binancef.base import parse_contract_arr
from tumbler.object import AccountData, PositionData
from tumbler.constant import Status, Direction, Exchange

from .base import parse_order_info
from .base import WEBSOCKET_TRADE_HOST, REST_TRADE_HOST
from .base import DIRECTION_VT2BINANCEF


class BinancefRestTradeApi(RestClient):

    def __init__(self, gateway):
        super(BinancefRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.ws_trade_api = self.gateway.ws_trade_api

        self.set_all_symbols = set([])

        self.key = ""
        self.secret = ""

        self.user_stream_key = ""
        self.keep_alive_count = 0
        self.recv_window = 5000
        self.time_offset = 0

        self.order_count = 1000000
        self.order_count_lock = Lock()
        self.connect_time = 0

    def sign(self, request):
        """
        Generate BINANCE signature.
        """
        security = request.data["security"]
        if security == Security.NONE.value:
            request.data = None
            return request

        if request.params:
            path = request.path + "?" + urlencode(request.params)
        else:
            request.params = dict()
            path = request.path

        if security == Security.SIGNED.value:
            timestamp = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["recvWindow"] = self.recv_window
            request.params["timestamp"] = timestamp

            query = urlencode(sorted(request.params.items()))
            signature = hmac.new(self.secret.encode('utf-8'), query.encode("utf-8"), hashlib.sha256).hexdigest()

            query += "&signature={}".format(signature)
            path = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # Add headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key
        }

        if security in [Security.SIGNED.value, Security.API_KEY.value]:
            request.headers = headers

        return request

    def connect(self, key, secret, proxy_host="", proxy_port=0):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count

        self.init(REST_TRADE_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BinanceRestTradeApi start success!")

        self.query_time()
        self.query_contract()
        self.query_account()
        self.query_position()
        self.query_order()
        self.start_user_stream()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_time(self):
        data = {
            "security": Security.NONE.value
        }
        path = "/fapi/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_account(self):
        data = {"security": Security.SIGNED.value}
        self.add_request(
            method="GET",
            path="/fapi/v1/account",
            callback=self.on_query_account,
            data=data
        )

    def query_position(self):
        data = {"security": Security.SIGNED.value}

        self.add_request(
            method="GET",
            path="/fapi/v1/positionRisk",
            callback=self.on_query_position,
            data=data
        )

    def query_order(self):
        data = {"security": Security.SIGNED.value}

        self.add_request(
            method="GET",
            path="/fapi/v1/openOrders",
            callback=self.on_query_open_orders,
            data=data
        )

    def query_complete_orders(self, list_order_ids):
        data = {"security": Security.SIGNED.value}
        for order_id in list_order_ids:
            order = self.order_manager.get_order(order_id)
            if order:
                params = {
                    "symbol": change_system_format_to_binance_format(order.symbol),
                    "origClientOrderId": order_id
                }
                self.add_request(
                    method="GET",
                    path="/fapi/v1/order",
                    params=params,
                    data=data,
                    callback=self.on_query_order
                )

    def query_contract(self):
        data = {
            "security": Security.NONE.value
        }
        self.add_request(
            method="GET",
            path="/fapi/v1/exchangeInfo",
            callback=self.on_query_contract,
            data=data
        )

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req):
        order_id = str(self.connect_time + self._new_order_id())
        order = req.create_order_data(
            order_id,
            self.gateway_name
        )

        self.set_all_symbols.add(req.symbol)

        self.order_manager.on_order(order)

        data = {
            "security": Security.SIGNED.value
        }

        params = {
            "symbol": change_system_format_to_binance_format(req.symbol),
            "timeInForce": "GTC",
            "side": DIRECTION_VT2BINANCEF[req.direction],
            "type": "LIMIT",
            "price": float(req.price),
            "quantity": req.volume,
            "newClientOrderId": order_id,
            "newOrderRespType": "ACK"
        }

        self.add_request(
            method="POST",
            path="/fapi/v1/order",
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_order_id

    def cancel_order(self, req):
        data = {
            "security": Security.SIGNED.value
        }

        params = {
            "symbol": change_system_format_to_binance_format(req.symbol),
            "origClientOrderId": req.order_id
        }

        self.add_request(
            method="DELETE",
            path="/fapi/v1/order",
            callback=self.on_cancel_order,
            params=params,
            data=data,
            extra=req
        )

    def cancel_system_order(self, client_id, symbol):
        data = {
            "security": Security.SIGNED.value
        }

        params = {
            "symbol": change_system_format_to_binance_format(symbol),
            "origClientOrderId": client_id
        }

        self.add_request(
            method="DELETE",
            path="/fapi/v1/order",
            callback=self.on_cancel_system_order,
            params=params,
            data=data,
            extra=client_id
        )

    def start_user_stream(self):
        data = {
            "security": Security.API_KEY.value
        }

        self.add_request(
            method="POST",
            path="/fapi/v1/listenKey",
            callback=self.on_start_user_stream,
            data=data
        )

    def keep_user_stream(self):
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data = {
            "security": Security.API_KEY.value
        }

        params = {
            "listenKey": self.user_stream_key
        }

        self.add_request(
            method="PUT",
            path="/fapi/v1/listenKey",
            callback=self.on_keep_user_stream,
            params=params,
            data=data
        )

    def on_query_time(self, data, request):
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

    def on_query_account(self, data, request):
        for asset in data["assets"]:
            account = AccountData()
            account.account_id = asset_from_other_exchanges_to_binance(asset["asset"].lower())
            account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
            account.balance = float(asset["marginBalance"])
            account.frozen = float(asset["maintMargin"])
            account.available = float(asset["walletBalance"])
            account.gateway_name = self.gateway_name

            self.gateway.on_account(account)

        self.gateway.write_log("[on_query_account] query account success!")

    def on_query_position(self, data, request):
        for d in data:
            pos = PositionData()
            pos.symbol = change_binance_format_to_system_format(d["symbol"])
            pos.exchange = Exchange.BINANCEF.value
            pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
            pos.direction = Direction.NET.value
            pos.position = float(d["positionAmt"])
            pos.frozen = 0
            pos.vt_position_id = get_vt_key(pos.vt_symbol, Direction.NET.value)
            pos.price = float(d["entryPrice"])

            if pos.position:
                self.gateway.on_position(pos)

    def on_query_open_orders(self, data, request):
        now_has_order_ids = self.order_manager.get_all_order_ids()
        for d in data:
            symbol = change_binance_format_to_system_format(d["symbol"])

            if symbol not in self.set_all_symbols:
                continue
            sys_order_id = str(d["clientOrderId"])

            if not self.order_manager.has_order_id(sys_order_id):
                self.cancel_system_order(sys_order_id, symbol)
                continue

            now_has_order_ids.remove(sys_order_id)
            # 币安系统，local_order_id 就是 ClientOrderId
            order = parse_order_info(d, sys_order_id, self.gateway_name)

            if order.status == Status.SUBMITTING.value:
                self.gateway.write_log("[on_query_open_orders] BINANCE new order status data:{}".format(d))
            self.gateway.on_order(order)

        if now_has_order_ids:
            self.query_complete_orders(now_has_order_ids)

    def on_query_order(self, data, request):
        order = parse_order_info(data, data["clientOrderId"], self.gateway_name)
        self.order_manager.on_order(order)

    def on_query_contract(self, data, request):
        arr = parse_contract_arr(data, Exchange.BINANCEF.value, self.gateway_name)
        for contract in arr:
            self.gateway.on_contract(contract)

    def on_send_order(self, data, request):
        pass

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        self.gateway.on_order(order)

        msg = "send_order failed,status:{},information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data, request):
        pass

    def on_cancel_system_order(self, data, request):
        msg = "on_cancel_system_order , sys_order_id:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_start_user_stream(self, data, request):
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.gateway.ws_trade_api:
            self.ws_trade_api.connect(WEBSOCKET_TRADE_HOST + self.user_stream_key, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data, request):
        pass
