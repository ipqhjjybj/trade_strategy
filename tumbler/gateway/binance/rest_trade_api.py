# coding=utf-8

import time
from datetime import datetime
from threading import Lock

from tumbler.api.rest import RestClient
from tumbler.constant import (
    Exchange,
    Status,
)

from .base import parse_contract_info, sign_request, parse_account_info_arr, parse_order_info
from .base import Security, REST_TRADE_HOST, change_system_format_to_binance_format
from .base import DIRECTION_VT2BINANCE, change_binance_format_to_system_format, ORDER_TYPE_VT2BINANCE
from .base import WEBSOCKET_TRADE_HOST


class BinanceRestTradeApi(RestClient):
    """
    BINANCE REST API
    """

    def __init__(self, gateway):
        super(BinanceRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.ws_trade_api = self.gateway.ws_trade_api

        self.host = ""
        self.key = ""
        self.secret = ""

        self.user_stream_key = ""
        self.keep_alive_count = 0
        self.recv_window = 50000
        self.time_offset = 0
        self.proxy_host = ""
        self.proxy_port = 0

        self.order_count = 1000000
        self.order_count_lock = Lock()
        self.connect_time = 0

        self.set_all_symbols = set([])

    def sign(self, request):
        """
        Generate BINANCE signature.
        """
        return sign_request(request, self.key, self.secret, self.recv_window, self.time_offset)

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
        self.query_account()
        self.query_order()
        self.query_contract()
        self.start_user_stream()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_time(self):
        data = {
            "security": Security.NONE.value
        }
        path = "/api/v1/time"

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
            path="/api/v3/account",
            callback=self.on_query_account,
            data=data
        )

    def query_order(self):
        data = {"security": Security.SIGNED.value}
        self.add_request(
            method="GET",
            path="/api/v3/openOrders",
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
                    path="/api/v3/order",
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
            path="/api/v1/exchangeInfo",
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
            "side": DIRECTION_VT2BINANCE[req.direction],
            "type": ORDER_TYPE_VT2BINANCE[req.type],
            "price": str(req.price),
            "quantity": str(req.volume),
            "newClientOrderId": order_id,
            "newOrderRespType": "ACK"
        }

        self.add_request(
            method="POST",
            path="/api/v3/order",
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
            path="/api/v3/order",
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
            path="/api/v3/order",
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
            path="/api/v1/userDataStream",
            callback=self.on_start_user_stream,
            data=data
        )

    def keep_user_stream(self):
        self.keep_alive_count += 1
        if self.keep_alive_count < 1800:
            return

        data = {
            "security": Security.API_KEY.value
        }

        params = {
            "listenKey": self.user_stream_key
        }

        self.add_request(
            method="PUT",
            path="/api/v1/userDataStream",
            callback=self.on_keep_user_stream,
            params=params,
            data=data
        )

    def on_query_time(self, data, request):
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

    def on_query_account(self, data, request):
        arr = parse_account_info_arr(data, self.gateway_name)
        for account in arr:
            if account.balance:
                self.gateway.on_account(account)

        self.gateway.write_log("query account success!")

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
                self.gateway.write_log("[BINANCE new order status] data:{}".format(d))
            self.order_manager.on_order(order)

        if now_has_order_ids:
            self.query_complete_orders(now_has_order_ids)

    def on_query_order(self, data, request):
        order = parse_order_info(data, data["clientOrderId"], self.gateway_name)
        self.order_manager.on_order(order)

    def on_query_contract(self, data, request):
        for d in data["symbols"]:
            contract = parse_contract_info(d, Exchange.BINANCE.value, self.gateway_name)
            self.gateway.on_contract(contract)

        self.gateway.write_log("query contract success!")

    def on_send_order(self, data, request):
        # 会在 ws_trade_api 里面返回
        pass

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        self.order_manager.on_order(order)

        msg = "send_order failed,status:{},information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        self.order_manager.on_order(order)

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
        url = WEBSOCKET_TRADE_HOST + self.user_stream_key

        if self.gateway.ws_trade_api:
            self.gateway.ws_trade_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data, request):
        pass

