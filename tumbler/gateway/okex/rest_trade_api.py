# coding=utf-8

import time
import sys
from datetime import datetime
from threading import Lock
from requests import ConnectionError

from tumbler.api.rest import RestClient
from tumbler.function import get_vt_key, get_two_currency
from tumbler.object import OrderData

from tumbler.constant import (
    Direction,
    Exchange,
    Status,
    OrderType,
    RunMode
)
from tumbler.object import (
    AccountData
)

from .base import REST_TRADE_HOST, ORDER_TYPE_VT2OKEX, DIRECTION_VT2OKEX
from .base import parse_order_info, okex_format_symbol, parse_contract_info
from .base import okex_format_to_system_format, parse_account_data, sign_request


class OkexRestTradeApi(RestClient):
    """
    OKEX REST TRADE API
    """

    def __init__(self, gateway):
        super(OkexRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.host = ""
        self.key = ""
        self.secret = ""
        self.passphrase = ""

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))
        self.set_all_coins = set([])
        self.set_all_symbols = set([])

        self.orders = self.gateway.orders

        self.order_count_lock = Lock()
        self.order_count = 10000

        self.run_mode = RunMode.NORMAL.value

    def sign(self, request):
        return sign_request(request, self.key, self.secret, self.passphrase)

    def connect(self, key, secret, passphrase="", url=REST_TRADE_HOST, proxy_host="", proxy_port=0,
                run_mode=RunMode.NORMAL.value):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

        self.run_mode = run_mode

        self.init(url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("OkexRestTradeApi start success!")

        self.query_time()
        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, sub):
        self.set_all_symbols.add(sub.symbol)
        a_symbol, b_symbol = get_two_currency(sub.symbol)
        self.set_all_coins.add(a_symbol)
        self.set_all_coins.add(b_symbol)

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req):
        order_id = "a{}{}".format(self.connect_time, self._new_order_id())

        data = {
            "client_oid": order_id,
            "type": ORDER_TYPE_VT2OKEX[req.type],
            "side": DIRECTION_VT2OKEX[req.direction],
            "instrument_id": req.symbol
        }

        if req.type == OrderType.MARKET.value:
            if req.direction == Direction.LONG.value:
                data["notional"] = req.volume
            else:
                data["size"] = req.volume
        else:
            data["price"] = req.price
            data["size"] = req.volume

        order = req.create_order_data(order_id, self.gateway_name)
        self.gateway.on_order(order)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                "POST",
                "/api/spot/v3/orders",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_failed=self.on_send_order_failed,
                on_error=self.on_send_order_error,
            )
        else:
            self.add_request(
                "POST",
                "/api/spot/v3/orders",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_failed=self.on_send_order_failed,
                on_error=self.on_send_order_error,
            )
        return order.vt_order_id, order

    def cancel_order(self, req):
        data = {
            "instrument_id": okex_format_symbol(req.symbol),
            "client_oid": req.order_id
        }
        path = "/api/spot/v3/cancel_orders/{}".format(req.order_id)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                "POST",
                path,
                callback=self.on_cancel_order,
                data=data,
                on_error=self.on_cancel_order_error,
                on_failed=self.on_cancel_order_failed,
                extra=req
            )
        else:
            self.add_request(
                "POST",
                path,
                callback=self.on_cancel_order,
                data=data,
                on_error=self.on_cancel_order_error,
                on_failed=self.on_cancel_order_failed,
                extra=req
            )

    def cancel_system_order(self, symbol, order_id, flag=False):
        data = {
            "instrument_id": okex_format_symbol(symbol),
            "client_oid": order_id
        }

        if flag:
            data = {
                "instrument_id": okex_format_symbol(symbol),
                "order_id": order_id
            }

        path = "/api/spot/v3/cancel_orders/{}".format(order_id)
        self.add_request(
            "POST",
            path,
            callback=self.on_cancel_system_order,
            data=data,
            on_error=None,
            on_failed=None,
            extra=order_id
        )

    def query_contract(self):
        self.add_request(
            "GET",
            "/api/spot/v3/instruments",
            callback=self.on_query_contract
        )

    def query_account(self):
        self.add_request(
            "GET",
            "/api/spot/v3/accounts",
            callback=self.on_query_account
        )

    def query_open_orders(self):
        self.add_request(
            "GET",
            "/api/spot/v3/orders_pending",
            callback=self.on_query_order
        )

    def query_send_orders(self):
        now_time = time.time()
        for order_id in self.orders.keys():
            order = self.orders[order_id]
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))
            if now_time - order_time > 5:
                self.add_request(
                    "GET",
                    "/api/spot/v3/orders/%s?instrument_id=%s" % (order_id, okex_format_symbol(order.symbol)),
                    callback=self.on_query_single_order,
                    extra=(order_id, order.symbol)
                )

    def query_time(self):
        self.add_request(
            "GET",
            "/api/general/v3/time",
            callback=self.on_query_time
        )

    def on_query_contract(self, data, request):
        ret = parse_contract_info(data, self.gateway_name)
        for contract in ret:
            self.gateway.on_contract(contract)

    def on_query_account(self, data, request):
        lcs = list(self.set_all_coins)
        account_arr = parse_account_data(data, self.gateway_name)
        for account in account_arr:
            if account.account_id in lcs:
                lcs.remove(account.account_id)
            self.gateway.on_account(account)

        for account_id in lcs:
            account = AccountData()
            account.account_id = account_id
            account.vt_account_id = get_vt_key(Exchange.OKEX.value, account_id)
            account.gateway_name = Exchange.OKEX.value
            self.gateway.on_account(account)

    def work_order(self, order_data):
        order_id = order_data["client_oid"]
        symbol = okex_format_to_system_format(order_data["instrument_id"])

        if symbol not in self.set_all_symbols:
            return

        if order_id not in self.orders.keys():
            if not order_id:
                order_id = order_data["order_id"]
                self.cancel_system_order(symbol, order_id)
            else:
                self.cancel_system_order(symbol, order_id)
            return

        order = parse_order_info(order_data, order_id, self.gateway_name)
        self.gateway.on_order(order)

    def on_query_single_order(self, data, request):
        if "error_code" in data.keys():
            # {'error_message': 'Order does not exist', 'code': 33014, 'error_code': '33014',
            # 'message': 'Order does not exist'}
            code = data["code"]
            if str(code) == "33014":
                order_id, symbol = request.extra
                order = OrderData.make_reject_order(order_id, symbol, Exchange.OKEX.value, self.gateway_name)
                self.gateway.on_order(order)
            else:
                self.gateway.write_log("Unknown error data:{}".format(data))
        else:
            self.work_order(data)

    def on_query_order(self, data, request):
        for order_data in data:
            self.work_order(order_data)

    def on_query_time(self, data, request):
        server_time = data["iso"]
        local_time = datetime.utcnow().isoformat()
        msg = "server_time:{}，local_machine:{}".format(server_time, local_time)
        self.gateway.write_log(msg)

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        if str(status_code) not in ["504"]:
            self.gateway.on_order(order)

        msg = "order failed, status:{} information:{}".format(status_code, request.response.text)
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

    def on_send_order(self, data, request):
        """Websocket will push a new order status"""
        order = request.extra

        error_msg = data["error_message"]
        if error_msg:
            self.gateway.write_log(error_msg)
            order.status = Status.REJECTED.value
            self.gateway.on_order(order)

    def on_cancel_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when cancelling order failed on server.
        """
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_system_order(self, data, request):
        """撤掉非系统发的单子"""
        msg = "cancel system_order:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_cancel_order(self, data, request):
        """Websocket will push a new order status"""
        pass

    def on_cancel_order_failed(self, status_code, request):
        """If cancel failed, mark order status to be rejected."""
        req = request.extra
        order = self.gateway.get_order(req.order_id)
        if order:
            order.status = Status.REJECTED.value
            self.gateway.on_order(order)

    def on_failed(self, status_code, request):
        """
        Callback to handle request failed.
        """
        msg = "request failed status:{} information:{} request:{}".format(status_code, request.response.text,
                                                                          request.__dict__)
        self.gateway.write_log(msg)

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Callback to handler request exception.
        """
        msg = "touch error, status:{} information:{}".format(exception_type, exception_value)
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb, request))
