# coding=utf-8

import sys

from tumbler.api.rest import RestClient

from tumbler.constant import (
    Direction,
    Status
)

from tumbler.function import split_url, get_str_dt_use_timestamp
from .base import parse_contract_info_arr, parse_order_info, parse_account_info_arr
from .base import create_rest_signature, REST_TRADE_HOST, change_system_format_to_gateio_format
from .base import change_gateio_format_to_system_format, asset_from_gateio_to_other_exchanges


class GateioRestTradeApi(RestClient):
    """
    GATEIO REST API
    """

    def __init__(self, gateway):
        super(GateioRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.host = ""
        self.key = ""
        self.secret = ""

        self.set_all_symbols = set([])

    def sign(self, request):
        request.headers = {
            "Accept": "application/json",
            'Content-Type': 'application/x-www-form-urlencoded',
            "User-Agent": "Chrome/39.0.2171.71",
            "KEY": self.key,
            "SIGN": create_rest_signature(request.data, self.secret)
        }
        return request

    def connect(self, key, secret, proxy_host="", proxy_port=0):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret

        self.host, _ = split_url(REST_TRADE_HOST)

        self.init(REST_TRADE_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("GateioRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_account(self):
        self.add_request(
            method="POST",
            data={},
            path="/api2/1/private/balances",
            callback=self.on_query_account
        )

    def query_open_orders(self):
        self.add_request(
            method="POST",
            data={},
            path="/api2/1/private/openOrders",
            callback=self.on_query_open_orders
        )

    def query_contract(self):
        self.add_request(
            method="GET",
            data={},
            path="/api2/1/marketinfo",
            callback=self.on_query_contract
        )

    def send_order(self, req):
        local_order_id = self.order_manager.new_local_order_id()
        order = req.create_order_data(
            local_order_id,
            self.gateway_name
        )
        self.set_all_symbols.add(order.symbol)

        data = {
            "currencyPair": change_system_format_to_gateio_format(req.symbol),
            "rate": req.price,
            "amount": req.volume
        }

        url_path = ""
        if req.direction == Direction.LONG.value:
            url_path = "/api2/1/private/buy"
        elif req.direction == Direction.SHORT.value:
            url_path = "/api2/1/private/sell"
        else:
            return None

        self.add_request(
            method="POST",
            path=url_path,
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )
        self.order_manager.on_order(order)
        return order.vt_order_id

    def cancel_order(self, req):
        sys_order_id = self.order_manager.get_sys_order_id(req.order_id)

        data = {
            "currencyPair": change_system_format_to_gateio_format(req.symbol),
            "orderNumber": sys_order_id
        }

        path = "/api2/1/private/cancelOrder"
        self.add_request(
            method="POST",
            path=path,
            data=data,
            callback=self.on_cancel_order,
            extra=req
        )

    def cancel_system_order(self, sys_order_id, symbol):
        data = {
            "currencyPair": change_system_format_to_gateio_format(symbol),
            "orderNumber": sys_order_id
        }

        path = "/api2/1/private/cancelOrder"
        self.add_request(
            method="POST",
            path=path,
            data=data,
            callback=self.on_cancel_system_order,
            extra=sys_order_id
        )

    def on_query_account(self, data, request):
        if self.check_error(data, "query_account"):
            return

        ret_account = parse_account_info_arr(data, self.gateway_name)
        for account in ret_account:
            if account.balance:
                self.gateway.on_account(account)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_open_order"):
            return

        list_order_ids = list(self.order_manager.get_all_alive_system_id())
        for d in data["orders"]:
            symbol = change_gateio_format_to_system_format(d["currencyPair"])

            if symbol not in self.set_all_symbols:
                continue
            sys_order_id = str(d["orderNumber"])

            if not self.order_manager.has_system_order(sys_order_id):
                self.cancel_system_order(sys_order_id, symbol)
                continue

            if sys_order_id in list_order_ids:
                list_order_ids.remove(sys_order_id)

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            order = parse_order_info(d, symbol, local_order_id, self.gateway_name, _type="query_order")
            self.order_manager.on_order(order)

        for sys_order_id in list_order_ids:
            order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
            if order:
                data = {
                    "orderNumber": str(sys_order_id),
                    "currencyPair": str(order.symbol)
                }
                self.add_request(
                    method="POST",
                    path="/api2/1/private/getOrder",
                    data=data,
                    callback=self.on_query_order
                )

    def on_query_order(self, data, request):
        if self.check_error(data, "query_order"):
            return

        d = data["order"]
        symbol = change_gateio_format_to_system_format(d["currencyPair"])
        if symbol not in self.set_all_symbols:
            return

        sys_order_id = str(d["orderNumber"])

        if not self.order_manager.has_system_order(sys_order_id):
            self.cancel_system_order(sys_order_id, symbol)
            return

        local_order_id = self.order_manager.get_local_order_id(sys_order_id)
        order = parse_order_info(d, symbol, local_order_id, self.gateway_name, _type="query_order")

        self.order_manager.on_order(order)

    def on_query_contract(self, data, request):
        if self.check_error(data, "query_contract"):
            return

        contracts = parse_contract_info_arr(data, self.gateway_name)
        for contract in contracts:
            self.gateway.on_contract(contract)

        self.gateway.write_log("query contract success!")

    def on_send_order(self, data, request):
        order = request.extra

        if self.check_error(data, "send_order"):
            order.status = Status.REJECTED.value
            self.order_manager.on_order(order)
            return

        sys_order_id = str(data["orderNumber"])
        self.order_manager.update_order_id_map(order.order_id, sys_order_id)

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
        cancel_request = request.extra
        local_order_id = cancel_request.order_id
        order = self.order_manager.get_order_with_local_order_id(local_order_id)

        if self.check_error(data, "cancel_order"):
            self.gateway.write_log("cancel_order failed!{}".format(str(order.order_id)))
            return 
        else:
            order.status = Status.CANCELLED.value
            self.gateway.write_log("cancel_order success!{}".format(str(order.order_id)))

        self.order_manager.on_order(order)

    def on_cancel_system_order(self, data, request):
        msg = "on_cancel_system_order , sys_order_id:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Callback to handler request exception.
        """
        msg = "on_error,status_code:{},information:{}".format(str(exception_type), str(exception_value))
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb, request))

    def check_error(self, data, func=""):
        result = data.get("result", None)
        if result == "true" or result is True or result == "True" or result == "TRUE":
            return False

        error_code = "g1"
        error_msg = str(data)

        self.gateway.write_log(
            "{} request_error, status_code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True
