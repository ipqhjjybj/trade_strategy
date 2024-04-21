# coding=utf-8

import sys
from copy import copy
from datetime import datetime
from threading import Lock
from requests import ConnectionError

from tumbler.function import get_vt_key
from tumbler.api.rest import RestClient

from tumbler.constant import (
    Direction,
    Exchange,
    OrderType,
    Status
)

from tumbler.object import (
    PositionData
)
from tumbler.gateway.okex.base import sign_request

from .base import REST_TRADE_HOST, TYPE_VT2OKEXS, okexs_format_symbol
from .base import parse_order_info, parse_contract_info
from .base import parse_account_info, parse_position_holding, okexs_format_to_system_format


class OkexsRestTradeApi(RestClient):
    """
    OKEX REST TRADE API
    """

    def __init__(self, gateway):
        super(OkexsRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.host = ""
        self.key = ""
        self.secret = ""
        self.passphrase = ""

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))
        self.set_all_symbols = set([])

        self.orders = self.gateway.orders

        self.order_count_lock = Lock()
        self.order_count = 10000

    def sign(self, request):
        return sign_request(request, self.key, self.secret, self.passphrase)

    def connect(self, key, secret, passphrase="", url=REST_TRADE_HOST, proxy_host="", proxy_port=0):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

        self.init(url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("OkexsRestTradeApi start success!")

        self.query_time()
        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, sub):
        self.set_all_symbols.add(sub.symbol)
        self.query_position()

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req):
        if (req.offset, req.direction) not in TYPE_VT2OKEXS:
            return None

        order_id = "a{}{}".format(self.connect_time, self._new_order_id())

        data = {
            "client_oid": order_id,
            "type": TYPE_VT2OKEXS[(req.offset, req.direction)],
            "instrument_id": okexs_format_symbol(req.symbol),
            "price": str(req.price),
            "size": str(int(req.volume))
        }

        if req.type == OrderType.MARKET.value:
            data["match_price"] = "1"
        else:
            data["match_price"] = "0"

        order = req.create_order_data(order_id, self.gateway_name)
        self.add_request(
            "POST",
            "/api/swap/v3/order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        self.gateway.on_order(order)
        return order.vt_order_id

    def cancel_order(self, req):
        path = "/api/swap/v3/cancel_order/{}/{}".format(okexs_format_symbol(req.symbol), req.order_id)
        self.add_request(
            "POST",
            path,
            callback=self.on_cancel_order,
            on_error=self.on_cancel_order_error,
            on_failed=self.on_cancel_order_failed,
            extra=req
        )

    def cancel_system_order(self, symbol, order_id):
        path = "/api/swap/v3/cancel_order/{}/{}".format(okexs_format_symbol(symbol), order_id)
        self.add_request(
            "POST",
            path,
            callback=self.on_cancel_system_order,
            on_error=None,
            on_failed=None,
            extra=order_id
        )

    def query_contract(self):
        self.add_request(
            "GET",
            "/api/swap/v3/instruments",
            callback=self.on_query_contract
        )

    def query_account(self):
        self.add_request(
            "GET",
            "/api/swap/v3/accounts",
            callback=self.on_query_account
        )

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            # get waiting orders
            self.add_request(
                "GET",
                "/api/swap/v3/orders/%s?status=0" % (okexs_format_symbol(symbol)),
                callback=self.on_query_order
            )
            # get part traded orders
            self.add_request(
                "GET",
                "/api/swap/v3/orders/%s?status=1" % (okexs_format_symbol(symbol)),
                callback=self.on_query_order
            )

        orders = self.gateway.get_active_orders()
        t_items = copy(list(orders.items()))
        for vt_order_it, order in t_items:
            if order.is_active():
                self.add_request(
                    "GET",
                    "/api/swap/v3/orders/%s/%s" % (okexs_format_symbol(order.symbol), order.order_id),
                    callback=self.on_query_order
                )

    def query_position(self):
        self.add_request(
            "GET",
            "/api/swap/v3/position",
            callback=self.on_query_position
        )

    def query_time(self):
        self.add_request(
            "GET",
            "/api/general/v3/time",
            callback=self.on_query_time
        )

    def on_query_contract(self, data, request):
        contract_arr = parse_contract_info(data, self.gateway_name)
        for contract in contract_arr:
            self.gateway.on_contract(contract)

    def on_query_account(self, data, request):
        for info in data["info"]:
            account = parse_account_info(info, gateway_name=self.gateway_name)
            self.gateway.on_account(account)

    def on_query_position(self, datas, request):
        all_position_sets = set([])
        for symbol in self.set_all_symbols:
            for direction in [Direction.LONG.value, Direction.SHORT.value]:
                all_position_sets.add((symbol, direction))

        for data in datas:
            holdings = data["holding"]

            for holding in holdings:
                symbol = holding["instrument_id"].lower()
                pos = parse_position_holding(holding, symbol=symbol, gateway_name=self.gateway_name)

                self.gateway.on_position(pos)
                if (pos.symbol, pos.direction) in all_position_sets:
                    all_position_sets.remove((pos.symbol, pos.direction))

        for symbol, direction in all_position_sets:
            new_pos = PositionData()
            new_pos.symbol = symbol
            new_pos.exchange = Exchange.OKEXS.value
            new_pos.vt_symbol = get_vt_key(new_pos.symbol, new_pos.exchange)
            new_pos.direction = direction
            new_pos.position = 0
            new_pos.vt_position_id = get_vt_key(new_pos.vt_symbol, new_pos.direction)
            self.gateway.on_position(new_pos)

    def on_query_order(self, data, request):
        if "order_info" in data.keys():
            for order_data in data["order_info"]:
                order_id = order_data["client_oid"]
                symbol = okexs_format_to_system_format(order_data["instrument_id"])

                if symbol not in self.set_all_symbols:
                    continue

                if order_id not in self.orders.keys():
                    if not order_id:
                        order_id = order_data["order_id"]
                        self.cancel_system_order(symbol, order_id)
                    else:
                        self.cancel_system_order(symbol, order_id)
                    continue

                order = parse_order_info(order_data, gateway_name=self.gateway_name)
                self.gateway.on_order(order)
        else:
            order = parse_order_info(data, gateway_name=self.gateway_name)
            self.gateway.on_order(order)

    def on_query_time(self, data, request):
        server_time = data["iso"]
        local_time = datetime.utcnow().isoformat("T")
        msg = "server_time:{},local_machine:{}".format(server_time, local_time)
        self.gateway.write_log(msg)

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED.value
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
        print(data)
        pass

    def on_cancel_order_failed(self, status_code, request):
        """If cancel failed, mark order status to be rejected."""
        pass

    def on_failed(self, status_code, request):
        """
        Callback to handle request failed.
        """
        msg = "request failed status:{} information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Callback to handler request exception.
        """
        msg = "touch error, status:{} information:{}".format(exception_type, exception_value)
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )
