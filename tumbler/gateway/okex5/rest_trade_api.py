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

from tumbler.gateway.okex.base import sign_request
from .base import okex5_format_symbol, okex5_format_to_system_format
from .base import REST_TRADE_HOST
from .base import parse_account_data
from .base import parse_order_info, parse_contract_info, parse_position_data


class Okex5RestTradeApi(RestClient):
    """
    OKEX5 REST TRADE API
    """

    def __init__(self, gateway):
        super(Okex5RestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.host = ""
        self.key = ""
        self.secret = ""
        self.passphrase = ""
        self.mode_type = ""
        self.ord_type = ""

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))
        self.set_all_coins = set([])
        self.set_all_symbols = set([])

        self.orders = self.gateway.orders

        self.order_count_lock = Lock()
        self.order_count = 10000

        self.run_mode = RunMode.NORMAL.value

    def sign(self, request):
        return sign_request(request, self.key, self.secret, self.passphrase)

    def connect(self, key, secret, passphrase="", url=REST_TRADE_HOST, mode_type="", ord_type="",
                proxy_host="", proxy_port=0, run_mode=RunMode.NORMAL.value):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.mode_type = mode_type
        self.ord_type = ord_type

        self.run_mode = run_mode

        self.init(url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("OkexRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_position()
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

    def format_req(self, req):
        order_id = "a{}{}".format(self.connect_time, self._new_order_id())

        if req.direction == Direction.LONG.value:
            side = "buy"
        else:
            side = "sell"

        if abs(req.volume - int(req.volume)) < 1e-8:
            sz = str(int(req.volume))
        else:
            sz = str(req.volume)

        data = {
            "instId": okex5_format_symbol(req.symbol),
            "clOrdId": order_id,
            "tdMode": self.mode_type,
            "side": side,
            "ordType": self.ord_type.lower(),
            "px": str(req.price),
            "sz": sz
        }

        order = req.create_order_data(order_id, Exchange.OKEX5.value)
        return data, order

    def send_order(self, req):
        data, order = self.format_req(req)

        self.set_all_symbols.add(req.symbol)

        self.gateway.on_order(order)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                "POST",
                "/api/v5/trade/order",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_failed=self.on_send_order_failed,
                on_error=self.on_send_order_error,
            )
        else:
            self.add_request(
                "POST",
                "/api/v5/trade/order",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_failed=self.on_send_order_failed,
                on_error=self.on_send_order_error,
            )
        return order.vt_order_id, order

    def send_orders(self, reqs):
        rets = []
        data_rets = []
        orders = []

        for req in reqs:
            data, order = self.format_req(req)
            data_rets.append(data)
            rets.append((order.vt_order_id, order))
            orders.append(order)

        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                method="POST",
                path="/api/v5/trade/batch-orders",
                callback=self.on_send_orders,
                data=data_rets,
                extra=orders,
                on_error=self.on_send_orders_error,
                on_failed=self.on_send_orders_failed
            )
        else:
            self.add_request(
                method="POST",
                path="/api/v5/trade/batch-orders",
                callback=self.on_send_orders,
                data=data_rets,
                extra=orders,
                on_error=self.on_send_orders_error,
                on_failed=self.on_send_orders_failed
            )

        for order in orders:
            self.gateway.on_order(order)
        return rets

    def cancel_order(self, req):
        data = {
            "clOrdId": str(req.order_id),
            "instId": okex5_format_symbol(req.symbol)
        }
        path = "/api/v5/trade/cancel-order"
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

    def cancel_orders(self, reqs):
        if reqs:
            data = [
                {
                    "instId": okex5_format_symbol(req.symbol),
                    "clOrdId": str(req.order_id)
                } for req in reqs
            ]

            path = "/api/v5/trade/cancel-batch-orders"
            self.add_request(
                method="POST",
                path=path,
                data=data,
                callback=self.on_cancel_orders,
                extra=reqs
            )

    def cancel_system_order(self, symbol, order_id, use_order_id=False):
        if use_order_id:
            data = {
                "ordId": str(order_id),
                "instId": okex5_format_symbol(symbol)
            }
        else:
            data = {
                "clOrdId": str(order_id),
                "instId": okex5_format_symbol(symbol)
            }

        path = "/api/v5/trade/cancel-order"
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
        for _type in ["SPOT", "SWAP", "FUTURES"]:
            url = "/api/v5/public/instruments?instType={}".format(_type)
            if _type == "OPTION":
                for uly in ["BTC_USD"]:
                    tmp_url = url + "&uly={}".format(uly)
                    self.add_request(
                        "GET",
                        tmp_url,
                        callback=self.on_query_contract,
                        extra=_type,
                    )
            else:
                self.add_request(
                    "GET",
                    url,
                    callback=self.on_query_contract,
                    extra=_type,
                )

    def query_account(self):
        self.add_request(
            "GET",
            "/api/v5/account/balance",
            callback=self.on_query_account
        )

    def query_open_orders(self):
        self.add_request(
            "GET",
            "/api/v5/trade/orders-pending",
            params={},
            callback=self.on_query_order
        )

    def query_send_orders(self):
        for order_id in self.orders.keys():
            order = self.orders[order_id]
            data = {
                "instId": okex5_format_symbol(order.symbol),
                "clOrdId": str(order_id),
            }
            self.add_request(
                "GET",
                "/api/v5/trade/order",
                params=data,
                callback=self.on_query_single_order,
                extra=order
            )

    def query_position(self):
        self.add_request(
            "GET",
            "/api/v5/account/positions",
            callback=self.on_query_position
        )

    def on_query_position(self, data, request):
        position_list = parse_position_data(data["data"], self.gateway_name)
        for position in position_list:
            self.gateway.on_position(position)

    def on_query_contract(self, data, request):
        _type = request.extra
        ret = parse_contract_info(data["data"], self.gateway_name, _type)
        for contract in ret:
            self.gateway.on_contract(contract)

    def on_query_account(self, data, request):
        lcs = list(self.set_all_coins)
        account_arr = parse_account_data(data["data"], self.gateway_name)
        for account in account_arr:
            if account.account_id in lcs:
                lcs.remove(account.account_id)
            self.gateway.on_account(account)

        for account_id in lcs:
            account = AccountData()
            account.account_id = account_id
            account.vt_account_id = get_vt_key(Exchange.OKEX5.value, account_id)
            account.gateway_name = Exchange.OKEX5.value
            self.gateway.on_account(account)

    def work_order(self, order_data):
        order_id = order_data["clOrdId"]
        symbol = okex5_format_to_system_format(order_data["instId"])
        if symbol not in self.set_all_symbols:
            return

        if order_id not in self.orders.keys():
            if not order_id:
                order_id = order_data["order_id"]
                self.cancel_system_order(symbol, order_id, use_order_id=True)
            else:
                self.cancel_system_order(symbol, order_id, use_order_id=False)
            return

        order = parse_order_info(order_data, order_id, self.gateway_name)
        self.gateway.on_order(order)

    def on_query_single_order(self, data, request):
        code = int(data["code"])
        if code == 0:
            for order_data in data["data"]:
                self.work_order(order_data)
        else:
            self.gateway.write_log("on_query_single_order code:{} msg:{} data:{}"
                                   .format(data, data["code"], data["msg"]))

            order = request.extra
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))
            if time.time() - order_time > 3600:
                order.status = Status.REJECTED.value
                self.gateway.write_log(f"on_query_single_order {time.time()} {order_time} rejected order {order.__dict__}")
                self.gateway.on_order(order)

    def on_query_order(self, data, request):
        for order_data in data["data"]:
            self.work_order(order_data)

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        msg = "order failed, status:{} information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_orders_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        msg = "send_orders failed,status:{},information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending order caused exception.
        """
        self.gateway.write_log("[on_send_order_error]")
        order = request.extra
        order.status = Status.REJECTED.value
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_orders_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending orders caused exception.
        """
        self.gateway.write_log(
            "on_send_orders_error exception_type:{} exception_value:{}".format(exception_type, exception_value))

        orders = request.extra
        for order in orders:
            order.status = Status.REJECTED.value
            self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_order(self, data, request):
        """
        Websocket will push a new order status

        {'code': '1', 'data': [{'clOrdId': 'a22062912563210030', 'ordId': '', 'sCode': '51008', 'sMsg': 'Order placement failed due to insufficient balance ', 'tag': ''}], 'msg': 'Operation failed.'}
        """
        order = request.extra

        if int(data["code"]) not in [0, 50004]:
            # 0 -> all right
            self.gateway.write_log(data)
            order.status = Status.REJECTED.value
            self.gateway.on_order(order)

    def on_send_orders(self, data, request):
        orders = request.extra
        if int(data["code"]) not in [0, 50004]:
            self.gateway.write_log("[on_send_orders] failed data:{}".format(data))
            for order in orders:
                order.status = Status.REJECTED.value
                self.gateway.on_order(order)
            return

        for dic in data["data"]:
            for order in orders:
                if str(order.order_id) == str(dic["clOrdId"]):
                    if int(dic["sCode"]) != 0:
                        order.status = Status.REJECTED.value
                        self.gateway.on_order(order)
                        self.gateway.write_log("[on_send_orders] failed single order:{}".format(dic))
                    break

    def on_cancel_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when cancelling order failed on server.
        """
        self.gateway.write_log("[on_cancel_order_error] exception_type:{} exception_value:{}"
                               .format(exception_type, exception_value))
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_system_order(self, data, request):
        """撤掉非系统发的单子"""
        msg = "cancel system_order:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_cancel_order(self, data, request):
        """Websocket will push a new order status"""
        self.gateway.write_log("[on_cancel_order] data:{}".format(data))

    def on_cancel_orders(self, data, request):
        if int(data["code"]) != 0:
            self.gateway.write_log("cancel_orders failed! data:{}".format(data))
            return
        else:
            for dic in data["data"]:
                if int(dic["sCode"]) != 0:
                    self.gateway.write_log("[on_cancel_orders] dic:{} failed!".format(dic))

    def on_cancel_order_failed(self, status_code, request):
        """If cancel failed, mark order status to be rejected."""
        req = request.extra
        order = self.gateway.get_order(req.order_id)
        if order:
            self.gateway.write_log(f"[on_cancel_order_failed] order:{order.__dict__}")

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
