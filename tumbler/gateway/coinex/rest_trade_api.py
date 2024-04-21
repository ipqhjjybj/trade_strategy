# coding=utf-8

import time
from datetime import datetime
import json
import sys

from tumbler.object import ContractData, AccountData, OrderData
from tumbler.constant import Product, Status, OrderType
from tumbler.constant import (
    Exchange,
    Direction
)

from tumbler.function import get_no_under_lower_symbol, get_vt_key, get_format_lower_symbol
from tumbler.function import get_str_dt_use_timestamp
from tumbler.api.rest import RestClient
from tumbler.function import split_url
from .base import create_signature, REST_TRADE_HOST


class CoinexRestTradeApi(RestClient):

    def __init__(self, gateway):
        super(CoinexRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.host = ""
        self.key = ""
        self.secret = ""

        self.set_all_symbols = set([])

    def sign(self, request):
        params = {}
        if request.method in ["GET", "DELETE"]:
            request.params["tonce"] = int(time.time() * 1000)
            request.params["access_id"] = self.key

            params = request.params
        else:
            request.data["tonce"] = int(time.time() * 1000)
            request.data["access_id"] = self.key
            params = request.data

            request.data = json.dumps(request.data)

        request.headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
        }
        request.headers["AUTHORIZATION"] = create_signature(self.secret, params)
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

        self.gateway.write_log("CoinexRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_account(self):
        self.add_request(
            method="GET",
            params={},
            path="/v1/balance/",
            callback=self.on_query_account
        )

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                method="GET",
                params={"market": get_no_under_lower_symbol(symbol).upper()},
                path="/v1/order/pending",
                callback=self.on_query_open_orders
            )

    def query_contract(self):
        self.add_request(
            method="GET",
            params={},
            path="/v1/market/info",
            callback=self.on_query_contract
        )

    def send_order(self, req):
        local_order_id = self.order_manager.new_local_order_id()
        order = req.create_order_data(
            local_order_id,
            self.gateway_name
        )

        direction = "sell"
        if req.direction == Direction.LONG.value:
            direction = "buy"

        data = {
            "market": get_no_under_lower_symbol(req.symbol).upper(),
            "price": req.price,
            "amount": req.volume,
            "type": direction
        }

        self.add_request(
            method="POST",
            path="/v1/order/limit",
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
            "market": get_no_under_lower_symbol(req.symbol).upper(),
            "id": sys_order_id
        }
        self.add_request(
            method="DELETE",
            path="/v1/order/pending",
            params=data,
            callback=self.on_cancel_order,
            extra=req
        )

    def cancel_system_order(self, sys_order_id, symbol):
        data = {
            "market": get_no_under_lower_symbol(symbol),
            "id": sys_order_id
        }
        self.add_request(
            method="DELETE",
            path="/v1/order/pending",
            params=data,
            callback=self.on_cancel_system_order,
            extra=sys_order_id
        )

    def on_query_account(self, data, request):
        if self.check_error(data, "query_account"):
            return

        data = data.get("data", {})
        for asset, items in data.items():
            asset = asset.lower()
            account = AccountData()
            account.account_id = asset
            account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
            account.balance = float(items["available"]) + float(items["frozen"])
            account.frozen = float(items["frozen"])
            account.available = float(items["available"])
            account.gateway_name = self.gateway_name

            if account.balance:
                self.gateway.on_account(account)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_order"):
            return

        data = data["data"]
        orders = data["data"]

        for d in orders:
            symbol = get_format_lower_symbol(d["market"])

            if symbol not in self.set_all_symbols:
                continue

            sys_order_id = str(d["id"])

            if not self.order_manager.has_system_order(sys_order_id):
                self.cancel_system_order(sys_order_id, symbol)
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)

            direction = Direction.LONG.value
            if d["type"] == "sell":
                direction = Direction.SHORT.value
            order_type = OrderType.LIMIT.value
            if d["order_type"] is "market":
                order_type = OrderType.MARKET.value

            order_time = get_str_dt_use_timestamp(d["create_time"], mill=1)
            status = d["status"]

            order = OrderData()
            order.order_id = local_order_id
            order.exchange = Exchange.COINEX.value
            order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
            order.symbol = symbol
            order.vt_symbol = get_vt_key(order.symbol, order.exchange)
            order.price = float(d["price"])
            order.volume = float(d["amount"])
            order.type = order_type
            order.direction = direction
            order.traded = float(d["deal_amount"])
            order.status = Status.NOTTRADED.value
            if status in ["done"]:
                if order.traded < order.volume:
                    order.status = Status.CANCELLED.value
                else:
                    order.status = Status.ALLTRADED.value
            elif status in ["cancel"]:
                order.status = Status.CANCELLED.value
            elif status in ["part_deal"]:
                order.status = Status.PARTTRADED.value
            elif status in ["not_deal"]:
                order.status = Status.NOTTRADED.value
            else:
                self.gateway.write_log(
                    " Exchange :{}, new_status:{}, data:{}".format(Exchange.COINEX.value, order.status, d))

            order.order_time = order_time
            order.gateway_name = self.gateway_name

            if order.status == Status.CANCELLED.value:
                order.cancel_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.order_manager.on_order(order)

    def on_query_contract(self, data, request):
        if self.check_error(data, "query_contract"):
            return
        pairs = data.get("data", None)
        for symbol, dic in pairs.items():
            dic = pairs[symbol]
            symbol = get_format_lower_symbol(symbol)
            contract = ContractData()
            contract.symbol = symbol
            contract.name = symbol.replace('_', '/')
            contract.exchange = Exchange.COINEX.value
            contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
            contract.price_tick = 10 ** (-1 * int(dic["pricing_decimal"]))
            contract.size = 1
            contract.min_volume = float(dic["min_amount"])
            contract.volume_tick = 10 ** (-1 * int(dic["pricing_decimal"]))
            contract.product = Product.SPOT.value
            contract.gateway_name = self.gateway_name

            self.gateway.on_contract(contract)

    def on_send_order(self, data, request):
        order = request.extra

        if self.check_error(data, "send_order"):
            order.status = Status.REJECTED.value
            self.order_manager.on_order(order)
            return

        sys_order_id = str(data["data"]["id"])
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
        result = data.get("code", None)
        if result is not None and str(result) == "0":
            return False

        error_code = "c1"
        error_msg = str(data)

        self.gateway.write_log(
            "{} request_error, status_code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True
