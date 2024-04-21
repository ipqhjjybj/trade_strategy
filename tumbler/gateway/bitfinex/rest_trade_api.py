# coding=utf-8

import sys
import time
import json
import base64
import hmac
import hashlib

from tumbler.api.rest import RestClient
from tumbler.function import get_vt_key, get_format_lower_symbol, get_volume_tick_from_min_volume
from tumbler.function import get_str_dt_use_timestamp

from tumbler.constant import (
    Exchange,
    Product,
    Status
)
from tumbler.object import (
    OrderData,
    AccountData,
    ContractData
)
from .base import REST_TRADE_HOST, ORDER_TYPE_BITFINEX2VT


class BitfinexRestTradeApi(RestClient):

    def __init__(self, gateway):
        """"""
        super(BitfinexRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.key = ""
        self.secret = ""

        self.url = REST_TRADE_HOST

        self.set_all_symbols = set([])

    def sign(self, request):
        """
        Generate BitfineX signature.
        """
        # Sign
        nonce = str(int(round(time.time() * 1000000)))
        payload = {}
        if request.method == "POST":
            payload = request.data

        payload["request"] = request.path
        payload["nonce"] = nonce

        j = json.dumps(payload)
        data = base64.standard_b64encode(j.encode('utf8'))

        h = hmac.new(self.secret.encode('utf8'), data, hashlib.sha384)
        signature = h.hexdigest()

        request.headers = {
            "X-BFX-APIKEY": self.key,
            "X-BFX-SIGNATURE": signature,
            "X-BFX-PAYLOAD": data
        }

        return request

    def connect(self, key, secret, url="", proxy_host="", proxy_port=0):
        """
        Initialize connection to REST server.
        """
        if url:
            self.url = url
        self.key = key
        self.secret = secret

        self.init(self.url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BitfinexRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_contract(self):
        self.add_request(
            method="GET",
            path="/v1/symbols_details",
            callback=self.on_query_contract,
        )

    def query_open_orders(self):
        self.add_request(
            method="POST",
            path="/v1/orders",
            data={},
            callback=self.on_query_open_orders
        )

    def query_account(self):
        self.add_request(
            method="POST",
            path="/v1/balances",
            data={},
            callback=self.on_query_account
        )

    def cancel_system_order(self, order_id):
        params = {"order_id": int(order_id)}
        self.add_request(
            method="POST",
            path="/v1/order/cancel",
            callback=self.on_cancel_system_order,
            data=params,
            extra=order_id
        )

    def on_query_account(self, data, request):
        for account_data in data:
            if account_data["type"] == "exchange":
                account = AccountData()
                account.account_id = account_data["currency"]
                account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
                account.balance = float(account_data["amount"])
                account.available = float(account_data["available"])
                account.frozen = account.balance - account.available

                account.gateway_name = self.gateway_name

                if account.balance:
                    self.gateway.on_account(account)

    def on_query_open_orders(self, data, request):
        for d in data:
            symbol = get_format_lower_symbol(d["symbol"])
            if symbol not in self.set_all_symbols:
                continue
            sys_order_id = str(d["cid"])

            if not self.order_manager.has_order_id(sys_order_id):
                real_id = d["id"]
                self.cancel_system_order(real_id)
                continue
            local_order_id = sys_order_id

            order = OrderData()
            order.order_id = local_order_id
            order.exchange = Exchange.BITFINEX.value
            order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
            order.symbol = symbol
            order.vt_symbol = get_vt_key(order.symbol, order.exchange)
            order.price = float(d["price"])
            order.volume = float(d["original_amount"])
            order.type = ORDER_TYPE_BITFINEX2VT[d["type"].upper()]
            order.traded = float(d["executed_amount"])

            status = Status.NOTTRADED.value
            if not d["is_live"]:
                if d["is_cancelled"]:
                    status = Status.CANCELLED.value
                else:
                    status = Status.ALLTRADED.value
            else:
                if float(d["executed_amount"]):
                    status = Status.PARTTRADED.value
                else:
                    status = Status.NOTTRADED.value

            order.status = status

            order.time = get_str_dt_use_timestamp(d["timestamp"], 1)
            order.gateway_name = self.gateway_name
            self.gateway.on_order(order)

    def on_cancel_system_order(self, data, request):
        msg = "on_cancel_system_order , sys_order_id:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_query_contract(self, data, request):
        for d in data:
            contract = ContractData()
            contract.symbol = get_format_lower_symbol(d["pair"])
            contract.name = contract.symbol.replace('_', '/')
            contract.exchange = Exchange.BITFINEX.value
            contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
            contract.price_tick = 10 ** (-1 * d["price_precision"])
            contract.size = 1
            contract.min_volume = float(d["minimum_order_size"])
            contract.volume_tick = get_volume_tick_from_min_volume(contract.min_volume)
            contract.product = Product.SPOT.value
            contract.gateway_name = self.gateway_name

            self.gateway.on_contract(contract)

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

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb, request))
