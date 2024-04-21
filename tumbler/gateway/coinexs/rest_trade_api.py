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


class CoinexsRestTradeApi(RestClient):

    def __init__(self, gateway):
        super(CoinexsRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.host = ""
        self.key = ""
        self.secret = ""

        self.set_all_symbols = set([])

    def sign(self, request):
        params = {}
        if request.method in ["GET"]:
            request.params["timestamp"] = int(time.time() * 1000)
            params = request.params
        else:
            request.data["timestamp"] = int(time.time() * 1000)
            params = request.data

        request.headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
        }
        request.headers["Authorization"] = create_signature(self.secret, params)
        request.headers["AccessId"] = self.key
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

        self.gateway.write_log("CoinexsRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_account(self):
        self.add_request(
            method="GET",
            params={},
            path="/v1/asset/query",
            callback=self.on_query_account
        )

    def query_positions(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                method="GET",
                params={"market":symbol},
                path="/v1/position/pending",
                callback=self.on_query_positions
            )

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                method="GET",
                params={"market": get_no_under_lower_symbol(symbol).upper(), "side":0, "offset":0, "limit":100},
                path="/v1/order/pending",
                callback=self.on_query_open_orders
            )

    def query_order(self, symbol, order_id):
        self.add_request(
            method="GET",
            params={"market":get_no_under_lower_symbol(symbol).upper(), "order_id":int(order_id)},
            path="/v1/order/status",
            callback=self.on_query_order
        )


    def query_finished_orders(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                method="GET",
                params={"market": get_no_under_lower_symbol(symbol).upper(), "side":2, "offset":0, "limit":100},
                path="/v1/order/finished",
                callback=self.on_query_finished_orders
            )

    def query_contract(self):
        self.add_request(
            method="GET",
            params={},
            path="/v1/market/list",
            callback=self.on_query_contract
        )

    def send_order(self, req):
        local_order_id = self.order_manager.new_local_order_id()
        order = req.create_order_data(
            local_order_id,
            self.gateway_name
        )

        side = 1
        if req.direction == Direction.LONG.value:
            side = 2

        data = {
            "market": get_no_under_lower_symbol(req.symbol).upper(),
            "price": req.price,
            "amount": req.volume,
            "side": side,
            "effect_type": 1,
            "option": 1         # 1 means only maker
        }

        self.add_request(
            method="POST",
            path="/v1/order/put_limit",
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
        if sys_order_id is None or str(sys_order_id) == "None":
            return

        data = {
            "market": get_no_under_lower_symbol(req.symbol).upper(),
            "order_id": int(sys_order_id),
        }
        self.add_request(
            method="POST",
            path="/v1/order/cancel",
            data=data,
            callback=self.on_cancel_order,
            extra=req
        )

    def cancel_system_order(self, sys_order_id, symbol):
        data = {
            "market": get_no_under_lower_symbol(symbol).upper(),
            "order_id": int(sys_order_id)
        }
        self.add_request(
            method="POST",
            path="/v1/order/cancel",
            data=data,
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
            account.account_id = asset.lower()
            account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
            account.balance = float(items["balance_total"])
            account.available = float(items["available"])
            account.frozen = float(items["frozen"])
            account.gateway_name = self.gateway_name

            if account.balance:
                self.gateway.on_account(account)

    def on_query_positions(self, data, request):
        if self.check_error(data, "query_position"):
            return

        data = data.get("data", [])
        for posinfo in data:
            pos = PositionData()
            pos.symbol = get_format_lower_symbol(posinfo["market"])
            pos.exchange = Exchange.COINEXS.value
            pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)

            xishu = 1
            if int(posinfo["side"]) == 1:
                #pos.direction = Direction.SHORT.value
                xishu = -1
            else:
                #pos.direction = Direction.LONG.value
                xishu = 1
            pos.position = float(posinfo["amount"]) * xishu
            pos.frozen = (float(posinfo["amount"]) - float(posinfo["close_left"])) * xishu
            pos.price = float(posinfo["open_price"])
            pos.vt_position_id = get_vt_key(pos.vt_symbol, pos.direction)
            pos.gateway_name = self.gateway_name

            self.gateway.on_position(pos)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_open_orders"):
            return

        data = data["data"]
        orders = data["records"]

        for d in orders:
            symbol = get_format_lower_symbol(d["market"])

            if symbol not in self.set_all_symbols:
                continue

            sys_order_id = str(d["order_id"])

            if not self.order_manager.has_system_order(sys_order_id):
                self.cancel_system_order(sys_order_id, symbol)
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            direction = Direction.LONG.value
            if int(d["side"]) == 1:
                direction = Direction.SHORT.value

            order_time = get_str_dt_use_timestamp(d["create_time"], mill=1)
            update_time = get_str_dt_use_timestamp(d["update_time"], mill=1)
            order_type = OrderType.LIMIT.value

            order = OrderData()
            order.order_id = local_order_id
            order.exchange = Exchange.COINEXS.value
            order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
            order.symbol = symbol
            order.vt_symbol = get_vt_key(order.symbol, order.exchange)
            order.price = float(d["price"])
            order.volume = float(d["amount"])
            order.type = order_type
            order.direction = direction
            order.traded = float(d["amount"]) - float(d["left"])

            if order.traded > 1e-6:
                order.status = Status.PARTTRADED.value
            else:
                order.status = Status.NOTTRADED.value

            order.order_time = order_time
            order.gateway_name = self.gateway_name

            self.order_manager.on_order(order)

    def on_query_finished_orders(self, data, request):
        if self.check_error(data, "query_finished_orders"):
            return

        data = data["data"]
        orders = data["records"]

        for d in orders:
            symbol = get_format_lower_symbol(d["market"])

            if symbol not in self.set_all_symbols:
                continue

            sys_order_id = str(d["order_id"])

            if not self.order_manager.has_system_order(sys_order_id):
                self.cancel_system_order(sys_order_id, symbol)
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            direction = Direction.LONG.value
            if int(d["side"]) == 1:
                direction = Direction.SHORT.value

            order_time = get_str_dt_use_timestamp(d["create_time"], mill=1)
            update_time = get_str_dt_use_timestamp(d["update_time"], mill=1)
            order_type = OrderType.LIMIT.value

            order = OrderData()
            order.order_id = local_order_id
            order.exchange = Exchange.COINEXS.value
            order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
            order.symbol = symbol
            order.vt_symbol = get_vt_key(order.symbol, order.exchange)
            order.price = float(d["price"])
            order.volume = float(d["amount"])
            order.type = order_type
            order.direction = direction
            order.traded = float(d["amount"]) - float(d["left"])

            if float(d["left"]) > 1e-8:
                order.status = Status.CANCELLED.value
            else:
                order.status = Status.ALLTRADED.value

            order.order_time = order_time
            order.gateway_name = self.gateway_name

            self.order_manager.on_order(order)

    def on_query_order(self, data, request):
        if self.check_error(data, "query_order"):
            return

        d = data["data"]
        symbol = get_format_lower_symbol(d["market"])

        if symbol not in self.set_all_symbols:
            return

        sys_order_id = str(d["order_id"])

        if not self.order_manager.has_system_order(sys_order_id):
            self.cancel_system_order(sys_order_id, symbol)
            return

        local_order_id = self.order_manager.get_local_order_id(sys_order_id)
        direction = Direction.LONG.value
        if int(d["side"]) == 1:
            direction = Direction.SHORT.value

        order_time = get_str_dt_use_timestamp(d["create_time"], mill=1)
        update_time = get_str_dt_use_timestamp(d["update_time"], mill=1)
        order_type = OrderType.LIMIT.value
        status = d["status"]

        order = OrderData()
        order.order_id = local_order_id
        order.exchange = Exchange.COINEXS.value
        order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
        order.symbol = symbol
        order.vt_symbol = get_vt_key(order.symbol, order.exchange)
        order.price = float(d["price"])
        order.volume = float(d["amount"])
        order.type = order_type
        order.direction = direction
        order.traded = float(d["amount"]) - float(d["left"])

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
                " Exchange :{}, new_status:{}, data:{}".format(Exchange.COINEXS.value, order.status, d))

        order.order_time = order_time
        if order.status == Status.CANCELLED.value:
            order.cancel_time = update_time

        order.gateway_name = self.gateway_name
        self.order_manager.on_order(order)

    def on_query_contract(self, data, request):
        if self.check_error(data, "query_contract"):
            return

        data = data.get("data",[])
        for item in data:
            symbol = get_format_lower_symbol(item["name"])
            contract = ContractData()
            contract.symbol = symbol
            contract.name = symbol.replace("_","/")
            contract.exchange = Exchange.COINEXS.value 
            contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
            contract.price_tick = float(item["tick_size"])
            contract.min_volume = float(item["amount_min"])
            contract.volume_tick =  1
            contract.size = float(item["multiplier"])
            contract.product = Product.FUTURES.value
            contract.gateway_name = self.gateway_name

            self.gateway.on_contract(contract)

    def on_send_order(self, data, request):
        self.gateway.write_log("coinexs, on_send_order, data:{}".format(data))
        order = request.extra

        if self.check_error(data, "send_order"):
            order.status = Status.REJECTED.value
            self.order_manager.on_order(order)
            return

        sys_order_id = str(data["data"]["order_id"])
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
