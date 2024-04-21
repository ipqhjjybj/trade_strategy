# coding=utf-8

import hashlib
import hmac
import sys
import time
from datetime import datetime
from tumbler.function import urlencode, split_url, get_vt_key
from tumbler.api.rest import RestClient
from tumbler.object import (
    OrderData,
    TradeData,
    AccountData,
    ContractData
)
from tumbler.constant import (
    Direction,
    Exchange,
    OrderType,
    Product,
    Status
)
from .base import REST_TRADE_HOST, _bittrex_format_symbol, _bittrex_symbol_to_system_symbol
from .base import _bittrex_to_system_format_date


class BittrexRestTradeApi(RestClient):
    """
    REST TRADE API
    """

    def __init__(self, gateway):
        super(BittrexRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.host = ""
        self.key = ""
        self.secret = ""

        self.set_all_symbols = set([])

    def sign(self, request):
        if request.method in ["GET"]:
            request.params["apikey"] = self.key
            request.params["nonce"] = str(int(time.time() * 1000))
            url = REST_TRADE_HOST + request.path + "?" + urlencode(request.params)
            signature = hmac.new(self.secret.encode('utf-8'), url.encode('utf-8'), hashlib.sha512).hexdigest()
            request.headers = {"apisign": signature}

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

        self.gateway.write_log("BittrexRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_account(self):
        self.add_request(
            method="GET",
            params={},
            path="/api/v1.1/account/getbalances",
            callback=self.on_query_account
        )

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                method="GET",
                params={"market": _bittrex_format_symbol(symbol)},
                path="/api/v1.1/market/getopenorders",
                callback=self.on_query_open_orders
            )

    def query_order(self, order_id):
        self.add_request(
            method="GET",
            params={"uuid": order_id},
            path="/api/v1.1/market/getopenorders",
            callback=self.on_query_order
        )

    def query_contract(self):
        self.add_request(
            method="GET",
            params={},
            path="/api/v1.1/public/getmarkets",
            callback=self.on_query_contract
        )

    def send_order(self, req):
        local_order_id = self.order_manager.new_local_order_id()
        order = req.create_order_data(
            local_order_id,
            self.gateway_name
        )
        data = {
            'market': _bittrex_format_symbol(req.symbol),
            'quantity': req.volume,
            'rate': req.price
        }
        if req.direction == Direction.LONG.value:
            self.add_request(
                method="GET",
                path="/api/v1.1/market/buylimit",
                callback=self.on_send_order,
                params=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed
            )
        else:
            self.add_request(
                method="GET",
                path="/api/v1.1/market/selllimit",
                callback=self.on_send_order,
                params=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed
            )

        self.order_manager.on_order(order)

        return order.vt_order_id

    def cancel_order(self, req):
        sys_order_id = self.order_manager.get_sys_order_id(req.order_id)
        data = {
            "uuid": sys_order_id
        }
        self.add_request(
            method="GET",
            path="/api/v1.1/market/cancel",
            params=data,
            callback=self.on_cancel_order,
            extra=req
        )

    def cancel_system_order(self, sys_order_id):
        data = {
            "uuid": sys_order_id
        }
        self.add_request(
            method="GET",
            path="/api/v1.1/market/cancel",
            params=data,
            callback=self.on_cancel_system_order,
            extra=sys_order_id
        )

    def on_query_account(self, data, request):
        if self.check_error(data, "query_account"):
            return

        data = data.get("result", [])
        for dic in data:
            asset = dic["Currency"].lower()
            account = AccountData()
            account.account_id = asset
            account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
            account.balance = float(dic["Balance"])
            account.frozen = float(dic["Pending"])
            account.available = float(dic["Available"])
            account.gateway_name = self.gateway_name

            if account.balance:
                self.gateway.on_account(account)

    def on_query_order(self, data, request):
        if self.check_error(data, "query_order"):
            return

        arr = data["result"]
        for d in arr:
            symbol = _bittrex_symbol_to_system_symbol(d["Exchange"])
            if symbol not in self.set_all_symbols:
                return

            sys_order_id = str(d["OrderUuid"])
            if not self.order_manager.has_system_order(sys_order_id):
                self.cancel_system_order(sys_order_id)
                return

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            if local_order_id:
                bef_order = self.order_manager.get_order_with_local_order_id(local_order_id)

                direction = Direction.LONG.value
                if d["OrderType"] == "LIMIT_SELL":
                    direction = Direction.SHORT.value

                order_time = _bittrex_to_system_format_date(d["Opened"])

                closed = d.get("Closed", False)

                order = OrderData()
                order.order_id = local_order_id
                order.exchange = Exchange.BITTREX.value
                order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
                order.symbol = symbol
                order.vt_symbol = get_vt_key(order.symbol, order.exchange)
                order.price = float(d["Limit"])
                order.volume = float(d["Quantity"])
                order.type = OrderType.LIMIT.value
                order.direction = direction
                order.traded = float(d["Quantity"]) - float(d["QuantityRemaining"])
                order.status = Status.NOTTRADED.value
                if closed:
                    if order.traded + 1e-12 < order.volume:
                        order.status = Status.ALLTRADED.value
                    else:
                        order.status = Status.CANCELLED.value
                else:
                    if order.traded > 1e-12:
                        order.status = Status.PARTTRADED.value

                order.order_time = order_time
                order.gateway_name = self.gateway_name

                if order.status == Status.CANCELLED.value:
                    order.cancel_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                self.order_manager.on_order(order)

                new_traded_volume = order.traded - bef_order.traded
                if new_traded_volume > 0:
                    trade = TradeData()
                    trade.symbol = order.symbol
                    trade.exchange = Exchange.BITTREX.value
                    trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
                    trade.order_id = order.order_id
                    trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
                    trade.trade_id = str(sys_order_id) + "_" + str(time.time())
                    trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
                    trade.direction = order.direction
                    trade.type = order.type
                    trade.offset = order.offset
                    trade.price = order.price
                    trade.volume = new_traded_volume
                    trade.trade_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    trade.gateway_name = self.gateway_name

                    self.gateway.on_trade(trade)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_order"):
            return

        before_has_system_ids = set(self.order_manager.get_all_alive_system_id())

        orders = data.get("result", [])
        for d in orders:
            symbol = _bittrex_symbol_to_system_symbol(d["Exchange"])
            if symbol not in self.set_all_symbols:
                continue
            sys_order_id = str(d["OrderUuid"])

            if not self.order_manager.has_system_order(sys_order_id):
                self.cancel_system_order(sys_order_id)
                continue

            if sys_order_id in before_has_system_ids:
                before_has_system_ids.remove(sys_order_id)

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            if not local_order_id:
                continue

            bef_order = self.order_manager.get_order_with_local_order_id(local_order_id)

            direction = Direction.LONG.value
            if d["OrderType"] == "LIMIT_SELL":
                direction = Direction.SHORT.value

            order_time = _bittrex_to_system_format_date(d["Opened"])

            closed = d.get("Closed", False)

            order = OrderData()
            order.order_id = local_order_id
            order.exchange = Exchange.BITTREX.value
            order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
            order.symbol = symbol
            order.vt_symbol = get_vt_key(order.symbol, order.exchange)
            order.price = float(d["Limit"])
            order.volume = float(d["Quantity"])
            order.type = OrderType.LIMIT.value
            order.direction = direction
            order.traded = float(d["Quantity"]) - float(d["QuantityRemaining"])
            order.status = Status.NOTTRADED.value
            if closed:
                if order.traded + 1e-12 < order.volume:
                    order.status = Status.ALLTRADED.value
                else:
                    order.status = Status.CANCELLED.value
            else:
                if order.traded > 1e-12:
                    order.status = Status.PARTTRADED.value

            order.order_time = order_time
            order.gateway_name = self.gateway_name

            if order.status == Status.CANCELLED.value:
                order.cancel_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.order_manager.on_order(order)

            new_traded_volume = order.traded - bef_order.traded
            if new_traded_volume > 0:
                trade = TradeData()
                trade.symbol = order.symbol
                trade.exchange = Exchange.BITTREX.value
                trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
                trade.order_id = order.order_id
                trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
                trade.trade_id = str(sys_order_id) + "_" + str(time.time())
                trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
                trade.direction = order.direction
                trade.type = order.type
                trade.offset = order.offset
                trade.price = order.price
                trade.volume = new_traded_volume
                trade.trade_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                trade.gateway_name = self.gateway_name

                self.gateway.on_trade(trade)

        if before_has_system_ids:
            for sys_order_id in before_has_system_ids:
                self.query_order(sys_order_id)

    def on_query_contract(self, data, request):
        if self.check_error(data, "query_contract"):
            return

        data = data.get("result", {})
        for dic in data:
            symbol = _bittrex_symbol_to_system_symbol(dic["MarketName"])
            contract = ContractData()
            contract.symbol = symbol
            contract.name = symbol.replace('_', '/')
            contract.exchange = Exchange.BITTREX.value
            contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
            contract.price_tick = 10 ** (-1 * 8)
            contract.size = 1
            contract.min_volume = float(dic["MinTradeSize"])
            contract.volume_tick = 10 ** (-1 * 3)
            contract.product = Product.SPOT.value
            contract.gateway_name = self.gateway_name

            self.gateway.on_contract(contract)

    def on_send_order(self, data, request):
        order = request.extra

        if self.check_error(data, "send_order"):
            order.status = Status.REJECTED.value
            self.order_manager.on_order(order)
            return

        sys_order_id = str(data["result"]["uuid"])
        self.order_manager.update_order_id_map(order.order_id, sys_order_id)

        order.status = Status.NOTTRADED.value
        self.order_manager.on_order(order)

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
            #order.status = Status.REJECTED.value
            self.gateway.write_log("cancel_order failed!{}".format(str(order.order_id)))
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
        if data["success"] is True:
            return False

        error_code = "b0"
        error_msg = data.get("message", str(data))

        self.gateway.write_log(
            "{}query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True
