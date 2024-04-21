# coding=utf-8

import sys
from collections import defaultdict
from tumbler.api.rest import RestClient
from tumbler.constant import Status

from tumbler.object import AccountData
from tumbler.function import get_two_currency, split_url, get_vt_key

from .base import REST_TRADE_HOST
from .base import nexus_format_symbol, sign_request, ORDER_TYPE_VT2NEXUS
from .base import parse_contract_info, parse_account_info, parse_position_list
from .base import parse_order_info, system_symbol_from_nexus


class NexusRestTradeApi(RestClient):

    def __init__(self, gateway):
        super(NexusRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.host = ""
        self.key = ""
        self.secret = ""

        self.set_all_symbols = set([])
        self.asset_dict = defaultdict(float)

    def sign(self, request):
        return sign_request(request)

    def connect(self, key, secret, url=REST_TRADE_HOST, proxy_host="", proxy_port=0):
        self.key = key
        self.secret = secret

        self.host, _ = split_url(REST_TRADE_HOST)

        self.init(REST_TRADE_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("NexusRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, sub):
        if sub.symbol not in self.set_all_symbols:
            a, b = get_two_currency(sub.symbol)
            self.asset_dict[a] += 1
            self.asset_dict[b] += 1
            self.set_all_symbols.add(sub.symbol)

    def send_order(self, req):
        local_order_id = self.order_manager.new_local_order_id()
        data = {
            "symbol": nexus_format_symbol(req.symbol),
            "order_type": "LIMIT",
            "order_price": req.price,
            "order_quantity": req.volume,
            "side": ORDER_TYPE_VT2NEXUS[req.direction]
        }

        order = req.create_order_data(local_order_id, self.gateway_name)

        param1 = "val1"
        param2 = "val2"

        self.add_request(
            "POST",
            "/v1/order",
            callback=self.on_send_order,
            params={
                "param1": param1,
                "param2": param2
            },
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )
        self.order_manager.on_order(order)
        return order.vt_order_id

    def cancel_order(self, req):
        sys_order_id = self.order_manager.get_sys_order_id(req.order_id)
        if sys_order_id:
            data = {
                "order_id": int(sys_order_id),
                "symbol": nexus_format_symbol(req.symbol)
            }
            self.add_request(
                method="DELETE",
                path="/v1/order",
                data=data,
                callback=self.on_cancel_order,
                extra=req
            )
            return data

    def cancel_system_order(self, symbol, order_id):
        data = {
            "order_id": int(order_id),
            "symbol": nexus_format_symbol(symbol)
        }
        self.add_request(
            method="DELETE",
            path="/v1/order",
            data=data,
            callback=self.on_cancel_system_order,
            extra=order_id
        )
        return data

    def query_contract(self):
        self.add_request(
            method="GET",
            path="/v1/public/info",
            callback=self.on_query_contract
        )

    def query_account(self):
        self.add_request(
            method="GET",
            path="/v1/client/info",
            params={},
            callback=self.on_query_account
        )

    def query_positions(self):
        self.add_request(
            "GET",
            "/v2/client/holding",
            params={},
            callback=self.on_query_position
        )

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                "GET",
                "/v1/orders",
                params={
                    "symbol": nexus_format_symbol(symbol),
                    "status": "INCOMPLETE"
                },
                callback=self.on_query_open_orders
            )

    def on_query_contract(self, data, request):
        if self.check_error(data, "query_contract"):
            return

        for d in data["rows"]:
            contract = parse_contract_info(d, self.gateway_name)
            self.gateway.on_contract(contract)

        self.gateway.write_log("query contract success!")

    def on_query_account(self, data, request):
        if self.check_error(data, "query_account"):
            return
        self.gateway.write_log("data:{}".format(data))
        self.gateway.write_log("asset_dict:{}".format(self.asset_dict))

        account_info = parse_account_info(data, self.gateway_name)
        balance = account_info.balance
        len_all = len(self.set_all_symbols) * 2
        for asset, val in self.asset_dict.items():
            acct = AccountData()
            acct.account_id = asset
            acct.vt_account_id = get_vt_key(acct.account_id, self.gateway_name)
            acct.balance = balance * 1.0 * val / len_all
            acct.frozen = 0
            acct.available = acct.balance - acct.frozen
            acct.gateway_name = self.gateway_name
            self.gateway.write_log("acct:{}".format(acct.__dict__))
            self.gateway.on_account(acct)

    def on_query_position(self, data, request):
        if self.check_error(data, "query_account"):
            return

        position_list = parse_position_list(data, self.gateway_name)
        for position in position_list:
            self.gateway.on_position(position)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_open_orders"):
            return

        for d in data["rows"]:
            sys_order_id = str(d["order_id"])
            symbol = system_symbol_from_nexus(d["symbol"])
            if symbol not in self.set_all_symbols:
                continue

            # 把非本系统发的订单，全部撤掉
            if self.order_manager.has_system_order(sys_order_id) is False:
                self.cancel_system_order(symbol, sys_order_id)
                continue

            bef_order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
            # 不与ws_trade_api 冲突
            if bef_order is not None and self.gateway.ws_trade_api is not None:
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            order = parse_order_info(d, local_order_id, self.gateway_name)
            self.order_manager.on_order(order)

    def on_query_order(self, d, request):
        if self.check_error(d, "query_order"):
            return

        sys_order_id = str(d["order_id"])
        symbol = system_symbol_from_nexus(d["symbol"])
        if symbol not in self.set_all_symbols:
            return

        # 把非本系统发的订单，全部撤掉
        if self.order_manager.has_system_order(sys_order_id) is False:
            self.cancel_system_order(symbol, sys_order_id)
            return

        local_order_id = self.order_manager.get_local_order_id(sys_order_id)
        order = parse_order_info(d, local_order_id, self.gateway_name)

        if self.gateway.ws_trade_api is not None:
            # 如果 ws trade 没开， 那就疯狂推送订单信息
            self.order_manager.on_order(order)
        else:
            # 为了不与 ws 部分冲突， 只有确定订单是完结订单，才会更新， 做一个 ws的补充，防止那边不推送的情况
            if not order.is_active():
                self.order_manager.on_order(order)

    def on_send_order(self, data, request):
        """Websocket will push a new order status"""
        order = request.extra

        if self.check_error(data, "send_order"):
            order.status = Status.REJECTED.value
            self.gateway.on_order(order)
            return

        sys_order_id = str(data["order_id"])
        self.order_manager.update_order_id_map(order.order_id, sys_order_id)

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

    def check_error(self, data, func=""):
        if data["success"]:
            return False

        self.gateway.write_log("{} request_error, data:{}".format(str(func), str(data)))
        return True
