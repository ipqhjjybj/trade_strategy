# coding=utf-8

import time
import sys
from datetime import datetime
from threading import Lock
from requests import ConnectionError

from tumbler.api.rest import RestClient
from tumbler.constant import (
    OrderType,
    Status,
    Exchange,
    Direction,
    RunMode
)

from tumbler.gateway.okex.base import sign_request
from tumbler.object import PositionData, OrderData
from .base import okexf_format_symbol, okexf_format_to_system_format, get_vt_key
from .base import get_underlying_symbol, parse_single_account
from .base import REST_TRADE_HOST, TYPE_VT2OKEXF
from .base import parse_contract_info, parse_order_info
from .base import parse_single_position


class OkexfRestTradeApi(RestClient):
    """
    OKEX REST TRADE API
    """

    def __init__(self, gateway):
        super(OkexfRestTradeApi, self).__init__()

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

        self.run_mode = RunMode.NORMAL.value

    def sign(self, request):
        """
        Generate OKEX signature.
        """
        return sign_request(request, self.key, self.secret, self.passphrase)

    def connect(self, key, secret, passphrase="", url=REST_TRADE_HOST, proxy_host="", proxy_port=0, run_mode=RunMode.NORMAL.value):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

        self.run_mode = run_mode

        self.init(url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("OkexfRestTradeApi start success!")

        self.query_time()
        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, sub):
        self.set_all_symbols.add(sub.symbol)

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req):
        if (req.offset, req.direction) not in TYPE_VT2OKEXF:
            return None

        order_id = "a{}{}".format(self.connect_time, self._new_order_id())

        data = {
            "client_oid": order_id,
            "type": TYPE_VT2OKEXF[(req.offset, req.direction)],
            "instrument_id": okexf_format_symbol(req.symbol),
            "price": str(req.price),
            "size": str(int(req.volume))
        }

        if req.type == OrderType.MARKET.value:
            data["match_price"] = "1"
        else:
            data["match_price"] = "0"

        order = req.create_order_data(order_id, self.gateway_name)
        self.gateway.on_order(order)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                "POST",
                "/api/futures/v3/order",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_failed=self.on_send_order_failed,
                on_error=self.on_send_order_error,
            )
        else:
            self.add_request(
                "POST",
                "/api/futures/v3/order",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_failed=self.on_send_order_failed,
                on_error=self.on_send_order_error,
            )
        return order.vt_order_id, order

    def cancel_order(self, req):
        path = "/api/futures/v3/cancel_order/{}/{}".format(okexf_format_symbol(req.symbol), req.order_id)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                "POST",
                path,
                callback=self.on_cancel_order,
                on_error=self.on_cancel_order_error,
                on_failed=self.on_cancel_order_failed,
                extra=req
            )
        else:
            self.add_request(
                "POST",
                path,
                callback=self.on_cancel_order,
                on_error=self.on_cancel_order_error,
                on_failed=self.on_cancel_order_failed,
                extra=req
            )

    def cancel_system_order(self, symbol, order_id):
        path = "/api/futures/v3/cancel_order/{}/{}".format(okexf_format_symbol(symbol), order_id)
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
            "/api/futures/v3/instruments",
            callback=self.on_query_contract
        )

    def query_position(self):
        for symbol in self.set_all_symbols:
            self.add_request(
                "GET",
                "/api/futures/v3/{}/position".format(okexf_format_symbol(symbol)),
                callback=self.on_query_position,
                extra=symbol
            )
        ## all positions
        # self.add_request(
        #     "GET",
        #     "/api/futures/v3/position",
        #     callback=self.on_query_position
        # )

    def query_account(self):
        # ## get_all_account
        # self.add_request(
        #     "GET",
        #     "/api/futures/v3/accounts",
        #     callback=self.on_query_account
        # )

        # single account
        for symbol in self.set_all_symbols:
            underly_symbol = get_underlying_symbol(symbol)
            self.add_request(
                "GET",
                "/api/futures/v3/accounts/{}".format(underly_symbol),
                callback=self.on_query_account,
                extra=underly_symbol
            )

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            # get waiting orders
            self.add_request(
                "GET",
                "/api/futures/v3/orders/%s?state=0" % (okexf_format_symbol(symbol)),
                callback=self.on_query_order
            )
            # get part traded orders
            self.add_request(
                "GET",
                "/api/futures/v3/orders/%s?state=1" % (okexf_format_symbol(symbol)),
                callback=self.on_query_order
            )

    def query_send_orders(self):
        now_time = time.time()
        for order_id in self.orders.keys():
            order = self.orders[order_id]
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))
            if order_time - now_time > 5:
                self.add_request(
                    "GET",
                    "/api/futures/v3/orders/%s/%s" % (okexf_format_symbol(order.symbol), order_id),
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
        account = parse_single_account(request.extra, data, Exchange.OKEXF.value)
        self.gateway.on_account(account)

        ## get_all_account
        # cp_symbol_sets = copy(okexf_contract_pairs)
        # account_list = parse_account_info(data, self.gateway_name)
        # for account in account_list:
        #     self.gateway.on_account(account)
        #     if account.account_id in cp_symbol_sets:
        #         cp_symbol_sets.remove(account.account_id)

        # for tsymbol in cp_symbol_sets:
        #     acct = AccountData()
        #     acct.account_id = tsymbol
        #     acct.vt_account_id = get_vt_key(Exchange.OKEXF.value, acct.account_id)
        #     acct.gateway_name = Exchange.OKEXF.value
        #     self.gateway.on_account(acct)

    def on_query_position(self, data, request):
        symbol = request.extra
        ret_positions = []
        all_position_sets = set([])
        for direction in [Direction.LONG.value, Direction.SHORT.value]:
            all_position_sets.add((symbol, direction))

        for d in data["holding"]:
            pos1, pos2 = parse_single_position(d)
            if (pos1.symbol, pos1.direction) in all_position_sets:
                all_position_sets.remove((pos1.symbol, pos1.direction))
            ret_positions.append(pos1)

            if (pos2.symbol, pos2.direction) in all_position_sets:
                all_position_sets.remove((pos2.symbol, pos2.direction))
            ret_positions.append(pos2)

        for symbol, direction in all_position_sets:
            new_pos = PositionData()
            new_pos.symbol = symbol
            new_pos.exchange = Exchange.OKEXF.value
            new_pos.vt_symbol = get_vt_key(new_pos.symbol, new_pos.exchange)
            new_pos.direction = direction
            new_pos.position = 0
            new_pos.vt_position_id = get_vt_key(new_pos.vt_symbol, new_pos.direction)
            ret_positions.append(new_pos)

        # all positions
        # ret_positions = parse_position_info(data, self.set_all_symbols)
        for position in ret_positions:
            self.gateway.on_position(position)

    def work_order(self, order_data):
        order_id = order_data["client_oid"]
        symbol = okexf_format_to_system_format(order_data["instrument_id"])

        if symbol not in self.set_all_symbols:
            return

        if order_id not in self.orders.keys():
            if not order_id:
                order_id = order_data["order_id"]
                self.cancel_system_order(symbol, order_id)
            else:
                self.cancel_system_order(symbol, order_id)
            return

        order = parse_order_info(order_data, self.gateway_name)
        self.gateway.on_order(order)

    def on_query_single_order(self, data, request):
        # okexf 跟其他的不太一样
        if "error_code" in data.keys() or "order_id" not in data.keys():
            if "order_id" not in data.keys():
                order_id, symbol = request.extra
                order = OrderData.make_reject_order(order_id, symbol, Exchange.OKEXS.value, self.gateway_name)
                self.gateway.on_order(order)
            else:
                self.gateway.write_log("Unknown error data:{}".format(data))
        else:
            self.work_order(data)

    def on_query_order(self, data, request):
        for order_data in data["order_info"]:
            self.work_order(order_data)

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
