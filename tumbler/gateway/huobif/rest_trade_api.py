# coding=utf-8

from copy import copy
import sys
from datetime import datetime
from threading import Lock

from tumbler.api.rest import RestClient
from tumbler.function import get_vt_key, split_url
from tumbler.constant import (
    Exchange,
    Status,
    Direction
)
from tumbler.object import (
    PositionData
)

from tumbler.service import log_service_manager
from tumbler.gateway.huobi.base import sign_request
from tumbler.gateway.huobis.base import DIRECTION_VT2HUOBIS, OFFSET_VT2HUOBIS, ORDERTYPE_VT2HUOBIS

from .base import REST_TRADE_HOST, parse_contract_info, parse_account_info
from .base import parse_position_holding, parse_order_info, get_huobi_future_cancel_order_format_symbol
from .base import get_huobi_future_system_format_symbol, get_from_huobi_to_system_format
from .base import get_huobi_future_ws_system_format_symbol


class HuobifRestTradeApi(RestClient):
    """
    HUOBIS REST API
    """

    def __init__(self, gateway):
        """"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.order_manager = gateway.order_manager

        self.order_count = 1000000
        self.order_count_lock = Lock()
        self.set_all_symbols = set([])

        self.connect_time = 0

        self.host = ""
        self.key = ""
        self.secret = ""
        self.account_id = ""

        self.positions = {}

    def sign(self, request):
        """
        Generate HUOBIF signature.
        """
        return sign_request(request, self.key, self.secret, self.host)

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def connect(self, key, secret, proxy_host="", proxy_port=0):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret
        self.host, _ = split_url(REST_TRADE_HOST)

        self.connect_time = (int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count)

        self.init(REST_TRADE_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("HuobifRestTradeApi start successily!")
        self.query_contract()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def unsubscribe(self, req):
        self.set_all_symbols.remove(req.symbol)

    def query_account(self):
        self.add_request(
            method="POST",
            path="/api/v1/contract_account_info",
            callback=self.on_query_account
        )

    def query_position(self):
        self.add_request(
            method="POST",
            path="/api/v1/contract_position_info",
            callback=self.on_query_position
        )

    def query_open_orders(self):
        tmp_set_all_symbols = set([])
        for symbol in self.set_all_symbols:
            tmp_set_all_symbols.add(get_huobi_future_ws_system_format_symbol(symbol))

        for symbol in tmp_set_all_symbols:
            # Open Orders
            data = {"symbol": symbol}

            self.add_request(
                method="POST",
                path="/api/v1/contract_openorders",
                callback=self.on_query_open_orders,
                data=data
            )

    def query_orders(self):
        list_order_ids = list(self.order_manager.get_all_alive_system_id())
        self.query_complete_orders(list_order_ids)

    def get_orders_dict(self, list_order_ids):
        orders_dic = {}
        for sys_order_id in list_order_ids:
            sys_order_id = str(sys_order_id)
            order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
            if order:
                symbol = order.symbol
                if symbol not in orders_dic.keys():
                    orders_dic[symbol] = [sys_order_id]
                else:
                    orders_dic[symbol].append(sys_order_id)
            else:
                self.gateway.write_log("[get_orders_dict] not found sys_order_id:{}".format(sys_order_id))
        return orders_dic

    def query_complete_orders(self, list_order_ids):
        orders_dic = self.get_orders_dict(list_order_ids)
        for symbol, sys_order_ids in orders_dic.items():
            if sys_order_ids:
                data = {
                    "symbol": get_huobi_future_ws_system_format_symbol(symbol),
                    "order_id": ','.join(sys_order_ids)
                }
                self.add_request(
                    method="POST",
                    path="/api/v1/contract_order_info",
                    callback=self.on_query_complete_orders,
                    data=data
                )

    def query_contract(self):
        self.add_request(
            method="GET",
            path="/api/v1/contract_contract_info",
            callback=self.on_query_contract
        )

    def format_req(self, req):
        local_order_id = str(self.connect_time + self._new_order_id())
        order = req.create_order_data(
            local_order_id,
            self.gateway_name
        )

        data = {
            "contract_code": get_huobi_future_system_format_symbol(req.symbol),
            "client_order_id": int(local_order_id),
            "price": req.price,
            "volume": int(req.volume),
            "direction": DIRECTION_VT2HUOBIS.get(req.direction, ""),
            "offset": OFFSET_VT2HUOBIS.get(req.offset, ""),
            "order_price_type": ORDERTYPE_VT2HUOBIS.get(req.type, ""),
            "lever_rate": 3
        }
        return data, order

    def send_order(self, req):
        data, order = self.format_req(req)

        self.add_request(
            method="POST",
            path="/api/v1/contract_order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        self.order_manager.on_order(order)
        return order.vt_order_id

    def send_orders(self, reqs):
        orders_data = []
        orders = []

        for req in reqs:
            d, order = self.format_req(req)
            self.order_manager.on_order(order)

            orders_data.append(d)
            orders.append(order)

        data = {
            "orders_data": orders_data
        }

        self.add_request(
            method="POST",
            path="/api/v1/contract_batchorder",
            callback=self.on_send_orders,
            data=data,
            extra=orders,
            on_error=self.on_send_orders_error,
            on_failed=self.on_send_orders_failed
        )

        return [(order.vt_order_id, copy(order)) for order in orders]

    def cancel_order(self, req):
        data = {
            "symbol": get_huobi_future_cancel_order_format_symbol(req.symbol),
        }
        sys_order_id = self.order_manager.get_sys_order_id(req.order_id)
        data["order_id"] = sys_order_id

        self.add_request(
            method="POST",
            path="/api/v1/contract_cancel",
            callback=self.on_cancel_order,
            on_failed=self.on_cancel_order_failed,
            data=data,
            extra=req
        )

    def cancel_orders(self, reqs):
        if reqs:
            arr_sys_order_ids = [self.order_manager.get_sys_order_id(req.order_id) for req in reqs]
            arr_sys_order_ids = [sys_order_id for sys_order_id in arr_sys_order_ids if sys_order_id is not None]
            orders_dic = self.get_orders_dict(arr_sys_order_ids)
            for symbol, sys_order_ids in orders_dic.items():
                data = {
                    "symbol": get_huobi_future_cancel_order_format_symbol(symbol),
                    "order_id": ','.join(sys_order_ids)
                }
                self.add_request(
                    method="POST",
                    path="/api/v1/contract_cancel",
                    callback=self.on_cancel_orders,
                    on_failed=self.on_cancel_orders_failed,
                    data=data
                )

    def cancel_system_order(self, symbol, sys_order_id):
        data = {
            "symbol": get_huobi_future_cancel_order_format_symbol(symbol),
            "order_id": sys_order_id
        }

        self.add_request(
            method="POST",
            path="/api/v1/contract_cancel",
            callback=self.on_cancel_system_order,
            data=data,
            extra=sys_order_id
        )

    def on_query_account(self, data, request):
        if self.check_error(data, "query_account"):
            return

        for d in data["data"]:
            account = parse_account_info(d, self.gateway_name)
            self.gateway.on_account(account)

    def on_query_position(self, data, request):
        if self.check_error(data, "query_position"):
            return

        all_position_sets = set([])
        for symbol in self.set_all_symbols:
            for direction in [Direction.LONG.value, Direction.SHORT.value]:
                all_position_sets.add((symbol, direction))

        for d in data["data"]:
            pos = parse_position_holding(d, gateway_name=self.gateway_name)

            self.gateway.on_position(pos)
            if (pos.symbol, pos.direction) in all_position_sets:
                all_position_sets.remove((pos.symbol, pos.direction))

        for symbol, direction in all_position_sets:
            new_pos = PositionData()
            new_pos.symbol = symbol
            new_pos.exchange = Exchange.HUOBIF.value
            new_pos.vt_symbol = get_vt_key(new_pos.symbol, new_pos.exchange)
            new_pos.direction = direction
            new_pos.position = 0
            new_pos.vt_position_id = get_vt_key(new_pos.vt_symbol, new_pos.direction)
            self.gateway.on_position(new_pos)

    def on_query_complete_orders(self, data, request):
        if self.check_error(data, "query_complete_order"):
            return

        for d in data["data"]:
            sys_order_id = str(d["order_id"])
            symbol = get_from_huobi_to_system_format(d["contract_code"])

            if symbol not in self.set_all_symbols:
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            order = parse_order_info(d, symbol, local_order_id, self.gateway_name)
            self.order_manager.on_order(order)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_order"):
            return

        list_order_ids = list(self.order_manager.get_all_alive_system_id())
        for d in data["data"]["orders"]:
            sys_order_id = str(d["order_id"])
            symbol = get_from_huobi_to_system_format(d["contract_code"])

            if symbol not in self.set_all_symbols:
                continue

            if sys_order_id in list_order_ids:
                list_order_ids.remove(sys_order_id)

            # 把非本系统发的订单，全部撤掉
            if self.order_manager.has_system_order(sys_order_id) is False:
                self.cancel_system_order(symbol, sys_order_id)
                continue

            bef_order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
            # 不与ws_trade_api 冲突
            if bef_order is not None and self.gateway.ws_trade_api is not None:
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            order = parse_order_info(d, symbol, local_order_id, self.gateway_name)

            self.order_manager.on_order(order)

        self.query_complete_orders(list_order_ids)
        self.gateway.write_log("order information query success!")

    def on_query_contract(self, data, request):
        if self.check_error(data, "query_contract"):
            return

        for d in data["data"]:
            contract = parse_contract_info(d, self.gateway_name)
            self.gateway.on_contract(contract)

        self.gateway.write_log("query contract success!")

    def on_send_order(self, data, request):
        order = request.extra
        log_service_manager.write_log("[on_send_order] data:{} vt_order_id:{}".format(data, order.vt_order_id))

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
        if str(status_code) not in ["504"]:
            self.order_manager.on_order(order)

        msg = "send_orders failed,status:{},information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        self.order_manager.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data, request):
        self.gateway.write_log("[on_cancel_order] data:{}".format(data))
        cancel_request = request.extra
        local_order_id = cancel_request.order_id
        order = self.order_manager.get_order_with_local_order_id(local_order_id)

        if self.check_error(data, "cancel_order"):
            self.gateway.write_log("cancel_order failed!{}".format(str(order.order_id)))
            return
        else:
            if data["data"]["successes"]:
                order.status = Status.CANCELLED.value
                self.gateway.write_log("cancel_order success!{}".format(str(order.order_id)))
                self.order_manager.on_order(order)

    def on_cancel_orders(self, data, request):
        if self.check_error(data, "cancel_orders"):
            self.gateway.write_log("cancel_orders failed!")
            return
        else:
            successes = data["data"]["successes"]
            success_orders = successes.split(',')
            for sys_order_id in success_orders:
                order = self.order_manager.get_order_with_sys_order_id(str(sys_order_id))
                order.status = Status.CANCELLED.value
                self.gateway.write_log("cancel_orders success!{}".format(str(order.order_id)))
                self.order_manager.on_order(order)

            for dic in data["data"]["errors"]:
                sys_order_id = str(dic["order_id"])
                err_code = dic["err_code"]
                err_msg = dic["err_msg"]
                self.gateway.write_log("cancel_orders failed! order_id:{} err_code:{} err_msg:{}"
                                       .format(sys_order_id, err_code, err_msg))

    def on_cancel_system_order(self, data, request):
        msg = "on_cancel_system_order , sys_order_id:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_cancel_order_failed(self, status_code, request):
        """
        Callback when canceling order failed on server.
        """
        msg = "cancel order failed，status：{}，msg：{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_cancel_orders_failed(self, status_code, request):
        """
        Callback when canceling order failed on server.
        """
        msg = "cancel orders failed，status：{}，msg：{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_orders(self, data, request):
        orders = request.extra
        if self.check_error(data, "on_send_orders"):
            for order in orders:
                order.status = Status.REJECTED.value
                self.order_manager.on_order(order)
            return
        else:
            errors = data["data"].get("errors", None)
            if errors:
                for d in errors:
                    ix = d["index"] - 1
                    code = d["err_code"]
                    msg = d["err_msg"]

                    order = orders[ix]
                    order.status = Status.REJECTED.value
                    self.order_manager.on_order(order)

                    msg = "send_orders failed，status：{}，msg：{}".format(code, msg)
                    self.gateway.write_log(msg)

            success = data["data"].get("success", None)
            if success:
                for d in success:
                    sys_order_id = str(d["order_id"])
                    ix = d["index"] - 1
                    order = orders[ix]
                    self.order_manager.update_order_id_map(order.order_id, sys_order_id)
                    self.gateway.write_log("[on_send_orders] updated order_id:{} sys_order_id:{}"
                                           .format(order.order_id, sys_order_id))

    def on_send_orders_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        self.gateway.write_log("on_send_orders_failed status_code:{}".format(status_code))
        orders = request.extra
        if str(status_code) not in ["504"]:
            for order in orders:
                order.status = Status.REJECTED.value
                self.order_manager.on_order(order)

        msg = "send_orders failed,status:{},information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

    def on_send_orders_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending order caused exception.
        """
        self.gateway.write_log(
            "on_send_orders_error exception_type:{} exception_value:{}".format(exception_type, exception_value))

        orders = request.extra
        for order in orders:
            order.status = Status.REJECTED.value
            self.order_manager.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Callback to handler request exception.
        """
        msg = "on_error,status_code:{},information:{}".format(str(exception_type), str(exception_value))
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )

    def check_error(self, data, func=""):
        if data["status"] != "error":
            return False

        error_code = data["err_code"]
        error_msg = data["err_msg"]

        self.gateway.write_log(
            "{} request_error, status_code:{},information:{}".
                format(str(func), str(error_code), str(error_msg)))
        return True

