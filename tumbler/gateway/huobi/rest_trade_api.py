# coding=utf-8

import sys

from tumbler.api.rest import RestClient
from tumbler.service import log_service_manager
from tumbler.function import get_format_lower_symbol, get_no_under_lower_symbol, split_url
from tumbler.constant import (
    RunMode,
    Status
)

from .base import REST_TRADE_HOST, parse_order_info, parse_contract_info, parse_account_info
from .base import create_signature, ORDER_TYPE_VT2HUOBI, sign_request


class HuobiRestTradeApi(RestClient):
    """
    HUOBI REST API
    """

    def __init__(self, gateway):
        super(HuobiRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.host = ""
        self.key = ""
        self.secret = ""
        #self.account_id = "104997"
        self.account_id = "11893624"

        self.set_all_symbols = set([])

        self.run_mode = RunMode.NORMAL.value

    def sign(self, request):
        """
        Generate HUOBI signature.
        """
        return sign_request(request, self.key, self.secret, self.host)

    def connect(self, key, secret, proxy_host="", proxy_port=0, run_mode=RunMode.NORMAL.value):
        """
        Initialize connection to REST server.
        """
        self.run_mode = run_mode

        self.key = key
        self.secret = secret

        self.host, _ = split_url(REST_TRADE_HOST)

        self.init(REST_TRADE_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("HuobiRestTradeApi start success!")

        self.query_contract()
        self.query_account()
        self.query_open_orders()

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_account(self):
        self.add_request(
            method="GET",
            path="/v1/account/accounts",
            callback=self.on_query_account
        )

    def query_account_balance(self):
        path = "/v1/account/accounts/{}/balance".format(self.account_id)
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account_balance
        )

    def query_open_orders(self):
        self.add_request(
            method="GET",
            path="/v1/order/openOrders",
            callback=self.on_query_open_orders
        )

    def query_complete_orders(self):
        list_order_ids = list(self.order_manager.get_all_alive_system_id())
        for order_id in list_order_ids:
            self.add_request(
                method="GET",
                path="/v1/order/orders/{}".format(order_id),
                callback=self.on_query_order
            )

    def query_contract(self):
        self.add_request(
            method="GET",
            path="/v1/common/symbols",
            callback=self.on_query_contract
        )

    def format_req(self, req):
        huobi_type = ORDER_TYPE_VT2HUOBI.get(
            (req.direction, req.type), ""
        )

        local_order_id = self.order_manager.new_local_order_id()
        order = req.create_order_data(
            local_order_id,
            self.gateway_name
        )

        data = {
            "account-id": self.account_id,
            "amount": str(req.volume),
            "symbol": get_no_under_lower_symbol(req.symbol),
            "type": huobi_type,
            "price": str(req.price),
            "source": "api"
        }
        return data, order

    def send_order(self, req):
        data, order = self.format_req(req)

        self.gateway.on_order(order)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.direct_request(
                method="POST",
                path="/v1/order/orders/place",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed
            )
        else:
            self.add_request(
                method="POST",
                path="/v1/order/orders/place",
                callback=self.on_send_order,
                data=data,
                extra=order,
                on_error=self.on_send_order_error,
                on_failed=self.on_send_order_failed
            )

        self.order_manager.on_order(order)
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
                path="/v1/order/batch-orders",
                callback=self.on_send_orders,
                data=data_rets,
                extra=orders,
                on_error=self.on_send_orders_error,
                on_failed=self.on_send_orders_failed
            )
        else:
            self.add_request(
                method="POST",
                path="/v1/order/batch-orders",
                callback=self.on_send_orders,
                data=data_rets,
                extra=orders,
                on_error=self.on_send_orders_error,
                on_failed=self.on_send_orders_failed
            )

        for order in orders:
            self.order_manager.on_order(order)
        return rets

    def cancel_order(self, req):
        sys_order_id = self.order_manager.get_sys_order_id(req.order_id)

        path = "/v1/order/orders/" + str(sys_order_id) + "/submitcancel"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_cancel_order,
            extra=req
        )

    def cancel_orders(self, reqs):
        if reqs:
            arr_sys_order_ids = [self.order_manager.get_sys_order_id(req.order_id) for req in reqs]
            data = {
                "order-ids": arr_sys_order_ids
            }
            path = "/v1/order/orders/batchcancel"
            self.add_request(
                method="POST",
                path=path,
                data=data,
                callback=self.on_cancel_orders,
                extra=reqs
            )

    def cancel_system_order(self, sys_order_id):
        path = "/v1/order/orders/" + str(sys_order_id) + "/submitcancel"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_cancel_system_order,
            extra=sys_order_id
        )

    def on_query_account(self, data, request):
        if self.check_error(data, "query_account"):
            return

        for d in data["data"]:
            if d["type"] == "spot":
                self.account_id = d["id"]
                self.gateway.write_log("account_id:{} query success!".format(self.account_id))

        self.query_account_balance()

    def on_query_account_balance(self, data, request):
        if self.check_error(data, "query_account_fund"):
            return

        account_list = parse_account_info(data, self.gateway_name)
        for account in account_list:
            if account.balance:
                self.gateway.on_account(account)

    def on_query_order(self, data, request):
        if self.check_error(data, "query_order"):
            return
        d = data["data"]
        sys_order_id = str(d["id"])
        symbol = get_format_lower_symbol(d["symbol"])
        if symbol not in self.set_all_symbols:
            return

        # 把非本系统发的订单，全部撤掉
        if self.order_manager.has_system_order(sys_order_id) is False:
            self.cancel_system_order(sys_order_id)
            return

        local_order_id = self.order_manager.get_local_order_id(sys_order_id)

        order = parse_order_info(d, symbol, local_order_id, self.gateway_name, _type="query_order")

        if self.gateway.ws_trade_api is not None:
            # 如果 ws trade 没开， 那就疯狂推送订单信息
            self.order_manager.on_order(order)
        else:
            # 为了不与 ws 部分冲突， 只有确定订单是完结订单，才会更新， 做一个 ws的补充，防止那边不推送的情况
            if not order.is_active():
                self.order_manager.on_order(order)

    def on_query_open_orders(self, data, request):
        if self.check_error(data, "query_open_orders"):
            return

        for d in data["data"]:
            sys_order_id = str(d["id"])
            symbol = get_format_lower_symbol(d["symbol"])

            if symbol not in self.set_all_symbols:
                continue

            # 把非本系统发的订单，全部撤掉
            if self.order_manager.has_system_order(sys_order_id) is False:
                self.cancel_system_order(sys_order_id)
                continue

            bef_order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
            # 不与ws_trade_api 冲突
            if bef_order is not None and self.gateway.ws_trade_api is not None:
                continue
            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            order = parse_order_info(d, symbol, local_order_id, self.gateway_name, _type="query_open_order")

            self.order_manager.on_order(order)

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

        sys_order_id = data["data"]
        self.order_manager.update_order_id_map(order.order_id, sys_order_id)

    def on_send_orders(self, data, request):
        orders = request.extra
        if self.check_error(data, "send_orders"):
            for order in orders:
                order.status = Status.REJECTED.value
                self.order_manager.on_order(order)
            return

        i = 0
        for dic in data["data"]:
            order = orders[i]
            if "order-id" in dic.keys():
                sys_order_id = str(dic["order-id"])
                self.order_manager.update_order_id_map(order.order_id, sys_order_id)
                log_service_manager.write_log("[on_send_orders] sys_order_id:{} vt_order_id:{}"
                                              .format(sys_order_id, order.vt_order_id))

                i = i + 1
            else:
                err_code = dic.get('err-code', "e8")
                err_msg = dic.get('err-msg', "error in order id")
                self.gateway.write_log("[on_send_orders] err_code:{}, err_msg:{} vt_order_id:{} dic:{}"
                                       .format(err_code, err_msg, order.vt_order_id, dic))

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        self.gateway.write_log("[on_send_order_failed] status_code:{} vt_order_id:{}"
                               .format(status_code, order.vt_order_id))
        order.status = Status.REJECTED.value
        if str(status_code) not in ["504"]:
            self.gateway.on_order(order)

        msg = "send_order failed,status:{},information:{}".format(status_code, request.response.text)
        self.gateway.write_log(msg)

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

    def on_send_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED.value
        self.gateway.write_log("[on_send_order_error] exception_value:{} vt_order_id:{}"
                               .format(exception_value, order.vt_order_id))
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
            self.order_manager.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_system_order(self, data, request):
        msg = "on_cancel_system_order , sys_order_id:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_cancel_order(self, data, request):
        cancel_request = request.extra
        local_order_id = cancel_request.order_id
        order = self.order_manager.get_order_with_local_order_id(local_order_id)

        if self.check_error(data, "cancel_order"):
            if order:
                self.gateway.write_log("cancel_order failed!{}".format(str(order.order_id)))
            return
        else:
            order.status = Status.CANCELLED.value
            if order:
                self.gateway.write_log("cancel_order success!{}".format(str(order.order_id)))

        self.order_manager.on_order(order)

    def on_cancel_orders(self, data, request):
        if self.check_error(data, "cancel_orders"):
            self.gateway.write_log("cancel_orders failed!")
            return
        else:
            for sys_order_id in data["data"]["success"]:
                order = self.order_manager.get_order_with_sys_order_id(str(sys_order_id))
                if order:
                    self.gateway.write_log("cancel_orders success!{}".format(str(order.order_id)))

            for sys_order_id in data["data"]["failed"]:
                self.gateway.write_log("cancel_orders failed!{}".format(str(sys_order_id)))

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

        error_code = data["err-code"]
        error_msg = data["err-msg"]

        self.gateway.write_log(
            "{} request_error, status_code:{},information:{}".format(str(func),
                str(error_code), str(error_msg)))
        return True
