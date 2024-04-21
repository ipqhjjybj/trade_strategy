# coding=utf-8

import sys
import json
from copy import copy
from datetime import datetime
from requests import ConnectionError
from threading import Lock

from tumbler.api.rest import RestClient
from tumbler.constant import (
    OrderType,
    Status,
    Offset
)
from .base import sign_request, change_from_bitmex_to_system, parse_order_info
from .base import REST_TRADE_HOST, change_from_system_to_bitmex, DIRECTION_VT2BITMEX, ORDER_TYPE_VT2BITMEX


class BitmexRestTradeApi(RestClient):
    """
    BitMEX REST API
    """

    def __init__(self, gateway):
        super(BitmexRestTradeApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.key = ""
        self.secret = ""

        self.order_count = 1000000
        self.order_count_lock = Lock()

        self.set_all_symbols = set([])

        self.connect_time = 0

        # Use 60 by default, and will update after first request
        self.rate_limit_limit = 60
        self.rate_limit_remaining = 60
        self.rate_limit_sleep = 0

    def sign(self, request):
        return sign_request(request, self.key, self.secret)

    def connect(self, key, secret, proxy_host="", proxy_port=0):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret

        self.connect_time = (int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)
        self.start()
        self.gateway.write_log("BITMEX REST API start successily!")

    def _new_order_id(self):
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

    def query_orders(self):
        list_order_ids = list(self.order_manager.get_all_alive_system_id())
        if list_order_ids:
            self.query_complete_orders(list_order_ids)

    def query_open_orders(self):
        for symbol in self.set_all_symbols:
            data = {
                'filter': json.dumps(
                    {
                        'ordStatus.isTerminated': False,
                        'symbol': change_from_system_to_bitmex(symbol)
                    }
                ),
                'count': 500
            }
            self.add_request(
                method="GET",
                path="/api/v1/order",
                params=data,
                callback=self.on_query_open_orders
            )

    def query_complete_orders(self, list_order_ids):
        data = {
            "filter": json.dumps({
                    "orderID": list_order_ids
                }
            )
        }
        self.add_request(
            method="GET",
            path="/api/v1/order",
            params=data,
            callback=self.on_query_order
        )

    def format_req(self, req):
        order_id = str(self.connect_time + self._new_order_id())
        data = {
            'symbol': change_from_system_to_bitmex(req.symbol),
            'orderQty': int(req.volume),
            'price': req.price,
            "side": DIRECTION_VT2BITMEX[req.direction],
            "ordType": ORDER_TYPE_VT2BITMEX[req.type],
            "execInst": ",".join(["ParticipateDoNotInitiate"])
        }
        order = req.create_order_data(order_id, self.gateway_name)
        return data, order

    def send_order(self, req):
        if not self.check_rate_limit():
            return None

        data, order = self.format_req(req)
        self.add_request(
            "POST",
            "/api/v1/order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        self.gateway.on_order(order)
        return order.vt_order_id

    def send_orders(self, reqs):
        if not self.check_rate_limit():
            return None

        ret_orders = []
        create_orders = []
        for req in reqs:
            data, order = self.format_req(req)
            create_orders.append(data)
            ret_orders.append(order)

        data = {
            "orders": json.dumps(create_orders)
        }
        self.add_request(
            "POST",
            "/api/v1/order/bulk",
            callback=self.on_send_orders,
            data=data,
            extra=ret_orders,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        return [(order.vt_order_id, copy(order)) for order in ret_orders]

    def cancel_order(self, req):
        if not self.check_rate_limit():
            return

        sys_order_id = self.order_manager.get_sys_order_id(req.order_id)
        data = {"orderID": sys_order_id}

        self.add_request(
            "DELETE",
            "/api/v1/order",
            callback=self.on_cancel_order,
            data=data,
            on_error=self.on_cancel_order_error,
            extra=req
        )

    def cancel_orders(self, reqs):
        if not self.check_rate_limit():
            return

        order_ids = [self.order_manager.get_sys_order_id(req.order_id) for req in reqs]
        data = {"orderID": json.dumps(order_ids)}

        self.add_request(
            "DELETE",
            "/api/v1/order",
            callback=self.on_cancel_orders,
            data=data,
            on_error=self.on_cancel_orders_error,
            extra=reqs
        )

    def cancel_system_order(self, sys_order_id):
        if not self.check_rate_limit():
            return

        params = {"orderID": sys_order_id}
        self.add_request(
            "DELETE",
            "/api/v1/order",
            callback=self.on_cancel_system_order,
            params=params,
            extra=sys_order_id
        )

    def on_query_order(self, data, request):
        for d in data:
            sys_order_id = str(d["orderID"])
            symbol = change_from_bitmex_to_system(d["symbol"])

            if symbol not in self.set_all_symbols:
                continue

            local_order_id = self.order_manager.get_local_order_id(sys_order_id)
            order = parse_order_info(d, local_order_id, self.gateway_name)
            self.order_manager.on_order(order)

    def on_query_open_orders(self, data, request):
        list_order_ids = list(self.order_manager.get_all_alive_system_id())
        for d in data:
            sys_order_id = str(d["orderID"])
            if sys_order_id in list_order_ids:
                list_order_ids.remove(sys_order_id)
            symbol = change_from_bitmex_to_system(d["symbol"])

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
            order = parse_order_info(d, local_order_id, self.gateway_name)

            self.order_manager.on_order(order)

        if list_order_ids:
            self.query_complete_orders(list_order_ids)

        self.gateway.write_log("order information query success!")

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        self.update_rate_limit(request)

        order = request.extra
        order.status = Status.REJECTED.value
        self.gateway.on_order(order)

        if request.response.text:
            data = request.response.json()
            error = data["error"]
            msg = "Send order failed:{} , type:{}, information:{}".format(status_code, error['name'], error['message'])
        else:
            msg = "Send order failed , status_code:{}".format(status_code)

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
        '''
        on_send_order data:{
            'orderID': 'e8cc080d-387d-4d8d-8096-6489ce8411db', 
            'clOrdID': '201029103707000004', 'clOrdLinkID': '', 
            'account': 1474991, 'symbol': 'XBTUSD', 'side': 'Sell', 
            'simpleOrderQty': None, 'orderQty': 35, 
            'price': 13271.5, 'displayQty': None, 
            'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 
            'currency': 'USD', 'settlCurrency': 'XBt', 
            'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 
            'execInst': '', 'contingencyType': '', 
            'exDestination': 'XBME', 'ordStatus': 'New', 'triggered': '', 
            'workingIndicator': True, 'ordRejReason': '', 'simpleLeavesQty': 
            None, 'leavesQty': 35, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': 
            None, 'multiLegReportingType': 'SingleSecurity', 'text': 'Submitted via API.', 
            'transactTime': '2020-10-29T02:37:19.221Z', 'timestamp': '2020-10-29T02:37:19.221Z'}
        '''
        self.update_rate_limit(request)

        order = request.extra
        self.gateway.write_log("[on_send_order] data:{}".format(data))

        sys_order_id = data["orderID"]
        self.order_manager.update_order_id_map(order.order_id, sys_order_id)

    def on_send_orders(self, data, request):
        self.update_rate_limit(request)

        orders = request.extra
        self.gateway.write_log("[on_send_orders] data:{}".format(data))

        for i in range(len(data)):
            order = orders[i]
            d = data[i]
            sys_order_id = d["orderID"]
            self.order_manager.update_order_id_map(order.order_id, sys_order_id)

    def on_cancel_order_error(self, exception_type, exception_value, tb, request):
        """
        Callback when cancelling order failed on server.
        """
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_orders_error(self, exception_type, exception_value, tb, request):
        """
        Callback when cancelling order failed on server.
        """
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_orders(self, data, request):
        """取消订单"""
        self.update_rate_limit(request)
        self.gateway.write_log("[on_cancel_orders] :{}".format(data))

        # Cancel Orders
        cancel_requests = request.extra
        for cancel_request in cancel_requests:
            local_order_id = cancel_request.order_id
            order = self.order_manager.get_order_with_local_order_id(local_order_id)
            if order:
                self.gateway.write_log("cancel_order success!{}".format(str(order.order_id)))

    def on_cancel_order(self, data, request):
        """Websocket will push a new order status"""
        self.update_rate_limit(request)

        cancel_request = request.extra
        local_order_id = cancel_request.order_id
        order = self.order_manager.get_order_with_local_order_id(local_order_id)
        if order:
            self.gateway.write_log("cancel_order success!{}".format(str(order.order_id)))

    def on_cancel_system_order(self, data, request):
        msg = "on_cancel_system_order , sys_order_id:{}".format(request.extra)
        self.gateway.write_log(msg)

    def on_failed(self, status_code, request):
        """
        Callback to handle request failed.
        """
        self.update_rate_limit(request)

        data = request.response.json()
        error = data["error"]
        msg = "request failed status:{} type:{} information:{}".format(status_code, error['name'], error['message'])
        self.gateway.write_log(msg)

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Callback to handler request exception.
        """
        msg = "touch error, status:{} information:{}".format(exception_type, exception_value)
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb, request))

    def update_rate_limit(self, request):
        """
        Update current request limit remaining status.
        """
        if request.response is None:
            return
        headers = request.response.headers

        self.rate_limit_remaining = int(headers.get("x-ratelimit-remaining", 0))
        self.rate_limit_sleep = int(headers.get("Retry-After", 0))
        if self.rate_limit_sleep:
            self.rate_limit_sleep += 1  # 1 extra second sleep

    def reset_rate_limit(self):
        """
        Reset request limit remaining every 1 second.
        """
        self.rate_limit_remaining += 1
        self.rate_limit_remaining = min(self.rate_limit_remaining, self.rate_limit_limit)

        # Countdown of retry sleep seconds
        if self.rate_limit_sleep:
            self.rate_limit_sleep -= 1

    def check_rate_limit(self):
        """
        Check if rate limit is reached before sending out requests.
        """
        # Already received 429 from server
        if self.rate_limit_sleep:
            msg = "request is too quick, please {} seconds check!".format(self.rate_limit_sleep)
            self.gateway.write_log(msg)
            return False
        # Just local request limit is reached
        elif not self.rate_limit_remaining:
            msg = "request is too quick, please try it later!"
            self.gateway.write_log(msg)
            return False
        else:
            self.rate_limit_remaining -= 1
            return True
