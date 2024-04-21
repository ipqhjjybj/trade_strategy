# encoding: UTF-8

from datetime import datetime
import time
import hashlib
import requests
import json
from copy import copy

from tumbler.function import split_url
from tumbler.constant import Exchange
from tumbler.gateway.bitmex.base import REST_TRADE_HOST, sign_request, parse_order_info

from tumbler.gateway.bitmex.base import change_from_system_to_bitmex, parse_ticker
from tumbler.object import TickData
from tumbler.service import log_service_manager
from tumbler.gateway.bitmex.base import DIRECTION_VT2BITMEX
from tumbler.gateway.bitmex.base import ORDER_TYPE_VT2BITMEX

from .base_client import BaseClient


class BitmexClient(BaseClient):
    def __init__(self, _apikey, _secret_key, proxy_host="", proxy_port=0):
        super(BitmexClient, self).__init__(_apikey, _secret_key)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key)

    def get_ticker(self, symbol, depth=20):
        bitmex_symbol = change_from_system_to_bitmex(symbol)
        data = self.wrap_request(
            method="GET",
            path="/api/v1/orderBook/L2?symbol={}&depth={}".format(bitmex_symbol, depth)
        )
        tick = TickData()
        tick = parse_ticker(tick, data)
        return copy(tick)

    def get_open_orders(self, symbol):
        ret_orders = []
        data = {
                'filter': json.dumps(
                    {
                        'ordStatus.isTerminated': False,
                        'symbol': change_from_system_to_bitmex(symbol)
                    }
                ),
                'count': 500
        }
        ret_data = self.wrap_request(
            "GET",
            "/api/v1/order",
            params=data
        )
        print(ret_data)
        for d in ret_data:
            order = parse_order_info(d, d["orderID"], Exchange.BITMEX.value)
            ret_orders.append(order)
        return ret_orders

    def get_order(self, order_id):
        ret_orders = []
        # data = {
        #     "filter": "{\"orderID\":\"%s\"}" % order_id
        # }
        data = {
            "filter": json.dumps({
                    "orderID": order_id
                }
            )
        }
        print(data)
        ret_data = self.wrap_request(
            "GET",
            "/api/v1/order",
            params=data
        )
        #print(ret_data)
        return ret_data

    def get_traded_orders(self, symbol):
        for ordStatus in ["Partially filled", "Filled", "Canceled"]:
            data = {
                "symbol": change_from_system_to_bitmex(symbol),
                "ordStatus": ordStatus
            }
            ret_data = self.wrap_request(
                "GET",
                "/api/v1/order",
                data=data
            )
            print(ret_data)

        return []

    def get_exchange_info(self):
        return {}

    def get_no_position_assets(self):
        return {}

    def get_assets(self, contract_pairs=[]):
        return {}

    def get_available_assets(self):
        return {}

    def cancel_order(self, symbol, order_id):
        if isinstance(order_id, list):
            data = {"orderID": json.dumps(order_id)}
        else:
            #data = {"orderID": json.dumps(order_id)}
            data = {"orderID": order_id}

        data = self.wrap_request(
            "DELETE",
            "/api/v1/order",
            data=data
        )
        log_service_manager.write_log("[cancel order] data:{}".format(data))
        for d in data:
            order_id = d["orderID"]
            log_service_manager.write_log(
                "BITMEX cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, order_id, d))
            return True

    def send_orders(self, req_lists):
        '''
        symbol, exchange, direction, price, volume
        '''
        order_dicts = []
        for symbol, exchange, direction, price, volume in req_lists:
            order_dic = {
                'symbol': change_from_system_to_bitmex(symbol),
                'orderQty': int(volume),
                'price': price,
                "side": DIRECTION_VT2BITMEX[direction],
                "ordType": "Limit",
                "execInst": ",".join(["ParticipateDoNotInitiate"])
            }
            order_dicts.append(order_dic)

        data = {
            "orders": json.dumps(order_dicts)
        }
        print(data)

        data = self.wrap_request(
            "POST",
            "/api/v1/order/bulk",
            data=data
        )
        log_service_manager.write_log("[send orders] data:{}".format(data))
        return data



