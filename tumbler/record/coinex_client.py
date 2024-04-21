# encoding: UTF-8

from datetime import datetime
from time import sleep, mktime
import time
import hashlib
import requests
import base64
import json
import hmac

from tumbler.function import get_format_lower_symbol, get_format_system_symbol, urlencode, get_str_dt_use_timestamp
from tumbler.constant import Direction, Status, Exchange
from tumbler.service import log_service_manager

from .base_client import BaseClient

COINEX_MARKET_URL = "api.coinex.com"


class CoinexClient(BaseClient):
    __headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
    }

    def get_sign(self, params):
        sort_params = sorted(params)
        data = []
        for item in sort_params:
            data.append(item + '=' + str(params[item]))
        str_params = "{0}&secret_key={1}".format('&'.join(data), str(self.secret_key))
        token = hashlib.md5(str_params).hexdigest().upper()
        return token

    def set_authorization(self, params):
        params['access_id'] = str(self.api_key)
        params['tonce'] = int(time.time() * 1000)
        self.headers['AUTHORIZATION'] = self.get_sign(params)

    # 各种请求,获取数据方式
    def http_get_request(self, url, resource, params, add_to_headers=None):
        headers = self.__headers
        if add_to_headers:
            headers.update(add_to_headers)
        try:
            data = requests.get(resource + '?' + params, headers=headers)
            return data.json()
        except Exception as ex:
            log_service_manager.write_log("httpGet failed, detail is:{}".format(ex))
            return {"status": "fail", "message": ex, "code": "ch6"}

    def request(self, method, url, params={}, data='', json_params={}):
        method = method.upper()
        try:
            if method == 'GET':
                self.set_authorization(params)
                result = requests.request('GET', url, params=params, headers=self.headers)
            elif method == "POST":
                if data:
                    json_params.update(json.loads(data))
                self.set_authorization(json_params)

                result = requests.request(method, url, json=json_params, headers=self.headers)
            elif method == "DELETE":
                self.set_authorization(json_params)
                temp_params = urlencode(json_params) if json_params else ''
                real_url = url + "?" + temp_params
                result = requests.request('DELETE', real_url, headers=self.headers)
            else:
                log_service_manager.write_log("Error in vncoinex.request , error method {}".format(str(method)))

            if result.status_code == 200:
                return result.json()
            else:
                log_service_manager.write_log("request failed , response is {}".format(str(result.status_code)))
                return result.json()

        except Exception as ex:
            log_service_manager.write_log("request failed, detail is:{}".format(ex))
            return {"status": "fail", "message": ex, "code": "ch7"}

    def get_price(self, symbol):
        symbol = ''.join(symbol.lower().split('_'))
        url = "https://api.coinex.com/v1/market/ticker?market={}".format(symbol)
        data = self.url_get(url)
        ret_price = float(data["data"]["ticker"]["last"])
        return ret_price

    def get_order_book(self, symbol):
        ret_dic = {}
        try:
            url = "https://api.coinex.com/v1/market/depth?market={}&limit=5&merge=0".format(
                get_format_system_symbol(symbol).lower())
            data = self.url_get(url)
            ret_dic = self.deal_order_books(data["data"]["asks"], data["data"]["bids"])
        except Exception as ex:
            log_service_manager.write_log(ex)
        return ret_dic

    def get_assets(self):
        try:
            real_url = "https://" + COINEX_MARKET_URL + "/v1/balance/"
            params = {}
            self.set_authorization(params)
            data = self.request("GET", real_url, params=params)
            data_info = data["data"]

            ret_dict = {}
            for symbol in data_info.keys():
                dic = data_info[symbol]
                ret_dict[symbol.lower()] = float(dic["available"]) + float(dic["frozen"])
            return ret_dict

        except Exception as ex:
            log_service_manager.write_log(ex)
            return {}

    def get_exchange_info(self):
        ret_info = {}
        try:
            data = self.url_get("https://api.coinex.com/v1/market/list")

            data = data["data"]
            for symbol in data:
                symbol = symbol.lower()
                symbol = get_format_lower_symbol(symbol)

                min_amount = 0
                # coinex没有什么限制， 这边就自己给加下限制了
                if "_btc" in symbol:
                    min_amount = 0.001
                elif "_bch" in symbol:
                    min_amount = 0.001
                elif "_eth" in symbol:
                    min_amount = 0.01
                elif "_usdt" in symbol:
                    min_amount = 10

                ret_info[symbol] = {"minPrice": 0.0, "minQty": 0.0, "min_amount": min_amount}
        except Exception as ex:
            log_service_manager.write_log(ex)
        return ret_info

    def get_available_assets(self):
        try:
            real_url = "https://" + COINEX_MARKET_URL + "/v1/balance/"
            params = {}
            self.set_authorization(params)
            data = self.request("GET", real_url, params=params)
            data_info = data["data"]

            ret_dict = {}
            for symbol in data_info.keys():
                dic = data_info[symbol]
                ret_dict[symbol.lower()] = float(dic["available"])
            return ret_dict

        except Exception as ex:
            log_service_manager.write_log(ex)
            return {}

    def cancel_order(self, symbol, order_id):
        try:
            real_url = "https://" + COINEX_MARKET_URL + "/v1/order/pending"
            params = {
                "market": get_format_system_symbol(symbol).upper(),
                "id": str(order_id)
            }
            data = self.request("DELETE", real_url, json_params=params)
            if str(data["code"]) == "0":
                log_service_manager.write_log("cancel symbol:{} order_id:{} succeeily".format(symbol, order_id))
                return True
            else:
                log_service_manager.write_log("cancel symbol:{} order_id:{} failed!".format(symbol, order_id))
                return False

        except Exception as ex:
            log_service_manager.write_log(ex)
            return False

    def parse_orders(self, orders):
        ret_order_list = []
        for use_order in orders:
            use_dt, use_date, now_time = self.generate_date_time(use_order["create_time"])

            direction = Direction.LONG.value
            if use_order["type"] == "sell":
                direction = Direction.SHORT.value

            status = use_order["status"]
            order_status = "UNKNOWN"
            if status == "done":
                if float(use_order["deal_amount"]) < float(use_order["amount"]):
                    order_status = Status.CANCELLED.value
                else:
                    order_status = Status.ALLTRADED.value
            elif status in ["cancel"]:
                order_status = Status.CANCELLED.value
            elif status == "part_deal":
                order_status = Status.PARTTRADED.value
            elif status == "not_deal":
                order_status = Status.NOTTRADED.value

            ret_order = {}
            ret_order["symbol"] = get_format_lower_symbol(use_order["market"])
            ret_order["order_id"] = use_order["id"]
            ret_order["exchange"] = Exchange.COINEX.value
            ret_order["direction"] = direction
            ret_order["price"] = float(use_order["price"])
            ret_order["total_volume"] = float(use_order["deal_amount"])
            ret_order["traded_volume"] = float(use_order["amount"])
            ret_order["status"] = order_status
            ret_order["orderTime"] = use_date + " " + now_time
            ret_order["cancelTime"] = ""

            ret_order_list.append(ret_order)

        return ret_order_list

    def get_open_orders(self, symbol):
        kwargs = {
            "market": get_format_system_symbol(symbol).upper(),
            "page": "1",
            "limit": "100"
        }
        real_url = "https://" + COINEX_MARKET_URL + "/v1/order/pending"
        data = self.request("GET", real_url, params=kwargs)

        data_info = data["data"]
        orders = data_info["data"]

        return self.parse_orders(orders)

    def get_traded_orders(self, symbol):
        kwargs = {
            "market": get_format_system_symbol(symbol).upper(),
            "page": "1",
            "limit": "100"
        }
        real_url = "https://" + COINEX_MARKET_URL + "/v1/order/finished"
        data = self.request("GET", real_url, params=kwargs)

        data_info = data["data"]
        orders = data_info["data"]

        return self.parse_orders(orders)

    def generate_date_time(self, s):
        """生成时间"""
        dt = datetime.fromtimestamp(float(s))
        time = dt.strftime("%H:%M:%S")
        date = dt.strftime("%Y-%m-%d")
        return dt, date, time
