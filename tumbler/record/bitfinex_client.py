# encoding: UTF-8

from datetime import datetime
from time import mktime
import hashlib
import requests
import base64
import json
import hmac

from tumbler.function import get_format_lower_symbol, get_format_system_symbol
from tumbler.constant import Direction, Status, Exchange
from tumbler.service import log_service_manager

from .base_client import BaseClient

BITFINEX_MARKET_URL = "api.bitfinex.com"


class BitfinexClient(BaseClient):

    @property
    def _nonce(self):
        if self.count_nonce == 0:
            self.count_nonce = int(mktime(datetime.now().timetuple())) * 1000
        else:
            self.count_nonce += 1

        return str(self.count_nonce)

    def _sign_payload(self, payload):
        j = json.dumps(payload)
        data = base64.standard_b64encode(j.encode('utf8'))

        h = hmac.new(self.secret_key.encode('utf8'), data, hashlib.sha384)
        signature = h.hexdigest()
        return {
            "X-BFX-APIKEY": self.api_key,
            "X-BFX-SIGNATURE": signature,
            "X-BFX-PAYLOAD": data
        }

    def http_get_request(self, url, resource, params, add_to_headers=None):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        if add_to_headers:
            headers.update(add_to_headers)
        try:
            response = requests.get(resource + '?' + params)
            return response.json()
        except Exception as ex:
            log_service_manager.write_log("httpGet failed, detail is:{}".format(ex))
            return {"status": "fail", "message": ex, "code": "ch6"}

    def http_post_request(self, url, resource, params):
        try:
            signed_payload = self._sign_payload(params)
            r = requests.post("https://" + url + resource, headers=signed_payload, verify=True)
            if r.status_code is 200:
                return r.json()
            else:
                return r.json()
        except Exception as ex:
            log_service_manager.write_log("request failed, detail is:{}".format(ex))
            return {"status": "fail", "message": ex, "code": "bf7"}

    def get_exchange_info(self):
        s = self.url_get("https://api.bitfinex.com/v1/symbols_details")
        ret_info = {}
        try:
            data = json.loads(s)
            for info in data:
                pre_symbol_pair = info["pair"]
                price_precision = info["price_precision"]
                minimum_order_size = info["minimum_order_size"]

                symbol_pair = get_format_lower_symbol(pre_symbol_pair).lower()
                ret_info[symbol_pair] = {"price_precision": price_precision, "min_volume": minimum_order_size}
        except Exception as ex:
            log_service_manager.write_log(ex)
        return ret_info

    @staticmethod
    def deal_bitfinex_order_books(data):
        bids = data["bids"]
        asks = data["asks"]

        bids_data = []
        asks_data = []

        for dic in bids:
            bids_data.append([dic["price"], dic["amount"]])
        for dic in asks:
            asks_data.append([dic["price"], dic["amount"]])

        return BaseClient.deal_order_books(asks_data, bids_data)

    def get_price(self, symbol):
        symbol = get_format_system_symbol(symbol).upper()
        url = "https://api.bitfinex.com/v1/pubticker/{}".format(symbol)
        data = self.url_get(url)
        ret_price = float(data["last_price"])
        return ret_price

    def get_order_book(self, symbol):
        ret_dic = {}
        try:
            symbol = get_format_system_symbol(symbol).upper()
            url = "https://api.bitfinex.com/v1/book/{}".format(symbol)
            data = self.url_get(url)
            ret_dic = self.deal_bitfinex_order_books(data)
        except Exception as ex:
            log_service_manager.write_log(ex)
        return ret_dic

    def get_assets(self):
        ret_dic = {}
        try:
            kwargs = {
                "request": "/v1/balances",
                "nonce": self._nonce
            }
            data = self.http_post_request(BITFINEX_MARKET_URL, "/v1/balances", kwargs)
            for dic in data:
                available = dic["available"]
                symbol = dic["currency"].lower()
                amount = dic["amount"]
                use_exchange_type = dic["type"]

                if use_exchange_type == "exchange":
                    ret_dic[symbol] = float(amount)

            return ret_dic
        except Exception as ex:
            return {}

    def get_available_assets(self):
        ret_dic = {}
        try:
            kwargs = {
                "request": "/v1/balances",
                "nonce": self._nonce
            }
            data = self.http_post_request(BITFINEX_MARKET_URL, "/v1/balances", kwargs)
            for dic in data:
                available = dic["available"]
                symbol = dic["currency"].lower()
                amount = dic["amount"]
                use_exchange_type = dic["type"]
                # 做现货账户过滤
                if use_exchange_type == "exchange":
                    ret_dic[symbol] = float(available)

            return ret_dic
        except Exception as ex:
            return {}

    def get_open_orders(self, symbol):
        try:
            ret_order_list = []
            kwargs = {
                "request": "/v1/orders",
                "nonce": self._nonce
            }
            data = self.http_post_request(BITFINEX_MARKET_URL, "/v1/orders", kwargs)

            for use_order in data:
                use_dt, use_date, now_time = self.generate_date_time(use_order["timestamp"])

                is_cancelled = use_order["is_cancelled"]
                side = use_order["side"]

                direction = Direction.LONG.value
                if side == "sell":
                    direction = Direction.SHORT.value

                order_status = Status.NOTTRADED.value
                if is_cancelled:
                    status = Status.CANCELLED.value
                else:
                    if float(use_order["executed_amount"]) > 0:
                        if float(use_order["remaining_amount"]) > 0:
                            status = Status.PARTTRADED.value
                        else:
                            status = Status.ALLTRADED.value
                    else:
                        status = Status.NOTTRADED.value

                ret_order = {}
                ret_order["symbol"] = get_format_lower_symbol(use_order["symbol"])

                if ret_order["symbol"] == symbol:
                    ret_order["order_id"] = str(use_order["id"])
                    ret_order["exchange"] = Exchange.BITFINEX.value
                    ret_order["direction"] = direction
                    ret_order["price"] = float(use_order["price"])
                    ret_order["total_volume"] = float(use_order["original_amount"])
                    ret_order["traded_volume"] = float(use_order["executed_amount"])
                    ret_order["status"] = order_status
                    ret_order["orderTime"] = use_date + " " + now_time
                    ret_order["cancelTime"] = ""

                    ret_order_list.append(ret_order)

            return ret_order_list

        except Exception as ex:
            return []

    def get_traded_orders(self, symbol):
        try:
            ret_order_list = []
            symbol = get_format_system_symbol(symbol)
            kwargs = {
                "request": "/v1/mytrades",
                "nonce": self._nonce,
                "symbol": symbol,
                "limit_trades": 50
            }
            data = self.http_post_request(BITFINEX_MARKET_URL, "/v1/mytrades", kwargs)
            for trade_dic in data:
                use_dt, use_date, now_time = self.generate_date_time(trade_dic["timestamp"])

                ret_order = {}
                ret_order["symbol"] = get_format_lower_symbol(symbol)
                ret_order["order_id"] = str(trade_dic["order_id"])
                ret_order["exchange"] = Exchange.BITFINEX.value

                direction = Direction.LONG.value
                if trade_dic["type"] == "Sell":
                    direction = Direction.SHORT.value
                ret_order["direction"] = direction
                ret_order["price"] = float(trade_dic["price"])
                ret_order["total_volume"] = float(trade_dic["amount"])
                ret_order["traded_volume"] = float(trade_dic["amount"])
                ret_order["status"] = Status.ALLTRADED.value
                ret_order["orderTime"] = use_date + " " + now_time
                ret_order["cancelTime"] = ""

                ret_order_list.append(ret_order)
            return ret_order_list
        except Exception as ex:
            return []