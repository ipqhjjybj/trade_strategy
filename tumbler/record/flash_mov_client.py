# encoding: UTF-8

import requests
import json

from tumbler.service.log_service import log_service_manager
from tumbler.constant import Exchange
from tumbler.gateway.movflash.base import REST_MARKET_HOST
from tumbler.gateway.mov.base import mov_format_symbol

from .mov_client import MovClient


class MMFlashClient(object):
    __headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
    }

    def __init__(self, _guid, _secret_key, _local_url_arr=["http://127.0.0.1:1024"]):
        self.guid = _guid
        self.local_url_arr = _local_url_arr
        self.secret_key = _secret_key

        self.mov_client = MovClient(self.guid, self.secret_key, use_decrypt=False)

    def _request(self, method, url, param):
        try:
            method = method.upper()
            if method in ["GET"]:
                result = requests.get(url, params=param, headers=self.__headers, timeout=5)
            else:
                encoded_data = json.dumps(param).encode('utf-8')
                result = requests.post(url, data=encoded_data, headers=self.__headers, timeout=5)

            if result:
                return result.json()
            else:
                return None
        except Exception as ex:
            log_service_manager.write_log("[Error] MMFlashClient _request:{}".format(ex))

    def get_depth(self, symbol):
        url = REST_MARKET_HOST + "/api/v1/market-depth?symbol={}".format(mov_format_symbol(symbol))
        data = self._request("GET", url, {})
        return data

    def send_order(self, symbol, side, price, amount):
        ret_arr = []
        for local_url in self.local_url_arr:
            url = local_url + "/api/v1/place-order"
            params = {"symbol": mov_format_symbol(symbol), "side": side, "price": str(price), "amount": str(amount)}
            data = self._request("POST", url, params)
            log_service_manager.write_log("[send_order] url:{} data:{}".format(local_url, data))
            ret_arr.append(local_url)
        return ret_arr

    def cancel_order(self, symbol, side):
        ret_arr = []
        for local_url in self.local_url_arr:
            url = local_url + "/api/v1/cancel-order"
            params = {"symbol": mov_format_symbol(symbol), "side": side}
            data = self._request("GET", url, params)
            log_service_manager.write_log("[cancel_order] url:{} data:{}".format(local_url, data))
            ret_arr.append(local_url)
        return ret_arr

    def query_contract(self, exchange=Exchange.FLASH.value, gateway_name=Exchange.FLASH.value):
        return self.mov_client.get_exchange_info(exchange, gateway_name)

    def query_balance(self, gateway_name=Exchange.FLASH.value):
        return self.mov_client.get_accounts(gateway_name)

    def inside_transfer(self, asset, amount, to_address):
        return self.mov_client.inside_transfer(asset, amount, to_address)
