# encoding: UTF-8

from copy import copy
from datetime import datetime

from tumbler.function import split_url
from tumbler.function import get_vt_key, simplify_tick
from tumbler.constant import Exchange
from tumbler.object import TickData
from tumbler.service import log_service_manager
from tumbler.gateway.binance.base import parse_contract_info, REST_TRADE_HOST, Security, parse_account_info_arr
from tumbler.gateway.binance.base import change_system_format_to_binance_format, sign_request

from .base_client import BaseClient


class BinanceClient(BaseClient):
    def __init__(self, _apikey, _secret_key, _passphrase="", proxy_host="", proxy_port=0):
        super(BinanceClient, self).__init__(_apikey, _secret_key, _passphrase)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def get_exchange_name(self):
        return Exchange.BINANCE.value

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.host)

    def get_ticker(self, symbol, depth=20, u_type="step0"):
        data = self.wrap_request(
            method="GET",
            path="/api/v1/depth?symbol={}&limit={}".format(change_system_format_to_binance_format(symbol), str(depth))
        )

        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(symbol, Exchange.BINANCE.value)
        tick.datetime = datetime.now()
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])
        return copy(tick)

    def get_price(self, symbol):
        ticker = self.get_ticker(symbol)
        return ticker.last_price

    def get_exchange_info(self):
        data = {
            "security": Security.NONE.value
        }
        data = self.wrap_request(
            method="GET",
            path="/api/v1/exchangeInfo",
            data=data
        )
        ret = []
        if data:
            for d in data["symbols"]:
                contract = parse_contract_info(d, Exchange.BINANCE.value, Exchange.BINANCE.value)
                ret.append(contract)
        return ret

    def get_balance(self):
        data = {"security": Security.SIGNED.value}
        return self.wrap_request(
            method="GET",
            path="/api/v3/account",
            data=data
        )

    def get_assets(self):
        data = self.get_balance()
        account_list = parse_account_info_arr(data, Exchange.BINANCE.value)
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        data = self.get_balance()
        account_list = parse_account_info_arr(data, Exchange.BINANCE.value)
        return self.get_available_assets_from_account_arr(account_list)


