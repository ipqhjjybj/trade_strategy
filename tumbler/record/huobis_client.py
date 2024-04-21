# encoding: UTF-8

from time import sleep
from copy import copy

from tumbler.function import get_vt_key, simplify_tick, get_dt_use_timestamp, split_url
from tumbler.constant import Exchange
from tumbler.object import TickData

from tumbler.gateway.huobi.base import sign_request
from tumbler.gateway.huobis.base import REST_TRADE_HOST, get_huobi_future_system_format_symbol
from tumbler.gateway.huobis.base import parse_order_info, parse_contract_info, parse_account_info

from .huobi_client import HuobiClient


class HuobisClient(HuobiClient):
    def __init__(self, _apikey, _secret_key, proxy_host="", proxy_port=0, account_id=None):
        super(HuobisClient, self).__init__(_apikey, _secret_key)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

        self.account_id = account_id

    def get_exchange_name(self):
        return Exchange.HUOBIS.value

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.host)

    def get_exchange_info(self):
        data = self.wrap_request(
            method="GET",
            path="/swap-api/v1/swap_contract_info"
        )
        ret = []
        for d in data["data"]:
            contract = parse_contract_info(d, Exchange.HUOBIS.value)
            ret.append(contract)
        return ret

    def get_ticker(self, symbol, depth=20, u_type="step0"):
        data = self.wrap_request("GET",
                                 "/swap-ex/market/depth?contract_code={}&type={}"
                                 .format(get_huobi_future_system_format_symbol(symbol), u_type, str(depth)))
        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(symbol, Exchange.HUOBIS.value)
        tick.datetime = get_dt_use_timestamp(data["ts"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["tick"]["bids"], data["tick"]["asks"])
        return copy(tick)

    def get_accounts(self):
        return self.wrap_request(method="POST", path="/swap-api/v1/swap_account_info")

    def parse_account_list(self, data):
        account_list = []
        if data:
            for d in data["data"]:
                account = parse_account_info(d, Exchange.HUOBIS.value)
                account_list.append(account)
        return account_list

    def get_assets(self):
        account_list = self.parse_account_list(self.get_accounts())
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        account_list = self.parse_account_list(self.get_accounts())
        return self.get_available_assets_from_account_arr(account_list)



