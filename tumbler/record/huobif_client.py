# encoding: UTF-8

from copy import copy

from tumbler.function import get_vt_key, simplify_tick, get_dt_use_timestamp, split_url
from tumbler.constant import Exchange
from tumbler.object import TickData

from tumbler.gateway.huobi.base import sign_request
from tumbler.gateway.huobif.base import REST_TRADE_HOST, get_huobi_future_system_format_symbol
from tumbler.gateway.huobif.base import parse_contract_info, parse_account_info

from .huobi_client import HuobiClient


class HuobifClient(HuobiClient):
    def __init__(self, _apikey, _secret_key, proxy_host="", proxy_port=0, account_id=None):
        super(HuobifClient, self).__init__(_apikey, _secret_key)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

        self.account_id = account_id

    def get_exchange_name(self):
        return Exchange.HUOBIF.value

    def get_exchange_info(self):
        data = self.wrap_request(
            method="GET",
            path="/api/v1/contract_contract_info"
        )
        ret = []
        for d in data["data"]:
            contract = parse_contract_info(d, self.get_exchange_name())
            ret.append(contract)
        return ret

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.host)

    def get_ticker(self, symbol, depth=20, u_type="step0"):
        data = self.wrap_request("GET",
                                 "/market/depth?symbol={}&type=step5"
                                 .format(get_huobi_future_system_format_symbol(symbol), u_type, str(depth)))
        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(symbol, self.get_exchange_name())
        tick.datetime = get_dt_use_timestamp(data["ts"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["tick"]["bids"], data["tick"]["asks"])
        return copy(tick)

    def get_accounts(self):
        return self.wrap_request(method="POST", path="/api/v1/contract_account_info")

    def get_position_info(self):
        return self.wrap_request(method="POST", path="/api/v1/contract_position_info")

    def parse_account_list(self, data):
        account_list = []
        if data:
            for d in data["data"]:
                account = parse_account_info(d, self.get_exchange_name())
                account_list.append(account)
        return account_list

    def get_assets(self):
        account_list = self.parse_account_list(self.get_accounts())
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        account_list = self.parse_account_list(self.get_accounts())
        return self.get_available_assets_from_account_arr(account_list)



