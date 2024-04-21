# encoding: UTF-8

from tumbler.function import split_url, get_vt_key
from tumbler.constant import Exchange
from tumbler.gateway.binance.base import sign_request, Security
from tumbler.gateway.binancef.base import REST_TRADE_HOST, parse_contract_arr
from tumbler.object import PositionData, Direction, AccountData
from tumbler.gateway.binancef.base import change_binance_format_to_system_format, asset_from_other_exchanges_to_binance
from .base_client import BaseClient


def parse_positions(data):
    ret = []
    for d in data:
        pos = PositionData()
        pos.symbol = change_binance_format_to_system_format(d["symbol"])
        pos.exchange = Exchange.BINANCEF.value
        pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
        pos.direction = Direction.NET.value
        pos.position = float(d["positionAmt"])
        pos.frozen = 0
        pos.vt_position_id = get_vt_key(pos.vt_symbol, Direction.NET.value)
        pos.price = float(d["entryPrice"])
        if pos.position:
            ret.append(pos)
    return ret


def parse_account(data, gateway_name):
    ret = []
    for asset in data["assets"]:
        account = AccountData()
        account.account_id = asset_from_other_exchanges_to_binance(asset["asset"].lower())
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(asset["marginBalance"])
        account.frozen = float(asset["maintMargin"])
        account.available = float(asset["walletBalance"])
        account.gateway_name = gateway_name

        ret.append(account)
    return ret


class BinancefClient(BaseClient):

    def __init__(self, _apikey, _secret_key, proxy_host="", proxy_port=0):
        super(BinancefClient, self).__init__(_apikey, _secret_key)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.host)

    def get_exchange_name(self):
        return Exchange.BINANCEF.value

    def get_exchange_info(self):
        data = self.wrap_request(
            method="GET",
            path="/fapi/v1/exchangeInfo",
            params={}
        )
        if data and data.get("symbols", []):
            return parse_contract_arr(data, Exchange.BINANCEF.value, Exchange.BINANCEF.value)
        return []

    def get_accounts(self):
        data = {"security": Security.SIGNED.value}
        return self.wrap_request(
            method="GET",
            path="/fapi/v1/account",
            data=data
        )

    def get_assets(self):
        account_arr = parse_account(self.get_accounts(), Exchange.BINANCEF.value)
        dic = {}
        for account in account_arr:
            dic[account.vt_account_id] = account.balance
        return dic

    def query_positions(self):
        data = {"security": Security.SIGNED.value}
        return self.wrap_request(
            method="GET",
            path="/fapi/v1/positionRisk",
            data=data
        )

    def get_position_info(self):
        dic = {}
        positions = parse_positions(self.query_positions())
        for position in positions:
            dic[position.vt_symbol] = position.position
        return dic


