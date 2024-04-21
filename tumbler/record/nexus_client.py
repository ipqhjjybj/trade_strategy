# coding=utf-8

from tumbler.object import AccountData
from tumbler.function import split_url
from tumbler.constant import Exchange
from tumbler.function import get_vt_key
from tumbler.record.base_client import BaseClient
from tumbler.gateway.nexus.base import sign_request, REST_TRADE_HOST
from tumbler.gateway.nexus.base import nexus_format_symbol
from tumbler.gateway.nexus.base import parse_account_info
from tumbler.gateway.nexus.base import parse_contract_info

# api_key = "SBvZfcpvt9n7uSla6aBYaw=="
# secret_key = "PSWD6AREF3MDDBPQHGFWB7WQDN5R"


class NexusClient(BaseClient):
    def __init__(self, _api_key, _secret_key, proxy_host="", proxy_port=0):
        super(NexusClient, self).__init__(_api_key, _secret_key)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def sign(self, request):
        """
        Generate NEXUS signature.
        """
        return sign_request(request)

    def get_ticker(self, symbol):
        data = self.wrap_request(
            "GET",
            "/v1/orderbook/{}".format(nexus_format_symbol(symbol))
        )
        return data

    def get_exchange_info(self):
        data = self.wrap_request(
            "GET",
            "/v1/public/info",
            params={}
        )
        ret = []
        for d in data["rows"]:
            contract = parse_contract_info(d, Exchange.NEXUS.value)
            ret.append(contract)
        return ret

    def get_accounts(self):
        ret = []
        data = self.wrap_request(
            "GET",
            "/v1/client/info",
            params={},
            data={}
        )
        account = parse_account_info(data, Exchange.NEXUS.value)
        account.account_id = "usdt"
        account.vt_account_id = get_vt_key(account.account_id, Exchange.NEXUS.value)
        account.gateway_name = Exchange.NEXUS.value
        ret.append(account)

        data = self.wrap_request(
            "GET",
            "/v2/client/holding",
            params={}
        )
        for d in data["holding"]:
            account = AccountData()
            account.account_id = d["token"].lower()
            account.vt_account_id = get_vt_key(account.account_id, Exchange.NEXUS.value)
            account.gateway_name = Exchange.NEXUS.value
            ret.append(data)

        return ret

    def get_orders(self):
        data = self.wrap_request(
            "GET",
            "/v1/orders",
            params={
                "symbol": "SPOT_BTC_USDT"
            },
            data={}
        )
        return data

    def send_order(self, req):
        price = "9400"
        order_quantity = "0.002"
        order_type = "LIMIT"
        param1 = "val1"
        param2 = "val2"
        side = "BUY"
        symbol = "SPOT_BTC_USDT"
        data = self.wrap_request(
            "POST",
            "/v1/order",
            params={
                "param1": param1,
                "param2": param2
            },
            data={
                "symbol": symbol,
                "order_type": order_type,
                "order_price": price,
                "order_quantity": order_quantity,
                "side": side
            }
        )
        return data

    def cancel_order(self, order_id, symbol):
        params = {
            "order_id": str(order_id),
            "symbol": symbol
        }
        data = self.wrap_request(
            "DELETE",
            "/v1/order",
            data=params
        )
        return data

    def get_traded_orders(self, symbol):
        params = {
        }
        data = self.wrap_request(
            "GET",
            "/v1/client/trades",
            data=params
        )
        return data

    def get_current_holding(self):
        params = {
        }
        data = self.wrap_request(
            "GET",
            "/v2/client/holding"
        )
        return data


# client = NexusClient(api_key, secret_key)
# # client.get_symbols()
# # print(client.get_account_info())
# print(client.send_order())
# # data = client.get_orders()
# # for dic in data["rows"]:
# #     print(dic)
# # print(client.cancel_order("81268", "SPOT_BTC_USDT"))
# # print(client.get_traded_orders())
# print(client.get_current_holding())
#
# print(client.get_ticker("BTC_USDT"))
