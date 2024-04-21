# encoding: UTF-8

from copy import copy
from collections import defaultdict

from tumbler.function import get_vt_key, split_url, parse_timestamp
from tumbler.function import simplify_tick
from tumbler.constant import Exchange, Direction
from tumbler.service import log_service_manager
from tumbler.object import TickData

from .base_client import BaseClient
from tumbler.gateway.okex.base import sign_request
from tumbler.gateway.okexs.base import REST_TRADE_HOST, parse_contract_info
from tumbler.gateway.okexs.base import parse_account_info, okexs_format_symbol, parse_position_holding
from tumbler.gateway.okexs.base import parse_order_info


class OkexsClient(BaseClient):
    def __init__(self, _apikey, _secret_key, _passphrase, proxy_host="", proxy_port=0):
        super(OkexsClient, self).__init__(_apikey, _secret_key, _passphrase)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.passphrase)

    def get_exchange_info(self):
        data = self.wrap_request(
            "GET",
            "/api/swap/v3/instruments"
        )
        return parse_contract_info(data, Exchange.OKEXS.value)

    def get_ticker(self, symbol, depth=20, u_type="step0"):
        data = self.wrap_request("GET",
                                 "/api/swap/v3/instruments/{}/depth?size={}"
                                 .format(okexs_format_symbol(symbol), depth))
        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(tick.symbol, Exchange.OKEXS.value)
        tick.datetime = parse_timestamp(data["time"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])
        return copy(tick)

    def parse_get_account_list(self, data):
        account_list = []
        for info in data["info"]:
            account = parse_account_info(info, Exchange.OKEXS.value)
            account_list.append(account)
        return account_list

    def get_assets(self):
        data = self.wrap_request("GET", "/api/swap/v3/accounts", data={})
        account_list = self.parse_get_account_list(data)
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        data = self.wrap_request("GET", "/api/swap/v3/accounts", data={})
        account_list = self.parse_get_account_list(data)
        return self.get_available_assets_from_account_arr(account_list)

    def get_order(self, symbol, order_id):
        data = self.wrap_request(
                    "GET",
                    "/api/swap/v3/orders/%s/%s" % (okexs_format_symbol(symbol), order_id),
                )
        order = parse_order_info(data, Exchange.OKEXS.value)
        return order

    def get_open_orders(self, symbol):
        ret_order_list = []
        for path in ["/api/swap/v3/orders/%s?state=0" % (okexs_format_symbol(symbol)),
                     "/api/swap/v3/orders/%s?state=1" % (okexs_format_symbol(symbol))]:
            data = self.wrap_request(
                "GET",
                path,
                params={"instrument_id": okexs_format_symbol(symbol)}
            )
            for order_data in data:
                order = parse_order_info(order_data, Exchange.OKEXS.value)
                ret_order_list.append(order)
        return ret_order_list

    def get_traded_orders(self, symbol):
        ret_order_list = []
        for path in ["/api/swap/v3/orders/%s?state=1" % (okexs_format_symbol(symbol)),
                     "/api/swap/v3/orders/%s?state=2" % (okexs_format_symbol(symbol))]:
            data = self.wrap_request(
                "GET",
                path,
                params={"instrument_id": okexs_format_symbol(symbol)}
            )
            for order_data in data:
                order = parse_order_info(order_data, Exchange.OKEXS.value)
                ret_order_list.append(order)
        return ret_order_list

    def transfer_symbol(self, symbol):
        symbol = symbol.replace('-', '_')
        if symbol.endswith('_usd_swap'):
            symbol = symbol.replace('usd_swap', 'usdt')
        return symbol

    def get_position_info(self):
        data = self.wrap_request(
            "GET",
            "/api/swap/v3/position"
        )
        ret = defaultdict(float)
        for dic in data:
            holdings = dic["holding"]
            for holding in holdings:
                symbol = holding["instrument_id"].lower()
                pos = parse_position_holding(holding, symbol=symbol, gateway_name=Exchange.OKEXS.value)
                symbol = self.transfer_symbol(symbol)
                ret[symbol] += pos.position * (1 if pos.direction == Direction.LONG.value else -1)

        return ret

    def cancel_order(self, symbol, order_id):
        data = {
            "instrument_id": okexs_format_symbol(symbol),
            "order_id": order_id
        }
        path = "/api/swap/v3/cancel_order/{}/{}".format(okexs_format_symbol(symbol), order_id)
        data = self.wrap_request(
            "POST",
            path,
            data=data
        )

        if str(data["result"]) == "True":
            log_service_manager.write_log(
                "OKEXS cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, order_id, data))
            return True
        else:
            code = str(data["error_code"])
            log_service_manager.write_log(
                "OKEXS cancel_order symbol:{}, order_id:{} failed! code:{} , data:{}".format(symbol, order_id, code,
                                                                                             data))
            return False
