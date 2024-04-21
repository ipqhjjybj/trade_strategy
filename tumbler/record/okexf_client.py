# encoding: UTF-8

from copy import copy
from collections import defaultdict

from tumbler.function import split_url, get_no_under_lower_symbol, parse_timestamp
from tumbler.function import simplify_tick, get_vt_key
from tumbler.constant import Exchange, Direction
from tumbler.service import log_service_manager
from tumbler.object import TickData

from tumbler.gateway.okex.base import sign_request
from tumbler.gateway.okexf.base import REST_TRADE_HOST, okexf_format_symbol, parse_contract_info
from tumbler.gateway.okexf.base import parse_account_info, parse_order_info, parse_position_info
from .base_client import BaseClient


class OkexfClient(BaseClient):
    def __init__(self, _apikey, _secret_key, _passphrase, proxy_host="", proxy_port=0):
        super(OkexfClient, self).__init__(_apikey, _secret_key, _passphrase)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.passphrase)

    def get_exchange_info(self):
        data = self.wrap_request(
            "GET",
            "/api/futures/v3/instruments"
        )
        return parse_contract_info(data, Exchange.OKEXF.value)

    def get_ticker(self, symbol, depth=20):
        data = self.wrap_request("GET",
                                 "/api/futures/v3/instruments/{}/book?size={}".format(
                                     okexf_format_symbol(symbol), str(depth)))
        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(symbol, Exchange.OKEXF.value)
        tick.datetime = parse_timestamp(data["timestamp"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])
        return copy(tick)

    def get_assets(self):
        data = self.wrap_request("GET", "/api/futures/v3/accounts", data={})
        account_list = parse_account_info(data, Exchange.OKEXF.value)
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        data = self.wrap_request("GET", "/api/futures/v3/accounts", data={})
        account_list = parse_account_info(data, Exchange.OKEXF.value)
        return self.get_available_assets_from_account_arr(account_list)

    def get_open_orders(self, symbol):
        ret_order_list = []
        for path in ["/api/futures/v3/orders/%s?state=0" % (okexf_format_symbol(symbol)),
                     "/api/futures/v3/orders/%s?state=1" % (okexf_format_symbol(symbol))]:
            data = self.wrap_request(
                "GET",
                path,
                params={"instrument_id": okexf_format_symbol(symbol)}
            )
            for order_data in data:
                order = parse_order_info(order_data, Exchange.OKEXF.value)
                ret_order_list.append(order)
        return ret_order_list

    def get_position_info(self):
        position_dic = defaultdict(float)
        data = self.wrap_request(
            "GET",
            "/api/futures/v3/position"
        )

        ret_positions = parse_position_info(data, [])
        for pos in ret_positions:
            position_dic[pos.symbol] += pos.position * (1 if pos.direction == Direction.LONG.value else -1)
        return ret_positions

    def get_traded_orders(self, symbol):
        ret_order_list = []
        for path in ["/api/futures/v3/orders/%s?state=1" % (okexf_format_symbol(symbol)),
                     "/api/futures/v3/orders/%s?state=2" % (okexf_format_symbol(symbol))]:
            data = self.wrap_request(
                "GET",
                path,
                params={"instrument_id": okexf_format_symbol(symbol)}
            )
            for order_data in data:
                order = parse_order_info(order_data, Exchange.OKEXF.value)
                ret_order_list.append(order)
        return ret_order_list

    def cancel_order(self, symbol, order_id):
        path = "/api/futures/v3/cancel_order/{}/{}".format(okexf_format_symbol(symbol), order_id)
        data = self.wrap_request(
            "POST",
            path
        )

        if str(data["result"]) == "True":
            log_service_manager.write_log(
                "OKEXF cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, order_id, data))
            return True
        else:
            code = str(data["error_code"])
            log_service_manager.write_log(
                "OKEXF cancel_order symbol:{}, order_id:{} failed! code:{} , data:{}".format(symbol, order_id, code,
                                                                                            data))
            return False
