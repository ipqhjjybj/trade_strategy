# encoding: UTF-8

from time import sleep
from copy import copy

from tumbler.function import get_format_system_symbol, split_url, get_vt_key
from tumbler.function import get_no_under_lower_symbol, simplify_tick, get_dt_use_timestamp
from tumbler.constant import Exchange
from tumbler.service import log_service_manager
from tumbler.object import TickData

from tumbler.gateway.huobi.base import REST_TRADE_HOST, sign_request
from tumbler.gateway.huobi.base import parse_order_info, parse_contract_info, parse_account_info
from tumbler.gateway.huobi.base import HUOBI_WITHDRAL_FEE

from .base_client import BaseClient


class HuobiClient(BaseClient):
    def __init__(self, _apikey, _secret_key, proxy_host="", proxy_port=0, account_id=None):
        super(HuobiClient, self).__init__(_apikey, _secret_key)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

        self.account_id = account_id

    def get_exchange_name(self):
        return Exchange.HUOBI.value

    def sign(self, request):
        return sign_request(request, self.api_key, self.secret_key, self.host)

    def get_ticker(self, symbol, depth=20, u_type="step0"):
        data = self.wrap_request("GET",
                                 "/market/depth?symbol={}&type={}&depth={}".format(get_no_under_lower_symbol(symbol),
                                                                                   u_type, str(depth)))
        if data is None or self.check_error(data):
            log_service_manager.write_log("[HUOBI get_ticker] symbol:{} data:{}".format(symbol, data))
            return None

        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(tick.symbol, Exchange.HUOBI.value)
        tick.datetime = get_dt_use_timestamp(data["ts"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["tick"]["bids"], data["tick"]["asks"])
        return copy(tick)

    def loop_get_account(self, times=0):
        if times < 5:
            if not self.account_id:
                try:
                    self.get_accounts()
                    self.loop_get_account(times + 1)
                    sleep(5)
                except Exception as e:
                    log_service_manager.write_log('get acct_id error.{}'.format(e))

    def get_accounts(self):
        data = self.wrap_request(method="GET", path="/v1/account/accounts")
        if data:
            for d in data["data"]:
                if d["type"] == "spot":
                    self.account_id = d["id"]
                    log_service_manager.write_log("[info] [get_accounts] account_id:{}".format(self.account_id))

    def get_open_orders(self, symbol):
        self.loop_get_account()

        kwargs = {"symbol": get_format_system_symbol(symbol), "account-id": self.account_id}
        data = self.wrap_request(
            method="GET",
            path="/v1/order/openOrders",
            params=kwargs
        )
        orders = data["data"]
        ret = []
        for d in orders:
            order = parse_order_info(d, symbol, str(d["id"]), Exchange.HUOBI.value, _type="query_open_order")
            ret.append(order)
        return ret

    def get_exchange_info(self):
        data = self.wrap_request(
            method="GET",
            path="/v1/common/symbols"
        )
        ret = []
        if self.check_msg(data):
            for info in data["data"]:
                contract = parse_contract_info(info, Exchange.HUOBI.value)
                ret.append(contract)
        return ret

    def get_traded_orders(self, symbol):
        self.loop_get_account()
        kwargs = {"symbol": get_format_system_symbol(symbol), 'states': "canceled,partial-filled,filled",
                  "account-id": self.account_id}
        data = self.wrap_request("GET", "/v1/order/orders", params=kwargs)
        orders = data["data"]
        ret = []
        for d in orders:
            order = parse_order_info(d, symbol, str(d["id"]), Exchange.HUOBI.value, _type="query_order")
            ret.append(order)
        return [order for order in ret if order.traded > 0]

    def get_balance(self):
        self.loop_get_account()
        path = "/v1/account/accounts/{}/balance".format(self.account_id)
        return self.wrap_request(
            method="GET",
            path=path
        )

    def get_assets(self):
        data = self.get_balance()
        account_list = parse_account_info(data, Exchange.HUOBI.value)
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        data = self.get_balance()
        account_list = parse_account_info(data, Exchange.HUOBI.value)
        return self.get_available_assets_from_account_arr(account_list)

    def cancel_order(self, symbol, order_id):
        symbol = get_format_system_symbol(symbol)
        request_path = "/v1/order/orders/{0}/submitcancel".format(order_id)
        data = self.wrap_request("POST", request_path, params={})
        if str(data["status"]) == "ok":
            log_service_manager.write_log(
                "HUOBI cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, order_id, data))
            return True
        else:
            code = str(data["status"])
            log_service_manager.write_log(
                "HUOBI cancel_order symbol:{}, order_id:{} failed! code:{} , data:{}".format(symbol, order_id, code,
                                                                                             data))
            return False

    def query_deposit_address(self, currency):
        request_path = "/v2/account/deposit/address"
        params = {"currency": currency}
        data = self.wrap_request("GET", request_path, params=params)
        log_service_manager.write_log("[HUOBI query_deposit_address] data:{}".format(data))
        if str(data["code"]) == "200":
            return data["data"]

    def withdrawal(self, address, amount, currency, chain="", addr_tag=""):
        request_path = "/v1/dw/withdraw/api/create"
        params = {
            "address": address,
            "amount": str(amount),
            "currency": currency,
            "fee": str(HUOBI_WITHDRAL_FEE[currency])
        }
        if chain:
            params["chain"] = chain
        if addr_tag:
            params["addr_tag"] = addr_tag
        data = self.wrap_request("POST", request_path, data=params)
        return data

    def query_deposit_withdrawal(self, currency, _type):
        """
        :param currency:
        :param type: deposit, withdraw
        :return:
        """
        request_path = "/v1/query/deposit-withdraw"
        params = {"currency": currency, "type": _type}
        data = self.wrap_request("GET", request_path, params=params)
        if str(data["status"]) == "ok":
            return data["data"]

    def check_error(self, data, func=""):
        if data["status"] != "error":
            return False

        error_code = data["err-code"]
        error_msg = data["err-msg"]

        log_service_manager.write_log(
            "{} request_error, status_code:{},information:{}".format(str(func),
                str(error_code), str(error_msg)))
        return True
