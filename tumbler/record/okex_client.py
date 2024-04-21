# encoding: UTF-8

from copy import copy

from tumbler.function import split_url, parse_timestamp
from tumbler.function import simplify_tick, get_vt_key
from tumbler.constant import Exchange, OrderType, Direction
from tumbler.service import log_service_manager
from tumbler.object import TickData

from .base_client import BaseClient

from tumbler.gateway.okex.base import REST_TRADE_HOST, parse_contract_info
from tumbler.gateway.okex.base import parse_account_data, parse_order_info
from tumbler.gateway.okex.base import okex_format_symbol, sign_request, ORDER_TYPE_VT2OKEX, DIRECTION_VT2OKEX

okex_transfer_type = {
    "1": "spot",
    "3": "future",
    "5": "margin",
    "9": "swap"
}

okex_transfer_reverse_type = {v: k for k, v in okex_transfer_type.items()}

okex_chain_info = {
    "USDT-TRC20": "USDT-TRC20",
    "USDT-ERC20": "USDT-ERC20",
}


def okex_transfer_symbol(symbol):
    return symbol.replace('_', "-").lower()


class OkexClient(BaseClient):
    def __init__(self, _apikey, _secret_key, _passphrase, proxy_host="", proxy_port=0):
        super(OkexClient, self).__init__(_apikey, _secret_key, _passphrase)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def sign(self, request):
        """
        Generate OKEX signature.
        """
        return sign_request(request, self.api_key, self.secret_key, self.passphrase)

    def get_exchange_info(self):
        data = self.wrap_request(
            "GET",
            "/api/spot/v3/instruments"
        )
        return parse_contract_info(data, Exchange.OKEX.value)

    def get_ticker(self, symbol, depth=20, u_type="step0"):
        data = self.wrap_request("GET",
                                 "/api/spot/v3/instruments/{}/book?size={}&depth=0.00000001"
                                 .format(okex_format_symbol(symbol), depth))
        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(symbol, Exchange.OKEX.value)
        tick.datetime = parse_timestamp(data["timestamp"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])
        return copy(tick)

    def get_assets(self, contract_pairs=[]):
        data = self.wrap_request("GET", "/api/spot/v3/accounts", data={})
        account_list = parse_account_data(data, Exchange.OKEX.value)
        return self.get_assets_from_account_arr(account_list)

    def get_available_assets(self):
        data = self.wrap_request("GET", "/api/spot/v3/accounts", data={})
        account_list = parse_account_data(data, Exchange.OKEX.value)
        return self.get_available_assets_from_account_arr(account_list)

    def get_open_orders(self, symbol):
        ret_order_list = []
        data = self.wrap_request(
            "GET",
            "/api/spot/v3/orders_pending",
            params={"instrument_id": okex_format_symbol(symbol)}
        )
        for order_data in data:
            order = parse_order_info(order_data, order_data["client_oid"], Exchange.OKEX.value)
            ret_order_list.append(order)
        return ret_order_list

    def get_traded_orders(self, symbol):
        ret_order_list = []
        for state in [1, 2, -1]:
            data = self.wrap_request(
                "GET",
                "/api/spot/v3/orders",
                params={"instrument_id": okex_format_symbol(symbol), "state": state}
            )
            for order_data in data:
                order = parse_order_info(order_data, order_data["client_oid"], Exchange.OKEX.value)
                if order.traded > 0:
                    ret_order_list.append(order)
        return ret_order_list

    def cancel_order(self, symbol, order_id):
        data = {
            "instrument_id": okex_format_symbol(symbol),
            "order_id": order_id
        }
        path = "/api/spot/v3/cancel_orders/{}".format(order_id)
        data = self.wrap_request(
            "POST",
            path,
            data=data
        )

        if str(data["result"]) == "True":
            log_service_manager.write_log(
                "OKEX cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, order_id, data))
            return True
        else:
            code = str(data["error_code"])
            log_service_manager.write_log(
                "OKEX cancel_order symbol:{}, order_id:{} failed! code:{} , data:{}".format(symbol, order_id, code,
                                                                                            data))
            return False

    def send_order(self, req):
        data = {
            "client_oid": self.get_order_id(),
            "type": ORDER_TYPE_VT2OKEX[OrderType.LIMIT.value],
            "side": DIRECTION_VT2OKEX[Direction.LONG.value],
            "instrument_id": req.symbol,
            "size": req.volume,
            "price": req.price
        }
        return self.wrap_request("POST", "/api/spot/v3/orders", data=data)

    def transfer(self, currency, amount, _from, _to, instrument_id="", to_instrument_id=""):
        data = {
            "currency": okex_transfer_symbol(currency),
            "amount": "%.8lf" % amount,
            "type": "0",
            "from": okex_transfer_reverse_type[_from],
            "to": okex_transfer_reverse_type[_to]
        }
        if instrument_id:
            data["instrument_id"] = okex_transfer_symbol(instrument_id)
        if to_instrument_id:
            data["to_instrument_id"] = okex_transfer_symbol(to_instrument_id)
        return self.wrap_request("POST", "/api/account/v3/transfer", data=data)

    def withdrawal(self, currency, amount, to_address, trade_pwd, chain=None, fee=None, destination="4"):
        # / api / account / v3 / withdrawal
        # {"amount": "1", "fee": "0.0005", "trade_pwd": "123456", "destination": "4", "currency": "USDT",
        #  "to_address": "17DKe3kkkkiiiiTvAKKi2vMPbm1Bz3CMKw", "chain": "USDT-ERC20"}
        if not fee:
            fee = self.get_withdrawal_fee(currency)
        if not chain:
            chain = okex_chain_info.get(currency.upper(), None)
        data = {
            "currency": currency.upper(),
            "amount": str(amount),
            "destination": destination,
            "to_address": to_address,
            "trade_pwd": trade_pwd,
            "fee": str(fee),
            "chain": chain
        }
        return self.wrap_request("POST", "/api/account/v3/withdrawal", data=data)

    def withdrawal_fee(self, currency):
        return self.wrap_request("GET", "/api/account/v3/withdrawal/fee", params={"currency": currency.lower()})

    def get_withdrawal_fee(self, currency):
        query_currency = currency.split('-')[0]
        data = self.withdrawal_fee(query_currency)
        for dic in data:
            now_currency = dic.get("currency", "")
            min_fee = dic.get('min_fee', None)

            if now_currency.lower() == currency.lower():
                return min_fee
        return None

    def currencies(self):
        return self.wrap_request("GET", "/api/account/v3/currencies", params={})
