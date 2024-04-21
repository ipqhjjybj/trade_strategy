# encoding: UTF-8

from copy import copy
from datetime import datetime

from tumbler.function import split_url, get_dt_use_timestamp, simplify_tick
from tumbler.constant import Direction, Status, Exchange
from tumbler.object import TickData
from tumbler.gateway.gateio.base import parse_order_info, parse_account_info_arr
from tumbler.gateway.gateio.base import change_system_format_to_gateio_format
from tumbler.gateway.gateio.base import create_rest_signature, REST_TRADE_HOST, REST_MARKET_HOST
from tumbler.gateway.gateio.base import parse_contract_info_arr, change_gateio_format_to_system_format
from tumbler.service import log_service_manager

from .base_client import BaseClient


class GateClient(BaseClient):
    def __init__(self, _apikey, _secret_key, proxy_host="", proxy_port=0):
        super(GateClient, self).__init__(_apikey, _secret_key)
        self.data_host, _ = split_url(REST_MARKET_HOST)
        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST, proxy_host, proxy_port)

    def get_exchange_name(self):
        return Exchange.GATEIO.value

    def sign(self, request):
        request.headers = {
            "Accept": "application/json",
            'Content-Type': 'application/x-www-form-urlencoded',
            "User-Agent": "Chrome/39.0.2171.71",
            "KEY": self.api_key,
            "SIGN": create_rest_signature(request.data, self.secret_key)
        }
        return request

    def get_ticker(self, symbol):
        url = "http://data.gateio.io/api2/1/orderBook/{}".format(symbol)
        data = self.url_get(url)
        tick = TickData()
        tick.symbol = symbol
        if "timestamp" in data.keys():
            tick.datetime = get_dt_use_timestamp(data["timestamp"], mill=1)
        else:
            tick.datetime = datetime.now()
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])
        return copy(tick)

    def get_exchange_info(self):
        data = self.wrap_request(
            method="GET",
            data={},
            path="/api2/1/marketinfo"
        )
        ret = []
        for info in data["data"]:
            contract = parse_contract_info_arr(info, Exchange.GATEIO.value)
            ret.append(contract)
        return ret

    def get_open_orders(self, symbol):
        data = self.wrap_request(
            method="POST",
            data={},
            path="/api2/1/private/openOrders"
        )
        ret = []
        for d in data["orders"]:
            symbol = change_gateio_format_to_system_format(d["currencyPair"])
            sys_order_id = str(d["orderNumber"])
            order = parse_order_info(d, symbol, sys_order_id, Exchange.GATEIO.value, _type="query_order")
            ret.append(order)
        return ret

    def parse_trade_history(self, trades, symbol):
        ret_trade_list = []

        for trade in trades:
            if trade["pair"] == symbol:
                use_dt, use_date, now_time = self.generate_date_time(trade["time_unix"])
                year, month, day = ((trade["date"].split(' '))[0]).split("-")
                trade_datetime_time = (trade["date"].split(' '))[1]

                direction = Direction.LONG.value
                if trade["type"] == "sell":
                    direction = Direction.SHORT.value

                new_trade = {}
                new_trade["symbol"] = symbol
                new_trade["order_id"] = trade["orderNumber"]
                new_trade["exchange"] = Exchange.GATEIO.value
                new_trade["direction"] = direction
                new_trade["price"] = trade["rate"]
                new_trade["total_volume"] = trade["amount"]
                new_trade["traded_volume"] = trade["amount"]
                new_trade["status"] = Status.ALLTRADED.value
                new_trade["orderTime"] = str(year) + str(month) + str(day) + " " + str(trade_datetime_time)
                new_trade["cancelTime"] = ""

                ret_trade_list.append(new_trade)

        return ret_trade_list

    def get_traded_orders(self, symbol):
        # 获得交易的订单
        data = self.wrap_request(
            method="POST",
            data={},
            path="/api2/1/private/tradeHistory/{}".format(symbol)
        )
        return self.parse_trade_history(data["trades"], symbol)

    def get_balance(self):
        data = self.wrap_request(
            method="POST",
            data={},
            path="/api2/1/private/balances"
        )
        arr_account = parse_account_info_arr(data, Exchange.GATEIO.value)
        return [account for account in arr_account if account.balance > 0]

    def get_assets(self):
        ret = {}
        arr = self.get_balance()
        for account in arr:
            if account.account_id in ret.keys():
                ret[account.account_id] += account.balance
            else:
                ret[account.account_id] = account.balance
        return ret

    def cancel_order(self, symbol, order_id):
        params = {
            "currencyPair": change_system_format_to_gateio_format(symbol),
            "orderNumber": order_id
        }

        path = "/api2/1/private/cancelOrder"
        data = self.wrap_request(
            method="POST",
            path=path,
            data=params
        )
        if str(data["result"]) in ["true", "True"]:
            log_service_manager.write_log(
                "Gateio cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, order_id, data))
            return True
        else:
            log_service_manager.write_log(
                "Gateio cancel_order symbol:{}, order_id:{} failed , data:{} ".format(str(symbol), str(order_id),
                                                                                      str(data)))
            return False

    def query_deposit_address(self, symbol):
        params = {"currency": change_system_format_to_gateio_format(symbol)}
        path = "/api2/1/private/depositAddress"
        data = self.wrap_request(
            method="POST",
            path=path,
            data=params
        )
        return data

    def query_deposit_withdral(self):
        '''
        start	String	否	起始UNIX时间(如 1469092370)
        end	String	否	终止UNIX时间(如 1469713981)
        sortType	String	否	排序顺序（"ASC":升序,"DESC":降序）
        page	String	否	页码，取值从1开始
        '''
        data = self.wrap_request(
            method="POST",
            path="/api2/1/private/depositsWithdrawals",
            data={}
        )
        return data

    def withdral(self, address, amount, currency, chain=None):
        params = {
            "currency": currency.lower(),
            "amount": str(amount),
            "address": address
        }
        data = self.wrap_request(
            method="POST",
            path="/api2/1/private/withdraw",
            data=params
        )
        return data


