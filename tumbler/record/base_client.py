# encoding: UTF-8

from copy import copy
from datetime import datetime
import requests

from tumbler.function import get_two_currency
from tumbler.api.rest import RestClient
from tumbler.service import log_service_manager
from tumbler.object import TickData
from tumbler.constant import MAX_PRICE_NUM


class BaseClient(RestClient):
    __headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
    }

    def __init__(self, _api_key, _secret_key, _passphrase="123"):
        super(BaseClient, self).__init__()

        self.api_key = str(_api_key)
        self.secret_key = str(_secret_key)
        self.passphrase = str(_passphrase)

        self.headers = self.__headers

        self.order_id = 1

    def _new_order_id(self):
        self.order_id += 1
        return self.order_id

    def get_exchange_name(self):
        return ""

    def get_ticker(self, symbol):
        return None

    def get_order(self, symbol, order_id):
        return None

    def get_open_orders(self, symbol):
        return []

    def get_traded_orders(self, symbol):
        return []

    def get_exchange_info(self):
        return {}

    def get_position_info(self):
        return {}

    def get_no_position_assets(self):
        return {}

    def get_assets(self):
        return {}

    def get_available_assets(self):
        return {}

    def query_future_positions(self, symbol):
        return {}

    def get_usdt_ticker(self, symbol):
        ticker = TickData()
        ticker.symbol = symbol
        ticker.exchange = self.get_exchange_name()
        ticker.vt_symbol = ticker.get_vt_key()
        ticker.bid_prices = [1.0] * MAX_PRICE_NUM
        ticker.ask_prices = [1.0] * MAX_PRICE_NUM
        ticker.bid_volumes = [0] * MAX_PRICE_NUM
        ticker.ask_volumes = [0] * MAX_PRICE_NUM
        ticker.volume = 0
        ticker.amount = 0
        ticker.last_price = 1
        ticker.gateway_name = self.get_exchange_name()
        return copy(ticker)

    def get_currency_ticker(self, currency):
        symbol = f"{currency}_usdt"
        if symbol in ["usdt_usdt"]:
            tick = self.get_usdt_ticker(symbol)
        else:
            tick = self.get_ticker(symbol)
        return tick

    def get_triangle_price(self, symbol):
        tc, bc = get_two_currency(symbol)

        tts = self.get_currency_ticker(tc)
        tbs = self.get_currency_ticker(bc)

        bid_price = tts.bid_prices[0] / tbs.ask_prices[0]
        ask_price = tts.ask_prices[0] / tbs.bid_prices[0]
        return bid_price, ask_price

    #################################
    # below need to delete
    @staticmethod
    def url_get(url):
        response = requests.get(url, timeout=5)
        return response.json()

    @staticmethod
    def deal_order_books(asks_data, bids_data):
        ret_dic = {}

        asks_data = [(float(x[0]), float(x[1])) for x in asks_data]
        bids_data = [(float(x[0]), float(x[1])) for x in bids_data]

        asks_data = sorted(asks_data, key=lambda price_pair: price_pair[0])
        bids_data = sorted(bids_data, key=lambda price_pair: price_pair[0])

        bids_data.reverse()

        ret_dic["bids"] = bids_data[:5]
        ret_dic["asks"] = asks_data[:5]

        return ret_dic

    def get_price(self, symbol):
        return None

    def get_order_book(self, symbol):
        return {}

    def cancel_all_orders(self, symbol):
        flag = True
        all_orders = self.get_open_orders(symbol)
        for order in all_orders:
            flag = self.cancel_order(order.symbol, order.order_id)
            if flag:
                log_service_manager.write_log("cancel_order symbol:{} , order_id:{} success!"
                                              .format(symbol, order.order_id))
            else:
                log_service_manager.write_log("cancel_order symbol:{} , order_id:{} failed!"
                                              .format(symbol, order.order_id))
                flag = False

        return flag

    def cancel_order(self, symbol, order_id):
        return True

    def send_order(self, req):
        return None

    def send_orders(self, reqs):
        return None

    def generate_date_time(self, s):
        dt = datetime.fromtimestamp(float(s) / 1e3)
        u_time = dt.strftime("%H:%M:%S")
        date = dt.strftime("%Y-%m-%d")
        return dt, date, u_time

    def get_available_assets_from_account_arr(self, account_arr):
        dic = {}
        for account in account_arr:
            dic[account.account_id.lower()] = account.available
        return dic

    def get_assets_from_account_arr(self, account_arr):
        dic = {}
        for account in account_arr:
            dic[account.account_id.lower()] = account.balance
        return dic

    def get_order_id(self):
        return int(datetime.now().strftime("%y%m%d%H%M%S"))

    def check_msg(self, data):
        return data and int(data["code"]) == 0

