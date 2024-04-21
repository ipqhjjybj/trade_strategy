# encoding: UTF-8

from copy import copy
from datetime import datetime
from collections import defaultdict

from pprint import pprint

from tumbler.object import TickData
from tumbler.constant import Exchange, Direction
from tumbler.function import simplify_tick
from tumbler.service import log_service_manager
from tumbler.function import split_url, get_vt_key, get_dt_use_timestamp
from tumbler.gateway.okex.base import sign_request
from tumbler.gateway.okex5.base import REST_TRADE_HOST, okex5_format_symbol, parse_contract_info
from tumbler.gateway.okex5.base import get_inst_type_from_okex_symbol
from tumbler.gateway.okex5.base import parse_position_data, parse_account_data, parse_order_info, OKEX5ModeType

from .base_client import BaseClient


class Okex5Client(BaseClient):
    def __init__(self, _apikey, _secret_key, _passphrase, mode_type=OKEX5ModeType.CASH.value, password=""):
        """
        :param _apikey: apikey
        :param _secret_key: secret_key
        :param _passphrase: 秘钥里面的password key
        :param mode_type: 账户的交易模式 cash, cross
        :param password: 账户资金密码
        """
        super(Okex5Client, self).__init__(_apikey, _secret_key, _passphrase)

        self.password = password
        self.mode_type = mode_type
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        self.host, _ = split_url(REST_TRADE_HOST)
        self.init(REST_TRADE_HOST)

        self.currencies_dic = {}

    def sign(self, request):
        """
        Generate OKEX signature.
        """
        return sign_request(request, self.api_key, self.secret_key, self.passphrase)

    def get_exchange_info(self, type_arr=[], uly="BTC_USD"):
        ret_arr = []
        if not type_arr:
            type_arr = ["SPOT", "SWAP", "FUTURES", "OPTION"]
        for _type in type_arr:
            url = "/api/v5/public/instruments?instType={}".format(_type)
            if _type == "OPTION":
                url = url + "&uly={}".format(uly)
            data = self.wrap_request(
                "GET",
                url
            )
            if self.check_msg(data):
                arr = parse_contract_info(data["data"], Exchange.OKEX5.value, _type)
                ret_arr.extend(arr)
        return ret_arr

    def get_ticker(self, symbol, depth=20):
        data = self.wrap_request("GET",
                                 "/api/v5/market/books?instId={}&sz={}"
                                 .format(okex5_format_symbol(symbol), depth))
        data = data["data"][0]
        tick = TickData()
        tick.symbol = symbol
        tick.vt_symbol = get_vt_key(symbol, Exchange.OKEX5.value)
        tick.datetime = get_dt_use_timestamp(data["ts"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])
        return copy(tick)

    def get_asset_balance_data(self):
        '''
        获得资金账户里的币
        '''
        return self.wrap_request("GET", "/api/v5/asset/balances", data={})

    def get_account_data(self):
        return self.wrap_request("GET", "/api/v5/account/balance", data={})

    def get_assets(self):
        ret = defaultdict(float)
        data = self.get_account_data()
        if self.check_msg(data):
            account_list = parse_account_data(data["data"], Exchange.OKEX5.value)
            for account in account_list:
                ret[account.account_id] = account.balance
            return ret
        return ret

    def get_positions(self):
        data = self.wrap_request("GET", "/api/v5/account/positions", data={})
        if self.check_msg(data):
            position_list = parse_position_data(data["data"], Exchange.OKEX5.value)
            return position_list
        return []

    def get_position_info(self):
        ret = defaultdict(float)
        for pos in self.get_positions():
            ret[pos.symbol] += pos.position * (1 if pos.direction == Direction.LONG.value else -1)
        return ret

    def format_req(self, req):
        '''
        {
            'code': '0',
            'data': [
                {'clOrdId': 'a2104301850422', 'ordId': '308314241833021440', 'sCode': '0', 'sMsg': '', 'tag': ''
                }
            ],
            'msg': ''
        }
        '''
        order_id = "a{}{}".format(self.connect_time, self._new_order_id())

        if req.direction == Direction.LONG.value:
            side = "buy"
        else:
            side = "sell"

        if abs(req.volume - int(req.volume)) < 1e-8:
            sz = str(int(req.volume))
        else:
            sz = str(req.volume)

        data = {
            "instId": okex5_format_symbol(req.symbol),
            "clOrdId": order_id,
            "tdMode": self.mode_type,
            "side": side,
            "ordType": "limit",
            "px": str(req.price),
            "sz": sz
        }
        pprint(data)

        order = req.create_order_data(order_id, Exchange.OKEX5.value)
        return data, order

    def send_order(self, req):
        data, order = self.format_req(req)
        data = self.wrap_request("POST", "/api/v5/trade/order", data=data)
        return data

    def cancel_order(self, symbol, order_id):
        data = {
            "ordId": str(order_id),
            "instId": okex5_format_symbol(symbol)
        }
        data = self.wrap_request(
            "POST",
            '/api/v5/trade/cancel-order',
            data=data
        )
        print(data)

        if self.check_msg(data):
            for dic in data["data"]:
                if int(dic["sCode"]) == 0:
                    log_service_manager.write_log(
                        "OKEX cancel_order symbol:{}, order_id:{} successily , data:{}".format(symbol, dic["ordId"], dic))
                else:
                    log_service_manager.write_log(
                        "OKEX cancel_order symbol:{}, order_id:{} failed , data:{}".format(symbol, dic["ordId"], dic))
            return True
        else:
            log_service_manager.write_log(
                "OKEX cancel_order symbol:{}, order_id:{} failed , data:{}".format(
                    symbol, order_id, data))
            return False

    def get_open_orders(self, symbol):
        data = {
            "instId": okex5_format_symbol(symbol)
        }
        data = self.wrap_request(
            "GET",
            "/api/v5/trade/orders-pending",
            data=data
        )
        ret = []
        if self.check_msg(data):
            for dic in data["data"]:
                order = parse_order_info(dic, dic["clOrdId"], Exchange.OKEX5.value)
                ret.append(order)
        return ret

    def query_order(self, symbol, order_id):
        data = {
            "instId": okex5_format_symbol(symbol),
            "clOrdId": str(order_id),
        }
        print(data)

        data = self.wrap_request(
            "GET",
            "/api/v5/trade/order",
            params=data
        )
        print(data)

        ret = []
        if self.check_msg(data):
            for dic in data["data"]:
                order = parse_order_info(dic, dic["clOrdId"], Exchange.OKEX5.value)
                ret.append(order)
        return ret

    def get_traded_orders(self, symbol, uly=""):
        ret = []
        params = {
            "instType": get_inst_type_from_okex_symbol(okex5_format_symbol(symbol)),
            "instId": okex5_format_symbol(symbol),
        }
        if uly:
            params["uly"] = uly

        for state in ['canceled', 'filled']:
            now_params = copy(params)
            now_params["state"] = state
            data = self.wrap_request(
                "GET",
                "/api/v5/trade/orders-history",
                params=now_params
            )
            print(data)
            if self.check_msg(data):
                for dic in data["data"]:
                    order = parse_order_info(dic, dic["clOrdId"], Exchange.OKEX5.value)
                    ret.append(order)
        return ret

    def get_currencies(self):
        return self.wrap_request("GET", "/api/v5/asset/currencies", params={})

    def get_currencies_info(self):
        if not self.currencies_dic:
            data = self.get_currencies()
            if self.check_msg(data):
                for dic in data["data"]:
                    self.currencies_dic[dic["chain"]] = copy(dic)
        return self.currencies_dic

    def withdrawal(self, chain, amt, toAddr, pwd="", dest="4"):
        if not pwd:
            pwd = self.password
        info_dic = self.get_currencies_info()[chain]
        data = {
            "ccy": chain.split('-')[0],
            "chain": chain,
            "amt": str(amt),
            "dest": dest,
            "toAddr": toAddr,
            "pwd": pwd,
            "fee": info_dic["minFee"]
        }
        print(data)
        return self.wrap_request("POST", "/api/v5/asset/withdrawal", data=data)

    def transfer(self, ccy, amt, _type, _from, _to, subAcct="", instId="", toInstId=""):
        '''
        _type	String	否	0：账户内划转
        1：母账户转子账户
        2：子账户转母账户
        默认为0。
        _from	String	是	转出账户
        1：币币账户 3：交割合约 5：币币杠杆账户 6：资金账户 9：永续合约账户 12：期权合约 18：统一账户
        _to	String	是	转入账户
        1：币币账户 3：交割合约 5：币币杠杆账户 6：资金账户 9：永续合约账户 12：期权合约 18：统一账户
        '''
        data = {
            "ccy": ccy.upper(),
            "amt": str(amt),
            "type": str(_type),
            "from": str(_from),
            "to": str(_to)
        }
        if subAcct:
            data["subAcct"] = subAcct
        if instId:
            data["instId"] = instId
        if toInstId:
            data["toInstId"] = toInstId
        return self.wrap_request("POST", "/api/v5/asset/transfer", data=data)

    def get_withdrawal_history(self):
        return self.wrap_request("GET", "/api/v5/asset/withdrawal-history", params={})

    def get_deposit_history(self):
        return self.wrap_request("GET", "/api/v5/asset/deposit-history", params={})

    def get_deposit_address(self, ccy):
        return self.wrap_request("GET", "/api/v5/asset/deposit-address", params={"ccy": ccy.upper()})



