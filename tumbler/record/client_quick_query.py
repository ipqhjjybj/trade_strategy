# encoding: UTF-8

import os
from tumbler.service import log_service_manager
from tumbler.function import get_from_vt_key
from tumbler.constant import Exchange, MarginMode

from . import client_map

from .huobi_client import HuobiClient
from .huobif_client import HuobifClient
from .huobiu_client import HuobiuClient
from .huobis_client import HuobisClient
from .binance_client import BinanceClient
from .binancef_client import BinancefClient
from .bitmex_client import BitmexClient
from .okex_client import OkexClient
from .okexf_client import OkexfClient
from .okex5_client import Okex5Client
from .okexs_client import OkexsClient
from .nexus_client import NexusClient
from .gate_client import GateClient
from .coinex_client import CoinexClient
from .bitfinex_client import BitfinexClient


def get_binance_symbols(reload=True):
    cache_file = ".tumbler/binance_future.csv"
    if not reload and os.path.exists(cache_file):
        ret = []
        f = open(cache_file, "r")
        for line in f:
            ret.append(line.strip())
        f.close()
        return ret

    binance_client = BinanceClient("", "")
    contract_arr = binance_client.get_exchange_info()
    arr = [x.symbol for x in contract_arr]

    f = open(cache_file, "w")
    for v in arr:
        f.write(v + "\n")
    f.close()
    return arr


def get_binance_future_symbols(reload=True):
    cache_file = ".tumbler/binancef_future.csv"
    if not reload and os.path.exists(cache_file):
        ret = []
        f = open(cache_file, "r")
        for line in f:
            ret.append(line.strip())
        f.close()
        return ret

    binancef_client = BinancefClient("", "")
    contract_arr = binancef_client.get_exchange_info()
    arr = [x.symbol for x in contract_arr]

    f = open(cache_file, "w")
    for v in arr:
        f.write(v + "\n")
    f.close()
    return arr


def get_huobiu_future_symbols(reload=True):
    cache_file = ".tumbler/huobiu_future.csv"
    if not reload and os.path.exists(cache_file):
        ret = []
        f = open(cache_file, "r")
        for line in f:
            ret.append(line.strip())
        f.close()
        return ret

    huobiu_client = HuobiuClient("", "")
    contract_arr = huobiu_client.get_exchange_info()
    arr = [x.symbol for x in contract_arr if x.support_margin_mode == MarginMode.ALL.value]

    f = open(cache_file, "w")
    for v in arr:
        f.write(v + "\n")
    f.close()
    return arr


def get_future_symbols(reload=True):
    huobi_symbols = get_huobiu_future_symbols(reload)
    binance_symbols = get_binance_future_symbols(reload)
    return [x for x in huobi_symbols if x in binance_symbols]


def get_spot_symbols(reload=True):
    binance_symbols = get_binance_symbols(reload)
    binancef_symbols = get_binance_future_symbols(reload)
    return [x for x in binance_symbols if x in binancef_symbols]


class ClientPosPriceQuery(object):
    '''
    后期可以修改
    def a(**x):
        print(x)

    a(x=1,y=2,z=3)

    d = {
        "x": 1,
        "b": 2
    }
    print(a(**d))


    def __init__(self, **args):
        super(BaseClient, self).__init__()
        d = self.__dict__
        for k, v in args.items():
            d[k] = v

        self.headers = self.__headers
        self.order_id = 1

    '''
    @staticmethod
    def query_client_new(exchange, api_key="", secret_key="", passphrase="", third_address="", third_public_key=""):
        client_class = client_map.get(exchange)
        if client_class:
            return client_class(api_key=api_key, secret_key=secret_key,
                                passphrase=passphrase, third_address="", third_public_key="")

    @staticmethod
    def query_client(exchange, api_key="", secret_key="", passphrase="", third_address="", third_public_key=""):
        client = None
        if exchange == Exchange.OKEX.value:
            client = OkexClient(api_key, secret_key, passphrase)
        elif exchange == Exchange.OKEXF.value:
            client = OkexfClient(api_key, secret_key, passphrase)
        elif exchange == Exchange.OKEXS.value:
            client = OkexsClient(api_key, secret_key, passphrase)
        elif exchange == Exchange.OKEX5.value:
            client = Okex5Client(api_key, secret_key, passphrase)
        elif exchange == Exchange.BINANCEF.value:
            client = BinancefClient(api_key, secret_key, passphrase)
        elif exchange == Exchange.BITMEX.value:
            client = BitmexClient(api_key, secret_key)
        elif exchange == Exchange.BINANCE.value:
            client = BinanceClient(api_key, secret_key)
        elif exchange == Exchange.GATEIO.value:
            client = GateClient(api_key, secret_key)
        elif exchange == Exchange.HUOBI.value:
            client = HuobiClient(api_key, secret_key)
        elif exchange == Exchange.COINEX.value:
            client = CoinexClient(api_key, secret_key)
        elif exchange == Exchange.BITFINEX.value:
            client = BitfinexClient(api_key, secret_key)
        elif exchange == Exchange.NEXUS.value:
            client = NexusClient(api_key, secret_key)
        elif exchange == Exchange.HUOBIS.value:
            client = HuobisClient(api_key, secret_key)
        elif exchange == Exchange.HUOBIU.value:
            client = HuobiuClient(api_key, secret_key)
        elif exchange == Exchange.HUOBIF.value:
            client = HuobifClient(api_key, secret_key)
        return client

    @staticmethod
    def query_position_info(exchange, api_key="", secret_key="", passphrase=""):
        client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase)
        return client.get_position_info()

    @staticmethod
    def query_exchange_info(exchange, api_key="", secret_key="", passphrase=""):
        client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase)
        return client.get_exchange_info()

    @staticmethod
    def query_ticker(vt_symbol):
        symbol, exchange = get_from_vt_key(vt_symbol)
        client = ClientPosPriceQuery.query_client(exchange)
        return client.get_ticker(symbol)

    '''
    获得还在执行的订单
    '''
    @staticmethod
    def query_open_orders(exchange, symbol, api_key, secret_key, passphrase=None, third_address="",
                          third_public_key=""):
        client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase,
                                                  third_address=third_address,
                                                  third_public_key=third_public_key)
        return client.get_open_orders(symbol)

    '''
    获得已经执行完的订单
    '''
    @staticmethod
    def query_traded_orders(exchange, symbol_pair, api_key, secret_key, passphrase=None, third_address="",
                            third_public_key=""):
        try:
            client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase, third_address,
                                                      third_public_key)
            if client is not None:
                return client.get_traded_orders(symbol_pair)
        except Exception as ex:
            log_service_manager.write_log(ex)
        return []

    '''
    查询实时资产数目
    '''
    @staticmethod
    def query_all_assets(exchange, api_key, secret_key, passphrase=None, third_address="", third_public_key=""):
        ret_dict = {}
        try:
            client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase, third_address,
                                                      third_public_key)
            if client is not None:
                ret_dict = client.get_assets()
        except Exception as ex:
            log_service_manager.write_log(ex)
        return ret_dict

    '''
    查询实时 available 资产数目
    '''
    @staticmethod
    def query_available_assets(exchange, api_key, secret_key, passphrase=None):
        client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase)
        return client.get_available_assets()


    '''
    取消某个交易所 某个交易对的所有订单
    '''
    @staticmethod
    def cancel_all_orders(exchange, symbol, api_key, secret_key, passphrase=None):
        client = ClientPosPriceQuery.query_client(exchange, api_key, secret_key, passphrase)
        return client.cancel_all_orders(symbol)


    '''
    单独查询账号
    '''
    @staticmethod
    def query_single_asset(exchange, asset, api_key, secret_key, passphrase=None):
        dic = ClientPosPriceQuery.query_all_assets(exchange, api_key, secret_key, passphrase)
        asset = asset.lower()
        if asset in dic.keys():
            return float(dic[asset])
        return 0.0

    '''
    查询所有的可用余额
    '''

    @staticmethod
    def query_single_available_assets(exchange, asset, api_key, secret_key):
        dic = ClientPosPriceQuery.query_available_assets(exchange, api_key, secret_key)
        asset = asset.lower()

        if asset in dic.keys():
            return float(dic[asset])
        return 0.0
