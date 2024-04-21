# coding=utf-8

from datetime import datetime, timedelta

from tumbler.record.binancef_client import BinancefClient
from tumbler.record.huobiu_client import HuobiuClient
from tumbler.record.okexs_client import OkexsClient

from tumbler.constant import Interval
from tumbler.data.binance_data import BinanceClient
from tumbler.service import mongo_service_manager


download_symbol_list = ["btc_usdt", "eth_usdt", "ltc_usdt", "bnb_usdt", "eos_usdt", "btm_usdt",
                        "ada_usdt", "xrp_usdt", "bch_usdt", "trx_usdt", "link_usdt", "dot_usdt"]


class DailyCoinsScheduler(object):
    def __init__(self, _symbol_list=None, _client=None, _interval=None, _out_path=None):
        if _symbol_list:
            self.symbol_list = _symbol_list
        else:
            self.symbol_list = self.init_binance_list()
            #self.symbol_list = self.init_symbol_list()
        if _client:
            self.client = _client
        else:
            self.client = BinanceClient()
        if _interval:
            self.interval = _interval
        else:
            self.interval = Interval.DAY.value
        if _out_path:
            self.out_path = _out_path
        else:
            self.out_path = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/day_future.csv"

    def init_symbol_list(self):
        binancef_client = BinancefClient("", "")
        binancef_contract_arr = binancef_client.get_exchange_info()

        binancef_symbols = [c.symbol for c in binancef_contract_arr]

        huobiu_client = HuobiuClient("", "")
        huobiu_contracts_arr = huobiu_client.get_exchange_info()
        huobiu_symbols = [c.symbol for c in huobiu_contracts_arr]

        okexs_client = OkexsClient("", "", "")
        okexs_contracts_arr = okexs_client.get_exchange_info()
        okexs_symbols = [c.symbol.replace("_swap", "") for c in okexs_contracts_arr]

        return [x for x in binancef_symbols if x in huobiu_symbols and x in okexs_symbols]

    def init_binance_list(self):
        binancef_client = BinancefClient("", "")
        binancef_contract_arr = binancef_client.get_exchange_info()
        return [c.symbol for c in binancef_contract_arr if c.symbol.endswith("usdt")]

    def run(self):
        now = datetime.now()
        if self.interval == Interval.DAY.value:
            before = now - timedelta(days=1000)
        else:
            before = now - timedelta(days=3)

        ret_data = []
        for symbol in self.symbol_list:
            self.client.download_save_mongodb(symbol=symbol, _start_dt=before,
                                              _end_dt=now, interval=self.interval)

            data = mongo_service_manager.load_bar_data(symbol, "BINANCE", self.interval,
                                                       datetime(2017, 1, 1), now)
            if data:
                ret_data.extend(data)
        self.output_data(ret_data)

    def output_data(self, ret_data):
        f = open(self.out_path, "w")
        f.write("symbol,exchange,datetime,open,high,low,close,volume\n")
        for bar in ret_data:
            arr = [bar.symbol, bar.exchange, bar.datetime,
                   bar.open_price, bar.high_price, bar.low_price, bar.close_price, bar.volume]
            arr = [str(x) for x in arr]
            f.write(','.join(arr) + "\n")
        f.close()


