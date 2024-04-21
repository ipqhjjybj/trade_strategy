# coding=utf-8

from copy import copy

from tumbler.function import get_vt_key, get_two_currency
from tumbler.constant import MarginMode, Exchange
from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.record.client_quick_query import get_future_symbols, get_spot_symbols


class FutureContractManager(object):
    '''
    期货季度管理管理模块
    1、确定当周、次周、当季、次季 是哪几个代码
    2、判断是否到了要换期的时候
    '''

    def __init__(self, exchange, strategy):
        self.exchange = exchange
        self.strategy = strategy
        self.client = ClientPosPriceQuery.query_client(exchange)

        self.contract_dict = {}
        self.pre_contract_dict = {}
        self.now_contract_dict = {}
        '''
        self.contract_dict = {
            "btc_usd_092011.HUOBIF": ContractData,
            "btc_usdd_092011.HUOBIF": ContractData,
        }
        self.pre_contract_dict = {
            "btc_usd": {
                "this_week": "btc_usd_092011",
                "next_week": "btc_usd_092011",
                "quarter": "btc_usd_092011"
            },
            "btc_usdt": {
                "this_week": "btc_usd_092011",
                "next_week": "btc_usd_092011",
                "quarter": "btc_usd_092011"
            },
        }
        self.now_contract_dict = {
            "btc_usd": {
                "this_week": "btc_usd_092011",
                "next_week": "btc_usd_092011",
                "quarter": "btc_usd_092011"
            },
            "btc_usdt": {
                "this_week": "btc_usd_092011",
                "next_week": "btc_usd_092011",
                "quarter": "btc_usd_092011"
            },
        }
        '''

    def save_old_symbol(self, pre_symbol, contract_type, symbol):
        if pre_symbol in self.pre_contract_dict.keys():
            self.pre_contract_dict[pre_symbol][contract_type] = symbol
        else:
            self.pre_contract_dict[pre_symbol] = {
                contract_type: symbol
            }

    def run_update(self):
        '''
        更新合约，并判断是否需要更换合约
        '''
        self.strategy.write_log("[FutureManager] run_update!")
        contract_arr = self.client.get_exchange_info()
        for contract in contract_arr:
            if contract.vt_symbol not in self.contract_dict.keys():
                self.contract_dict[contract.vt_symbol] = copy(contract)

            if contract.contract_type:
                base_symbol = contract.get_contract_base_symbol()
                if base_symbol in self.now_contract_dict.keys():
                    if contract.contract_type in self.now_contract_dict[base_symbol].keys():
                        self.save_old_symbol(self.now_contract_dict[base_symbol][contract.contract_type],
                                             contract.contract_type, contract.symbol)
                    self.now_contract_dict[base_symbol][contract.contract_type] = contract.symbol
                else:
                    self.now_contract_dict[base_symbol] = {
                        contract.contract_type: contract.symbol
                    }

    def get_contract_dic_from_base_symbol(self, base_symbol):
        return self.now_contract_dict.get(base_symbol, None)

    def get_contract(self, vt_symbol):
        return self.contract_dict.get(vt_symbol, None)

    def get_pre_contract_from_contract_type(self, base_symbol, contract_type):
        return self.get_contract(get_vt_key(self.get_pre_contract_symbol_from_contract_type(
            base_symbol, contract_type), self.exchange))

    def get_now_contract_from_contract_type(self, base_symbol, contract_type):
        return self.get_contract(get_vt_key(self.get_now_contract_symbol_from_contract_type(
            base_symbol, contract_type), self.exchange))

    def get_pre_contract_symbol_from_contract_type(self, base_symbol, contract_type):
        '''
        判断某 contract_type类型的 前一个 contract是什么
        '''
        if base_symbol in self.pre_contract_dict.keys():
            return self.pre_contract_dict[base_symbol].get(contract_type, None)
        return None

    def get_now_contract_symbol_from_contract_type(self, base_symbol, contract_type):
        '''
        判断某 contract_type类型的 当前 contract是什么
        '''
        if base_symbol in self.now_contract_dict.keys():
            return self.now_contract_dict[base_symbol].get(contract_type, None)
        return None

    def is_in_update_contract_time(self, symbol, hours=1, minutes=0, seconds=0):
        '''
        判断是否处于更新当周合约 到 下一周合约的时间
        '''
        contract = self.get_contract(symbol)
        return contract and contract.is_in_update_contract_time(hours, minutes, seconds)

    def is_in_clear_time(self, symbol, hours=1, minutes=0, seconds=0):
        '''
        判断是否处于需要清算所有合约头寸的时间
        '''
        contract = self.get_contract(symbol)
        return contract and contract.is_in_clear_time(hours, minutes, seconds)

    def is_in_stop_trading_time(self, symbol, hours=1, minutes=0, seconds=0):
        '''
        判断是否处于需要清算所有合约头寸的时间
        '''
        contract = self.get_contract(symbol)
        return contract and contract.is_in_stop_trading_time(hours, minutes, seconds)


class UsdtContractManager(object):
    '''
    USDT 永续合约期货管理模块
    '''
    def __init__(self, exchange, strategy, inst_type=""):
        self.exchange = exchange
        self.strategy = strategy
        self.inst_type = inst_type

        self.client = ClientPosPriceQuery.query_client(exchange)

        self.contract_dict = {}

        self.future_symbols = set([])
        self.spot_symbols = set([])

    def get_future_symbols(self):
        if not self.future_symbols:
            self.future_symbols = set(get_future_symbols())
        return self.future_symbols

    def get_spot_symbols(self):
        if not self.spot_symbols:
            self.spot_symbols = set(get_spot_symbols())
        return self.spot_symbols

    def run_update(self):
        self.strategy.write_log("[UsdtContractManager] run_update!")

        new_spot_symbols = get_spot_symbols()
        if new_spot_symbols:
            self.spot_symbols = set(new_spot_symbols)

        new_future_symbols = get_future_symbols()
        if new_future_symbols:
            self.future_symbols = set(new_future_symbols)

        if self.exchange in [Exchange.OKEX5.value]:
            contract_arr = self.client.get_exchange_info(type_arr=[self.inst_type])
        else:
            contract_arr = self.client.get_exchange_info()
        for contract in contract_arr:
            if contract.vt_symbol not in self.contract_dict.keys():
                self.contract_dict[contract.vt_symbol] = copy(contract)

    def get_contract(self, vt_symbol):
        return self.contract_dict.get(vt_symbol, None)

    def get_all_symbols(self):
        if self.exchange in [Exchange.HUOBIU.value]:
            arr = []
            for vt_symbol, contract in self.contract_dict.items():
                if contract.support_margin_mode == MarginMode.ALL.value \
                        and contract.symbol in self.get_future_symbols():
                    arr.append(contract.symbol)
            return arr
        elif self.exchange in [Exchange.OKEX5.value]:
            arr = []
            for vt_symbol, contract in self.contract_dict.items():
                if self.inst_type == "SPOT":
                    for suffix in ["_usdt", "_btc", "_eth"]:
                        if contract.symbol.endswith(suffix) and contract.symbol in self.get_spot_symbols():
                            arr.append(vt_symbol)
                elif self.inst_type == "SWAP":
                    symbol = contract.symbol.replace("_swap", "")
                    if symbol.endswith("_usdt"):
                        if symbol in self.get_spot_symbols():
                            arr.append(vt_symbol)
                    elif symbol.endswith("_usd"):
                        continue
                        # symbol = symbol + "t"
                        # if symbol in self.get_spot_symbols():
                        #     arr.append(vt_symbol)
                    else:
                        self.strategy.write_log(f"[get_all_vt_symbols] found not know suffix! vt_symbol:{vt_symbol}")
                else:
                    self.strategy.write_log(f"[get_all_vt_symbols] not support {self.inst_type}! vt_symbol:{vt_symbol}")
            return arr
        else:
            arr = []
            for vt_symbol, contract in self.contract_dict.items():
                for suffix in ["_usdt", "_btc", "_eth"]:
                    if contract.symbol.endswith(suffix) and contract.symbol in self.get_spot_symbols():
                        arr.append(vt_symbol)
            return arr

    def get_all_vt_symbols(self):
        if self.exchange in [Exchange.HUOBIU.value]:
            arr = []
            for vt_symbol, contract in self.contract_dict.items():
                if contract.support_margin_mode == MarginMode.ALL.value \
                        and contract.symbol in self.get_future_symbols():
                    arr.append(vt_symbol)
            return arr
        elif self.exchange in [Exchange.OKEX5.value]:
            arr = []
            for vt_symbol, contract in self.contract_dict.items():
                if self.inst_type == "SPOT":
                    for suffix in ["_usdt", "_btc", "_eth"]:
                        if contract.symbol.endswith(suffix) and contract.symbol in self.get_spot_symbols():
                            arr.append(vt_symbol)
                elif self.inst_type == "SWAP":
                    symbol = contract.symbol.replace("_swap", "")
                    if symbol.endswith("_usdt"):
                        if symbol in self.get_spot_symbols():
                            arr.append(vt_symbol)
                    elif symbol.endswith("_usd"):
                        continue
                        # symbol = symbol + "t"
                        # if symbol in self.get_spot_symbols():
                        #     arr.append(vt_symbol)
                    else:
                        self.strategy.write_log(f"[get_all_vt_symbols] found not know suffix! vt_symbol:{vt_symbol}")
                else:
                    self.strategy.write_log(f"[get_all_vt_symbols] not support {self.inst_type}! vt_symbol:{vt_symbol}")
            return arr
        else:
            arr = []
            for vt_symbol, contract in self.contract_dict.items():
                #for suffix in ["_usdt", "_btc", "_eth"]:
                for suffix in ["_usdt"]:
                    if contract.symbol.endswith(suffix) and contract.symbol in self.get_spot_symbols():
                        arr.append(vt_symbol)
            return arr


