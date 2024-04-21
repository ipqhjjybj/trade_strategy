# coding=utf-8

import os
from copy import copy, deepcopy
from datetime import timedelta

import pandas as pd

from tumbler.apps.alpha_trader.template import (
    AlphaTemplate
)

import tumbler.function.risk as risk
from tumbler.object import BarData, TickData
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator, PandasDeal
from tumbler.constant import Direction, Status, Interval, Exchange
from tumbler.apps.cta_strategy.template import OrderSendModule
from tumbler.function import load_json, save_json
from tumbler.function.technique import PD_Technique
from tumbler.function.future_manager import UsdtContractManager
from tumbler.apps.cta_strategy.template import NewOrderSendModule
from tumbler.function.bar import BarGenerator, ArrayManager


class MultiAssetSymbolExchange(object):
    '''
    初始化指定 cash 以及 target_asset_amount ，
    然后每日调仓，保持 target_asset_amount * price 与 cash 差距不大
    '''

    def __init__(self, contract, strategy, target_asset, cash=0, target_asset_amount=0, day_window=1, init_pos=0):
        self.target_asset = target_asset
        self.cash = cash
        self.target_asset_amount = target_asset_amount

        self.contract = contract
        self.strategy = strategy

        self.am_day = ArrayManager(150)

        self.tick_decorator = risk.TickDecorder(contract.vt_symbol, self)
        self.order_module = NewOrderSendModule.init_order_send_module_from_contract(
            contract, self.strategy, init_pos, wait_seconds=10)
        self.order_module.start()

        self.get_now_pos = self.order_module.get_now_pos
        self.get_target_pos = self.order_module.get_target_pos

        self.day_bg = BarGenerator(None, window=day_window, on_window_bar=self.on_day_bar,
                                   interval=Interval.DAY.value, quick_minute=2)

    def is_inited(self):
        return self.am_day.inited and self.am_day.inited

    def on_tick(self, tick):
        self.tick_decorator.update_tick(tick)

    def is_change_too_small(self, trade_volume):
        now_target_pos = self.order_module.get_target_pos()
        if abs(trade_volume - now_target_pos) < max(abs(trade_volume), abs(now_target_pos)) * 0.01:
            self.strategy.write_log(f"[run_hour_bar] trade_volume:{trade_volume} "
                                    f"now_target_pos{now_target_pos} changes too small, not go to trade!")
            return True
        else:
            return False

    def on_bar(self, bar):
        self.order_module.on_bar(copy(bar))
        self.day_bg.update_bar(bar)

    def get_now_target_asset_val(self):
        return self.order_module.get_now_pos() * self.tick_decorator.tick.price * self.contract.size

    def get_min_trade_amount(self):
        return self.tick_decorator.tick.price * self.contract.size

    def on_day_bar(self, bar):
        self.am_day.update_bar(bar)
        if self.am_day.inited:
            # 这里开始计算每天的仓位
            now_target_asset_val = self.get_now_target_asset_val()
            if now_target_asset_val > self.cash + 2 * self.get_min_trade_amount():
                # 卖出目标 asset
                pos_val = (now_target_asset_val - self.cash) / self.get_min_trade_amount() / 2
                self.write_log(f"[MultiAssetSymbolExchange] on_day_bar pos_val:{pos_val}!")
                #self.order_module.set_to_target_pos(self.order_module.get_now_pos() - pos_val)

            elif now_target_asset_val < self.cash - 2 * self.get_min_trade_amount():
                # 买入目标 asset
                pos_val = (self.cash - now_target_asset_val) / self.get_min_trade_amount() / 2
                self.write_log(f"[MultiAssetSymbolExchange] on_day_bar pos_val:{pos_val}!")
                #self.order_module.set_to_target_pos(self.order_module.get_now_pos() + pos_val)

            #trade_volume = self.fixed * algo_pos / self.contract.size

    def run_day_func(self, func):
        return func(self.am_day, self)

    def on_order(self, order):
        if order.vt_symbol == self.contract.vt_symbol:
            msg = f"[SymbolExchange][on_order] {order.vt_symbol}, {order.direction}, {order.price}, " \
                  f"order.volume:{order.volume}, order.traded:{order.traded}"
            self.write_log(msg)
            bef_target_pos = self.order_module.get_now_pos()
            self.order_module.on_order(order)
            after_target_pos = self.order_module.get_now_pos()
            minus_pos = after_target_pos - bef_target_pos
            self.cash -= minus_pos * self.contract.size * self.tick_decorator.tick.price

    def on_trade(self, trade):
        pass

    def get_condition(self):
        return self.target_asset, {
            "cash": 0,
            "target_asset_val": 0
        }

    def write_log(self, msg):
        self.strategy.write_log(msg)

    def write_important_log(self, msg):
        self.strategy.write_important_log(msg)


class MultiAssetBalancedStrategy(AlphaTemplate):
    """
    在永续合约上运行该策略
    多资产均衡策略, 指定多种资产，初始化现金及币种， 让任一币种现金与该币种金额保持 接近相等。
    每天调仓一次。
    save_position_dic = {
        "btc": {
            "cash": 333.0,
            "target_asset_val": 200
        },
        "eth": {
            "cash": 333.0,
            "target_asset_val": 200
        }
    }
    """
    author = "ipqhjjybj"
    class_name = "MultiAssetBalancedStrategy"

    inst_type = "SWAP"
    contract_exchange = Exchange.OKEX5.value
    day_window = 1

    start_cash = 0
    do_assets_arr = []

    support_long = True
    support_short = False

    # 参数列表
    parameters = [
        'strategy_name',
        'class_name',
        'author',
        'contract_exchange',
        'start_cash',
        'do_assets_arr'
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MultiAssetBalancedStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.contract_tick_dict = {}
        self.future_manager = UsdtContractManager(self.contract_exchange, self, self.inst_type)
        self.order_module_dict = {}

        self.time_work_bbo = risk.TimeWork(2)
        self.time_read_coins_time = risk.TimeWork(5)
        self.time_update_contracts = risk.TimeWork(10)

        self.symbol_exchanges = {}

        self.load_local_positions()
        self.save_position_dic = {}

        self.do_assets_symbols_set = set([])
        for asset in self.do_assets_arr:
            self.do_assets_symbols_set.add(asset + "_usdt")

    def on_init(self):
        self.write_log("[on_init]")

        self.subscribe_bbo_exchanges(self.contract_exchange, [self.inst_type])

    def on_start(self):
        self.write_log("[on_start]")

        self.load_local_positions()
        self.update_contracts()

    def on_stop(self):
        self.write_log("[on_stop]")

    def load_local_positions(self):
        if os.path.exists("multi_asset.json"):
            self.save_position_dic = load_json("multi_asset.json")

            for asset in self.save_position_dic.keys():
                vt_symbol = get_vt_key(asset + "_usdt", Exchange.OKEX5.value)
                contract = self.future_manager.get_contract(vt_symbol)

                if contract and vt_symbol not in self.symbol_exchanges.keys():
                    asset_dic = self.save_position_dic[asset]
                    cash = asset_dic["cash"]
                    target_asset_val = asset_dic["target_asset_val"]

                    self.symbol_exchanges[vt_symbol] = MultiAssetSymbolExchange(
                        contract, self, asset, cash=cash, target_asset_amount=target_asset_val, day_window=1,
                        init_pos=0)

    def save_condition(self):
        new_save_position_dic = {}
        for symbol in list(self.symbol_exchanges.keys()):
            obj = self.symbol_exchanges[symbol]
            target_asset, dic = obj.get_condition()
            new_save_position_dic[target_asset] = dic

        if new_save_position_dic:
            self.save_position_dic = new_save_position_dic
            save_json("multi_asset.json", self.save_position_dic)
        else:
            self.write_log(f"[save_condition] new_save_position_dic:{new_save_position_dic}!")

    def update_contracts(self):
        if self.time_update_contracts.can_work():
            self.future_manager.run_update()

            # for vt_symbol in self.future_manager.get_all_vt_symbols():
            for asset in self.do_assets_arr:
                vt_symbol = get_vt_key(asset + "_usdt", Exchange.OKEX5.value)
                contract = self.future_manager.get_contract(vt_symbol)
                if contract and vt_symbol not in self.symbol_exchanges.keys():
                    cash = self.start_cash
                    target_asset_val = 0
                    if asset in self.save_position_dic.keys():
                        asset_dic = self.save_position_dic[asset]
                        cash = asset_dic["cash"]
                        target_asset_val = asset_dic["target_asset_val"]

                    # contract, strategy, target_asset, cash=0, target_asset_amount=0, day_window=1, init_pos=0
                    self.symbol_exchanges[vt_symbol] = MultiAssetSymbolExchange(
                        contract, self, asset, cash=cash,
                        target_asset_amount=target_asset_val, day_window=1, init_pos=0)

    def update_day_work(self):
        '''
        每天要更新的数据
        '''
        pass

    def update_account(self):
        self.write_log("[update_account]")
        if self.time_update_contracts.can_work():
            if self.contract_exchange == Exchange.OKEX5.value:
                target_key = get_vt_key(self.contract_exchange, "usdt_all")
            else:
                target_key = get_vt_key(self.contract_exchange, "usdt")

            acct_te_target = self.get_account(target_key)
            if acct_te_target is not None:
                self.exchange_info["pos_usdt"] = acct_te_target.balance

            self.write_log(f"[update_account] exchange_info:{self.exchange_info}")

    def on_bbo_tick(self, bbo_ticker):
        # self.write_log(f"[on_bbo_tick] bbo_ticker:{bbo_ticker.__dict__}")
        if self.time_work_bbo.can_work():
            self.update_account()
            self.update_contracts()
            tickers = bbo_ticker.get_tickers()
            for tick in tickers:
                if tick.vt_symbol in self.symbol_exchanges.keys():
                    self.symbol_exchanges[tick.vt_symbol].on_tick(deepcopy(tick))
                # else:
                #     self.write_log(f"[on_bbo_tick] has new tick:{tick.vt_symbol}")

            self.update_day_work()

    def on_order(self, order):
        msg = f"[on_order] {order.vt_symbol}, {order.direction}, {order.price}, " \
              f"order.volume:{order.volume}, order.traded:{order.traded} order.status:{order.status}"
        self.write_log(msg)

        if order.vt_symbol in self.symbol_exchanges.keys():
            self.symbol_exchanges[order.vt_symbol].on_order(order)

        if order.traded > 0 and not order.is_active():
            self.write_important_log(msg)

            self.save_condition()

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        msg = '[trade detail] :{}'.format(trade.__dict__)
        self.write_important_log(msg)
        self.send_ding_msg(msg)
