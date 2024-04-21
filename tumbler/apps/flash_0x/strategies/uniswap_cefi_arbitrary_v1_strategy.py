# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
import random

from tumbler.constant import Direction
from tumbler.function import get_vt_key
from tumbler.apps.flash_0x.template import (
    Flash0xTemplate,
)
import tumbler.function.risk as risk
from tumbler.function import get_two_currency
from tumbler.constant import Exchange, TradeOrderSendType, CheckTradeAccountType
from tumbler.apps.cta_strategy.template import NewOrderSendModule
from tumbler.record.uniswap_client import UniswapClient
from tumbler.record.pancake_client import PancakeClient
from tumbler.record.bytom_client import BytomClient
from tumbler.record.huobi_client import HuobiClient
from tumbler.record.binance_client import BinanceClient
from tumbler.record.defi.tokens import EthNet


class UniswapCefiArbitraryV1Strategy(Flash0xTemplate):
    '''
    cefi 跟 defi 之间做套利
    '''
    author = "ipqhjjybj"
    class_name = "UniswapCefiArbitraryV1Strategy"

    symbol = "eth_usdt"
    defi_exchange = Exchange.PANCAKE.value
    exchange = Exchange.HUOBI.value

    vt_symbols_subscribe = []
    work_exchange_info = {}
    cover_exchange_info = {}
    secret_key = ""
    address = ""

    fixed_trade_usdt_amount = 1000
    profit_rate = 0.6
    fee_rate = 0.3

    delete_order_seconds = 1800

    parameters = [
        'strategy_name',
        'vt_symbols_subscribe',
        'symbol',
        'defi_exchange',
        'exchange',
        'secret_key',
        'address',
        'fixed_trade_usdt_amount',
        'profit_rate',
        'fee_rate',
    ]

    # 需要保存的运行时变量
    variables = [
        'inited',
        'trading'
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = [
        'work_exchange_info',
        'cover_exchange_info',
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(UniswapCefiArbitraryV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.profit_rate = self.profit_rate / 100.0
        self.fee_rate = self.fee_rate / 100.0

        self.vt_symbol = get_vt_key(self.symbol, self.exchange)
        target_symbol, base_symbol = get_two_currency(self.symbol)
        self.target_symbol = target_symbol
        self.base_symbol = base_symbol
        self.order_module = None

        if self.defi_exchange == Exchange.UNISWAP.value:
            self.client = UniswapClient(address=self.address, private_key=self.secret_key, network=EthNet.MainNet.value)
        elif self.defi_exchange == Exchange.BMC.value:
            self.client = BytomClient(address=self.address, private_key=self.secret_key, network=EthNet.BmcTestNet.value
                                      , provider="", version=2)
        else:
            self.client = PancakeClient(address=self.address, private_key=self.secret_key, network=EthNet.BscNet.value)

        self.defi_main_symbol = self.client.get_main_symbol()
        self.gas_symbol = f"{self.defi_main_symbol}_usdt".lower()

        if self.defi_main_symbol.lower() == 'ht':
            self.tick_client = HuobiClient(_apikey="", _secret_key="")
        else:
            self.tick_client = BinanceClient(_apikey="", _secret_key="")

        self.huobi_client = HuobiClient(_apikey="", _secret_key="")

        self.spot_tick_decorder = risk.TickDecorder(self.vt_symbol, self)
        self.can_update_account = risk.TimeWork(10)
        self.can_compute_spread = risk.TimeWork(3)
        self.can_update_gas_price = risk.TimeWork(60)
        self.can_update_defi_volume = risk.TimeWork(60)
        self.timer_can_work = risk.TimeWork(1)

        # check_approve
        self.can_check_approve = risk.TimeWork(60)
        self.now_approved = False
        self.approve_count = 0

        self.defi_buy_price = 0.0
        self.defi_sell_price = 0.0
        self.defi_volume = 0.0

        self.buy_spread = 0
        self.sell_spread = 0

        self.buy_profit = 0
        self.sell_profit = 0

        self.gas_price = 0
        self.last_eth_price = 0
        self.gas_fee_as_usdt = 0

        self.buy_check_dict = {}
        self.sell_check_dict = {}

        # for debug
        self.debug_work_num = 0
        self.debug_cover_num = 0
        self.debug_num = 5

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))

        self.init_order_module()

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))

    def init_order_module(self):
        if self.order_module is None:
            contract = self.get_contract(self.vt_symbol)
            self.order_module = NewOrderSendModule.init_order_send_module_from_contract(
                contract, self, init_pos=0, wait_seconds=10, send_order_type=TradeOrderSendType.MARKET.value,
                check_account_type=CheckTradeAccountType.NOT_CHECK_ACCOUNT.value)
            self.order_module.start()

    def update_account(self):
        try:
            self.write_log("[update_account] run update work!")
            balance_target = self.client.get_balance(self.target_symbol)
            balance_base = self.client.get_balance(self.base_symbol)

            self.work_exchange_info[self.target_symbol] = balance_target
            self.work_exchange_info[self.base_symbol] = balance_base

            target_key = self.get_account(get_vt_key(self.exchange, self.target_symbol))
            if target_key:
                self.cover_exchange_info[self.target_symbol] = target_key.available
                if self.order_module and self.spot_tick_decorder.tick:
                    limit_short_pos = target_key.available * 0.9
                    self.order_module.on_limit_account_short_pos(limit_short_pos)
                    self.write_log(f"[update_account] on_limit_account_short_pos:{limit_short_pos}")

            base_key = self.get_account(get_vt_key(self.exchange, self.base_symbol))
            if base_key:
                self.cover_exchange_info[self.base_symbol] = base_key.available
                if self.order_module and self.spot_tick_decorder.tick:
                    limit_long_pos = base_key.available * 0.9 / self.spot_tick_decorder.tick.ask_prices[0]
                    self.order_module.on_limit_account_long_pos(limit_long_pos)
                    self.write_log(f"[update_account] on_limit_account_long_pos:{limit_long_pos}")

            self.write_log(f"[update_account] work_exchange_info:{self.work_exchange_info}")
            self.write_log(f"[update_account] cover_exchange_info:{self.cover_exchange_info}")
        except Exception as ex:
            self.write_log(f"[update_account] ex:{ex}")

    def get_volume_from_usdt_val(self):
        try:
            if self.base_symbol in ["usdt", "usdc"]:
                price = self.spot_tick_decorder.tick.ask_prices[0]
            else:
                ticker = self.tick_client.get_currency_ticker(self.target_symbol)
                price = ticker.ask_prices[0]
            volume = self.fixed_trade_usdt_amount / price
            return volume
        except Exception as ex:
            self.write_log(f"[get_volume_from_usdt_val] ex:{ex}")
        return 0

    def get_defi_volume(self):
        if not self.defi_volume:
            self.defi_volume = self.get_volume_from_usdt_val()
            self.write_log(f"[get_defi_volume] self.defi_volume:{self.defi_volume}")
        else:
            if self.can_update_defi_volume.can_work():
                volume = self.get_volume_from_usdt_val()
                if volume:
                    self.defi_volume = volume
        return self.defi_volume

    def is_work_exchange_updated(self):
        return self.target_symbol in self.work_exchange_info.keys() and \
               self.base_symbol in self.work_exchange_info.keys()

    def update_gas_price(self):
        try:
            if not self.gas_fee_as_usdt or self.can_update_gas_price.can_work():
                if self.work_exchange_info[self.target_symbol] > self.get_defi_volume():
                    transaction, tx_params = self.client.simple_trade(
                        self.symbol, Direction.SHORT.value,
                        self.spot_tick_decorder.tick.ask_prices[0], self.get_defi_volume())
                elif self.work_exchange_info[self.base_symbol] > self.get_defi_volume() \
                        * self.spot_tick_decorder.tick.bid_prices[0]:
                    transaction, tx_params = self.client.simple_trade(
                        self.symbol, Direction.LONG.value,
                        self.spot_tick_decorder.tick.bid_prices[0], self.get_defi_volume()
                    )
                else:
                    self.write_log(f"[update_gas_price] money not enough! {self.work_exchange_info}")
                    return self.gas_fee_as_usdt

                gas = self.client.estimate_gas(transaction)
                self.write_log("[compute_spread] estimate_gas:{}".format(gas))

                self.gas_price = self.client.get_gas_price()
                tot_gas = self.gas_price * gas * 1.0 / (10 ** 18)
                self.write_log("[compute_spread] tot_gas:{}".format(tot_gas))

                ticker = self.tick_client.get_ticker(self.gas_symbol)
                if ticker:
                    self.last_eth_price = ticker.ask_prices[0]
                    self.gas_fee_as_usdt = self.last_eth_price * tot_gas
                    self.write_log(f"[compute_spread] gas_fee_as_usdt:{self.gas_fee_as_usdt} "
                                   f"last_eth_price:{self.last_eth_price}")
                else:
                    self.write_log(f"ticker: {self.gas_symbol} is None!")
        except Exception as ex:
            self.write_log("[update_gas_price] error ex:{}".format(ex))
        return self.gas_fee_as_usdt

    def compute_spread(self):
        if not self.is_work_exchange_updated():
            self.write_log(f"[compute_spread] exchange has not been updated!"
                           f" work_exchange_info:{self.work_exchange_info}")
            return False

        if not self.update_gas_price():
            self.write_log(f"[compute_spread] gas_fee_as_usdt:{self.gas_fee_as_usdt} has not updated!")
            return False

        try:
            self.defi_sell_price = self.client.get_sell_price(self.symbol, self.get_defi_volume())
            self.defi_buy_price = self.client.get_buy_price(self.symbol, self.get_defi_volume() * self.defi_sell_price)
            self.write_log(f"[compute_spread] defi_buy_price:{self.defi_buy_price} defi_sell_price:{self.defi_sell_price}")

            huobi_ticker = self.huobi_client.get_ticker(self.symbol)
            if huobi_ticker:
                self.write_log("[compute_spread]")

                self.sell_spread = self.defi_sell_price / huobi_ticker.ask_prices[0] - 1
                self.buy_spread = 1 - self.defi_buy_price / huobi_ticker.bid_prices[0]

                tot_fee_rate = 0.0006 + self.fee_rate + self.gas_fee_as_usdt / self.fixed_trade_usdt_amount

                self.sell_profit = self.sell_spread - tot_fee_rate
                self.buy_profit = self.buy_spread - tot_fee_rate

                self.write_log(f"[compute_spread] tot_fee_rate:{tot_fee_rate}")
                self.write_log(f"[compute_spread] sell_spread:{self.sell_spread} sell_profit:{self.sell_profit}")
                self.write_log(f"[compute_spread] buy_spread:{self.buy_spread} buy_profit:{self.buy_profit}")

                return self.sell_profit > self.profit_rate or self.buy_profit > self.profit_rate
        except Exception as ex:
            self.write_log(f"[compute_spread] ex:{ex}")
        return False

    def get_need_check_order_nums(self):
        '''
        获得当前需要查询hash的总数
        '''
        return len(self.buy_check_dict.keys()) + len(self.sell_check_dict.keys())

    def get_need_check_buy_order_nums(self):
        return len(self.buy_check_dict.keys())

    def get_need_check_sell_order_nums(self):
        return len(self.sell_check_dict.keys())

    def add_to_check_order(self, direction, tx_id, to_cover_volume):
        '''
        添加到查hash的队列里面
        '''
        now = time.time()
        self.write_log(f"[add_to_check_order] {direction} tx_id:{tx_id}, now:{now}, to_cover_volume:{to_cover_volume}!")
        if direction == Direction.LONG.value:
            self.write_log("[add_to_check_order] add to buy_check_dict!")
            self.buy_check_dict[tx_id] = (now, to_cover_volume)
        else:
            self.write_log("[add_to_check_order] add to sell_check_dict!")
            self.sell_check_dict[tx_id] = (now, to_cover_volume)

    def check_order_finished(self):
        '''
        检查hash是否完成
        '''
        for tx_id in list(self.buy_check_dict.keys()):
            try:
                transaction_info = self.client.get_transaction_receipt(tx_id)
                if transaction_info:
                    status = transaction_info["status"]
                    if status:
                        now_time, to_cover_volume = self.buy_check_dict.pop(tx_id)
                        self.write_log(f"[check_order_finished] pop buy_check_dict {now_time}, {to_cover_volume}")

                        # debug
                        if self.debug_cover_num > self.debug_num:
                            continue
                        self.debug_cover_num += 1

                        self.order_module.go_new_trade_pos(to_cover_volume * -1)

                        msg = f"go_new_trade_pos {to_cover_volume * -1}"
                        self.send_ding_msg(msg)
                    else:
                        now_time, to_cover_volume = self.buy_check_dict.pop(tx_id)
                        self.write_log(f"[check_order_finished] sell_check_dict {now_time}, {to_cover_volume} failed!")

                        msg = f"failed trade tx_id:{tx_id} ! stop trading now!"
                        self.write_log(msg)
                        self.send_ding_msg(msg)
                        self.trading = False
            except Exception as ex:
                self.write_log(f"[check_order_finished] buy_check_dict, {tx_id} ex:{ex}")

        for tx_id in list(self.sell_check_dict.keys()):
            try:
                transaction_info = self.client.get_transaction_receipt(tx_id)
                if transaction_info:
                    status = transaction_info["status"]
                    if status:
                        now_time, to_cover_volume = self.sell_check_dict.pop(tx_id)
                        self.write_log(f"[check_order_finished] pop sell_check_dict {now_time}, {to_cover_volume}")

                        # debug
                        if self.debug_cover_num > self.debug_num:
                            continue
                        self.debug_cover_num += 1

                        self.order_module.go_new_trade_pos(to_cover_volume)

                        msg = f"go_new_trade_pos {to_cover_volume}"
                        self.send_ding_msg(msg)
                    else:
                        now_time, to_cover_volume = self.sell_check_dict.pop(tx_id)
                        self.write_log(f"[check_order_finished] sell_check_dict {now_time}, {to_cover_volume} failed!")

                        msg = f"failed trade tx_id:{tx_id} ! stop trading now!"
                        self.write_log(msg)
                        self.send_ding_msg(msg)
                        self.trading = False
            except Exception as ex:
                self.write_log(f"[check_order_finished] sell_check_dict, {tx_id} ex:{ex}")

    def delete_old_orders(self):
        '''
        删除太久远的hash
        '''
        now = time.time()
        for dic in [self.buy_check_dict, self.sell_check_dict]:
            items = copy(list(dic.items()))
            for tx_hash, val in items:
                t, _ = val
                if now - t >= self.delete_order_seconds:
                    if tx_hash in dic.keys():
                        dic.pop(tx_hash)
                        self.write_log("[delete_old_orders] delete in tx_hash:{}".format(tx_hash))

    def check_work_exchange_account_enough(self, direction, price, volume):
        if direction == Direction.LONG.value:
            return price * volume <= self.work_exchange_info[self.base_symbol]
        else:
            return volume <= self.work_exchange_info[self.target_symbol]

    def trade(self):
        try:
            self.write_log(f"[trade] go work, sell_profit:{self.sell_profit} "
                           f"buy_profit:{self.buy_profit} "
                           f"profit_rate:{self.profit_rate}!")
            if self.sell_profit > self.profit_rate:
                # 所以是在 defi 卖出, 交易所买入
                if self.order_module.check_account(Direction.LONG.value, self.defi_buy_price, self.get_defi_volume()):
                    if self.check_work_exchange_account_enough(
                            Direction.SHORT.value, self.defi_buy_price, self.get_defi_volume()):
                        transaction, tx_params = self.client.simple_trade(
                            self.symbol, Direction.SHORT.value, self.spot_tick_decorder.tick.ask_prices[0],
                            self.get_defi_volume())

                        #debug
                        if self.debug_work_num > self.debug_num:
                            return
                        self.debug_work_num += 1

                        self.write_log(f"[trade] SHORT self.client.signed_and_send!:{transaction}")
                        tx_id = self.client.signed_and_send(transaction, tx_params)
                        if tx_id:
                            to_cover_volume = self.get_defi_volume()
                            self.add_to_check_order(Direction.SHORT.value, tx_id.hex(), to_cover_volume)
                            self.work_exchange_info[self.target_symbol] -= self.get_defi_volume()

                            msg = f"[trade] add_to_check_order {Direction.SHORT.value} {tx_id.hex()} {to_cover_volume}"
                            self.send_ding_msg(msg)
                        else:
                            self.write_log(f"[trade] short failed, tx_id:{tx_id} !")
                    else:
                        self.write_log(f"[trade] short work_exchange account not enough! {self.work_exchange_info}")
                else:
                    self.write_log(f"[trade] cover_exchange account not enough! {self.cover_exchange_info}")

            elif self.buy_profit > self.profit_rate:
                # 所以是在 defi 买入，交易所卖出
                if self.order_module.check_account(Direction.SHORT.value, self.defi_sell_price, self.get_defi_volume()):
                    if self.check_work_exchange_account_enough(
                            Direction.LONG.value, self.defi_sell_price, self.get_defi_volume()):
                        transaction, tx_params = self.client.simple_trade(
                            self.symbol, Direction.LONG.value, self.spot_tick_decorder.tick.bid_prices[0],
                            self.get_defi_volume())

                        # debug
                        if self.debug_work_num > self.debug_num:
                            return
                        self.debug_work_num += 1

                        self.write_log(f"[trade] LONG self.client.signed_and_send!:{transaction}")
                        tx_id = self.client.signed_and_send(transaction, tx_params)

                        if tx_id:
                            to_cover_volume = self.get_defi_volume()
                            self.add_to_check_order(Direction.LONG.value, tx_id.hex(), to_cover_volume)
                            self.work_exchange_info[self.base_symbol] -= self.get_defi_volume() * self.defi_sell_price

                            msg = f"[trade] add_to_check_order {Direction.LONG.value} {tx_id.hex()} {to_cover_volume}"
                            self.send_ding_msg(msg)
                        else:
                            self.write_log(f"[trade] long failed, tx_id:{tx_id} !")
                    else:
                        self.write_log(f"[trade] long work_exchange account not enough! {self.work_exchange_info}")
                else:
                    self.write_log(f"[trade] cover_exchange account not enough! {self.cover_exchange_info}")
            else:
                self.write_log(f"[trade] error, why spread:{self.buy_profit},{self.sell_profit}) "
                               f" < profit_rate:{self.profit_rate}!")
        except Exception as ex:
            self.write_log("[trade] error! ex:{}".format(ex))

    def on_tick(self, tick):
        self.write_log("[tick] tick.symbol:{} bid_price:{} ask_price:{}"
                       .format(tick.symbol, tick.bid_prices[0], tick.ask_prices[0]))

        if self.can_update_account.can_work():
            self.update_account()

        if not self.now_approved:
            if self.can_check_approve.can_work():
                self.approve_count += 1
                if self.approve_count < 2:
                    self.write_log(f"[on_tick] go to approve:{self.symbol}!")
                    self.now_approved = self.client.check_approve_and_go_approve(self.symbol)
                else:
                    self.write_log("[on_tick] some error may existed during approve!")
            return

        if self.timer_can_work.can_work():
            self.check_order_finished()
            self.delete_old_orders()

            if self.order_module and tick.vt_symbol == self.vt_symbol:
                self.spot_tick_decorder.update_tick(tick)
                if self.spot_tick_decorder.is_tick_ok():
                    self.order_module.on_tick(self.spot_tick_decorder.tick)

                    if self.get_need_check_order_nums() > 0:
                        self.write_log(f"[on_tick] get_need_check_order_nums:{self.get_need_check_order_nums()} exceed!"
                                       f" return !")
                        return

                    if self.trading and self.can_compute_spread.can_work() and self.compute_spread():
                        self.trade()
            else:
                self.init_order_module()

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if self.order_module and order.vt_symbol == self.vt_symbol:
            self.order_module.on_order(order)

        if order.traded > 0:
            self.output_important_log()

    def on_trade(self, trade):
        self.write_important_log('[trade detail] :{}'.format(trade.__dict__))

    def output_important_log(self):
        self.write_log(f"[output_important_log] {self.work_exchange_info}")
        self.write_log(f"[output_important_log] {self.cover_exchange_info}")
