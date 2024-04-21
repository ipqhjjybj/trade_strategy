# coding=utf-8
import os
from copy import copy, deepcopy
from datetime import datetime, timedelta

from tumbler.apps.alpha_trader.template import (
    AlphaTemplate
)

from tumbler.constant import Exchange, Interval, Direction
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function import get_vt_key
import tumbler.function.risk as risk
from tumbler.function.future_manager import UsdtContractManager
from tumbler.apps.cta_strategy.template import NewOrderSendModule


class SymbolExchange(object):
    def __init__(self, contract, strategy, hour_func, hour_window=1, day_window=1, init_pos=0):
        self.hour_func = hour_func
        self.contract = contract
        self.strategy = strategy
        self.hour_bg = BarGenerator(self.on_bar, window=hour_window, on_window_bar=self.on_hour_bar,
                                    interval=Interval.HOUR.value, quick_minute=2)
        self.day_bg = BarGenerator(None, window=day_window, on_window_bar=self.on_day_bar,
                                   interval=Interval.DAY.value, quick_minute=2)

        self.am_hour = ArrayManager(300)
        self.am_day = ArrayManager(40)

        self.tick_decorder = risk.TickDecorder(contract.vt_symbol, self)
        self.order_module = NewOrderSendModule.init_order_send_module_from_contract(
            contract, self.strategy, init_pos, wait_seconds=10)
        self.order_module.start()

        self.direction_condition = Direction.FORBID.value
        self.fixed = 0  # 交易的单位

        self.get_now_pos = self.order_module.get_now_pos
        self.get_target_pos = self.order_module.get_target_pos

    def is_inited(self):
        return self.am_day.inited and self.am_day.inited

    def on_tick(self, tick):
        self.tick_decorder.update_tick(tick)
        if self.contract.exchange in [Exchange.OKEX5.value]:
            # okex5 交易所，tick很慢。。。所以允许值高点。
            tick_ok_time = 60
        else:
            tick_ok_time = 10
        if self.tick_decorder.is_tick_ok(tick_ok_time):
            self.order_module.on_tick(self.tick_decorder.get_tick())

        if self.tick_decorder.is_tick_ok(tick_ok_time):
            self.hour_bg.update_tick(tick)
        else:
            self.strategy.write_log(f"[on_tick] {self.contract.vt_symbol} tick not ok:"
                                    f" tick.datetime:{tick.datetime}, now:{datetime.now()}")

    def on_bar(self, bar):
        self.order_module.on_bar(copy(bar))

        self.hour_bg.update_bar(bar)
        self.day_bg.update_bar(bar)

    def on_hour_bar(self, bar):
        self.am_hour.update_bar(bar)

        datetime_arr = self.am_hour.datetime_array
        if datetime_arr[-1] is not None and datetime_arr[-2] is not None:
            if datetime_arr[-2] + timedelta(hours=1) != datetime_arr[-1]:
                self.write_important_log(f"[on_hour_bar] {self.contract.vt_symbol}"
                                         f" -2:{datetime_arr[-2]} -1:{datetime_arr[-1]}")
        self.run_hour_bar()

    def is_change_too_small(self, trade_volume):
        now_target_pos = self.order_module.get_target_pos()
        if abs(trade_volume - now_target_pos) < max(abs(trade_volume), abs(now_target_pos)) * 0.01:
            self.strategy.write_log(f"[run_hour_bar] trade_volume:{trade_volume} "
                                    f"now_target_pos{now_target_pos} changes too small, not go to trade!")
            return True
        else:
            return False

    def run_hour_bar(self):
        if self.am_hour.inited and self.direction_condition in [Direction.LONG.value, Direction.SHORT.value]:
            algo_pos = self.hour_func(self.am_hour, self)
            trade_volume = self.fixed * algo_pos / self.contract.size
            self.write_log(f"[SymbolExchange] [run_hour_bar] assume {self.contract.vt_symbol} "
                           f"algo_pos:{algo_pos} trade_volume:{trade_volume}")

            if algo_pos > 0 and self.direction_condition in [Direction.BOTH.value, Direction.LONG.value]:
                self.write_log(f"[SymbolExchange] [run_hour_bar] real {self.contract.vt_symbol} "
                               f" trade_volume:{trade_volume}")
                if not self.is_change_too_small(trade_volume):
                    self.order_module.set_to_target_pos(trade_volume)

            elif algo_pos > 0 and self.direction_condition in [Direction.SHORT.value]:
                self.write_log(f"[SymbolExchange] [run_hour_bar] real {self.contract.vt_symbol} "
                               f" trade_volume:0 algo_pos > 0, {self.direction_condition}")
                self.order_module.set_to_target_pos(0)
            elif algo_pos == 0:
                self.write_log(f"[SymbolExchange] [run_hour_bar] real {self.contract.vt_symbol} "
                               f" trade_volume: 0")
                self.order_module.set_to_target_pos(0)
            elif algo_pos < 0 and self.direction_condition in [Direction.BOTH.value, Direction.SHORT.value]:
                self.write_log(f"[SymbolExchange] [run_hour_bar] real {self.contract.vt_symbol} "
                               f" trade_volume: {trade_volume}")
                if not self.is_change_too_small(trade_volume):
                    self.order_module.set_to_target_pos(trade_volume)
            elif algo_pos < 0 and self.direction_condition in [Direction.LONG.value]:
                self.write_log(f"[SymbolExchange] [run_hour_bar] real {self.contract.vt_symbol} "
                               f" trade_volume:0 algo_pos < 0, {self.direction_condition}")
                self.order_module.set_to_target_pos(0)

    def on_day_bar(self, bar):
        self.am_day.update_bar(bar)

        datetime_arr = self.am_day.datetime_array
        if datetime_arr[-1] is not None and datetime_arr[-2] is not None:
            if datetime_arr[-2] + timedelta(days=1) != datetime_arr[-1]:
                self.write_log(f"[on_day_bar] {self.contract.vt_symbol}"
                               f" -2:{datetime_arr[-2]} -1:{datetime_arr[-1]}")

    def run_day_func(self, func):
        return func(self.am_day, self)

    def set_direction_condition_and_amount(self, direction, trade_usdt_amount):
        self.direction_condition = direction

        self.fixed = trade_usdt_amount / self.tick_decorder.tick.bid_prices[0]
        self.write_log(f"[SymbolExchange] [set_direction_condition_and_amount]: "
                       f"{self.contract.vt_symbol} trade_usdt_amount:{trade_usdt_amount} "
                       f"fixed:{self.fixed} {direction}")

        if direction == Direction.FORBID.value:
            self.write_log(f"[SymbolExchange] [set_direction_condition_and_amount] "
                           f"{self.contract.vt_symbol} go to pos 0!")
            self.order_module.set_to_target_pos(0)
        elif direction in [Direction.LONG.value, Direction.SHORT.value]:
            self.run_hour_bar()

    def is_day_ready(self):
        tmp_datetime = self.am_day.datetime_array[-1]
        # self.write_log(f"[SymbolExchange] [is_day_ready] {self.contract.vt_symbol} tmp_datetime:{tmp_datetime} "
        #                f" status:{tmp_datetime and tmp_datetime.day == (datetime.now()- timedelta(days=1)).day}")
        return tmp_datetime and tmp_datetime.day == (datetime.now() - timedelta(days=1)).day

    def on_order(self, order):
        if order.vt_symbol == self.contract.vt_symbol:
            msg = f"[SymbolExchange][on_order] {order.vt_symbol}, {order.direction}, {order.price}, " \
                  f"order.volume:{order.volume}, order.traded:{order.traded}"
            self.write_log(msg)
            self.order_module.on_order(order)

    def on_trade(self, trade):
        pass

    def write_log(self, msg):
        self.strategy.write_log(msg)

    def write_important_log(self, msg):
        self.strategy.write_important_log(msg)


class AlphaCsiV3Strategy(AlphaTemplate):
    '''
    V2 是直接采用内存内计算的方案，
    1.首先记载历史的 bar
    2.自己合成 day_bar, hour_bar
    '''
    author = "ipqhjjybj"
    class_name = "AlphaCsiV3Strategy"

    contract_exchange = Exchange.HUOBIU.value
    inst_type = ""
    hour_window = 1
    day_window = 1

    day_func = None
    hour_func = None
    select_num = 15
    keep_num = 10
    per_trade_usdt_amount = 100

    support_long = True
    support_short = False
    support_mul_amount = True

    exchange_info = {}

    # 参数列表
    parameters = [
        'strategy_name',
        'class_name',
        'author',
        'contract_exchange',
        'inst_type',
        'hour_func',
        'hour_window',
        'day_func',
        'day_window',
        'keep_num',
        'select_num',
        'support_long',
        'support_short',
        'support_mul_amount',
        'per_trade_usdt_amount',  # 固定大小的下单USDT数量
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AlphaCsiV3Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.contract_tick_dict = {}
        self.future_manager = UsdtContractManager(self.contract_exchange, self, self.inst_type)
        self.order_module_dict = {}

        self.today_long_coins = []
        self.today_short_coins = []
        self.pre_day_long_coins = []
        self.pre_day_short_coins = []

        self.time_read_coins_time = risk.TimeWork(5)

        self.symbol_exchanges = {}

        self.exchange_info = {}

        self.bef_update_day = -1

        self.time_work_bbo = risk.TimeWork(2)
        self.time_update_contracts = risk.TimeWork(10)
        self.time_update_accounts = risk.TimeWork(10)
        self.time_update_per_trade = risk.TimeWork(60 * 60 * 24)

        self.positions_dic = {}

    def on_init(self):
        self.write_log("[on_init]")

        self.positions_dic = self.load_current_positions()
        self.subscribe_bbo_exchanges(self.contract_exchange, [self.inst_type])

    def on_start(self):
        self.write_log("[on_start]")

        self.update_contracts()

    def on_stop(self):
        self.write_log("[on_stop]")

    def update_contracts(self):
        if self.time_update_contracts.can_work():
            self.future_manager.run_update()
            for vt_symbol in self.future_manager.get_all_vt_symbols():
                if vt_symbol not in self.symbol_exchanges.keys():
                    contract = self.future_manager.get_contract(vt_symbol)
                    if contract:
                        init_pos = self.positions_dic.get(contract.vt_symbol, 0)
                        se = SymbolExchange(contract, self, self.hour_func, self.hour_window,
                                            self.day_window, init_pos=init_pos)
                        self.symbol_exchanges[vt_symbol] = se

                        bars = self.load_bars_from_mysql(vt_symbol, Interval.HOUR.value,
                                                         datetime.now() - timedelta(days=60), datetime.now())

                        start_time = None
                        end_time = None
                        if bars:
                            start_time = bars[0].datetime
                            end_time = bars[-1].datetime
                        self.write_log(f"[update_contracts] load_bars_from_mysql hour "
                                       f" {vt_symbol}, {start_time}, {end_time}")
                        # 直接导
                        for bar in bars:
                            se.on_bar(bar)

                        if len(bars):
                            # 补充 minute 数据
                            bars = self.load_bars_from_mysql(vt_symbol, Interval.MINUTE.value,
                                                             bars[-1].datetime + timedelta(hours=1), datetime.now())

                            start_time = None
                            end_time = None
                            if bars:
                                start_time = bars[0].datetime
                                end_time = bars[-1].datetime
                            self.write_log(f"[update_contracts] load_bars_from_mysql min "
                                           f" {vt_symbol}, {start_time}, {end_time}")
                            for bar in bars:
                                se.on_bar(bar)

                    else:
                        self.write_log(f"[update_contracts] why contract{contract} vt_symbol:{vt_symbol}")

    def update_account(self):
        self.write_log("[update_account]")
        if self.time_update_accounts.can_work():
            if self.contract_exchange == Exchange.OKEX5.value:
                target_key = get_vt_key(self.contract_exchange, "usdt_all")
            else:
                target_key = get_vt_key(self.contract_exchange, "usdt")

            acct_te_target = self.get_account(target_key)
            if acct_te_target is not None:
                self.exchange_info["pos_usdt"] = acct_te_target.balance
                if self.time_update_per_trade.can_work():
                    if self.support_mul_amount:
                        self.per_trade_usdt_amount = self.exchange_info["pos_usdt"] / self.keep_num
                        self.write_log(f"[update_account] update per_trade_usdt_amount:{self.per_trade_usdt_amount}")
                    else:
                        self.write_log(f"[update_account] not suppert to update per_trade_usdt_amount!")
            self.write_log(f"[update_account] exchange_info:{self.exchange_info}")

    def flush_day_work(self, val_arr):
        if not os.path.exists(".tumbler/day_result"):
            os.mkdir(".tumbler/day_result")
        d = datetime.now().strftime("%Y-%m-%d")
        f = open(f".tumbler/day_result/{d}.csv", "w")
        for val, symbol in val_arr:
            line = f"{symbol},{val}"
            f.write(line + "\n")
        f.close()

    def update_day_work(self):
        '''
        每天更新今天需要交易的票
        '''
        # 提前1分钟开始算今天的票
        now = datetime.now() + timedelta(minutes=1)
        if self.time_read_coins_time.can_work() and 8 <= now.hour and self.bef_update_day != now.day:
            all_ready = True
            for vt_symbol in self.future_manager.get_all_vt_symbols():
                symbol_exchange = self.symbol_exchanges.get(vt_symbol, None)
                if symbol_exchange:
                    if not symbol_exchange.is_day_ready():
                        all_ready = False
                        self.write_log(f"[update_day_work] {vt_symbol} is not day ready!")

            if all_ready:
                self.write_log(f"[update_day_work] now all_ready:{all_ready}")
                ret = []
                for vt_symbol in self.future_manager.get_all_vt_symbols():
                    symbol_exchange = self.symbol_exchanges.get(vt_symbol, None)
                    if symbol_exchange and symbol_exchange.is_inited():
                        val = symbol_exchange.run_day_func(self.day_func)
                        ret.append((val, vt_symbol))
                    else:
                        self.write_log(f"[update_day_work] symbol_exchange:{vt_symbol} is not inited!")

                ret.sort(reverse=True)
                self.flush_day_work(ret)

                ret = [x[1] for x in ret]

                self.pre_day_long_coins = copy(self.today_long_coins)
                self.pre_day_short_coins = copy(self.today_short_coins)

                select_long_coins = ret[:self.select_num]
                select_short_coins = ret[-self.select_num:]

                still_has_long_coins = [x for x in self.pre_day_long_coins if x in select_long_coins]
                still_has_short_coins = [x for x in self.pre_day_short_coins if x in select_short_coins]

                not_in_long_coins = [x for x in select_long_coins if x not in self.pre_day_long_coins]
                not_in_short_coins = [x for x in select_short_coins if x not in self.pre_day_short_coins]

                self.today_long_coins = still_has_long_coins + not_in_long_coins[
                                                               :self.keep_num - len(still_has_long_coins)]
                self.today_short_coins = still_has_short_coins + not_in_short_coins[
                                                                 -(self.keep_num - len(still_has_short_coins)):]

                td_vt_symbols = set(self.today_long_coins + self.today_short_coins)
                pd_vt_symbols = set(self.pre_day_long_coins + self.pre_day_short_coins)

                to_new_add_coins = [x for x in td_vt_symbols if x not in pd_vt_symbols]
                to_remove_coins = [x for x in pd_vt_symbols if x not in td_vt_symbols]

                self.write_log("[update_day_work] today_long_coins:{}".format(self.today_long_coins))
                self.write_log("[update_day_work] today_short_coins:{}".format(self.today_short_coins))

                self.write_log(f"[update_day_work] to_new_add_coins:{to_new_add_coins}")
                self.write_log(f"[update_day_work] to_remove_coins:{to_remove_coins}")

                self.bef_update_day = now.day

                # run amount!
                for vt_symbol in to_remove_coins:
                    symbol_exchange = self.symbol_exchanges.get(vt_symbol, None)
                    if symbol_exchange:
                        symbol_exchange.set_direction_condition_and_amount(
                            Direction.FORBID.value, 0)

                for vt_symbol in to_new_add_coins:
                    if self.support_long:
                        if vt_symbol in self.today_long_coins and vt_symbol not in self.pre_day_long_coins:
                            symbol_exchange = self.symbol_exchanges.get(vt_symbol, None)
                            if symbol_exchange:
                                symbol_exchange.set_direction_condition_and_amount(
                                    Direction.LONG.value, self.per_trade_usdt_amount)

                    if self.support_short:
                        if vt_symbol in self.today_short_coins and vt_symbol not in self.pre_day_short_coins:
                            symbol_exchange = self.symbol_exchanges.get(vt_symbol, None)
                            if symbol_exchange:
                                symbol_exchange.set_direction_condition_and_amount(
                                    Direction.SHORT.value, self.per_trade_usdt_amount)

                # 去清空历史遗留positions仓位
                for vt_symbol in self.positions_dic.keys():
                    pos = self.positions_dic.get(vt_symbol, 0)
                    symbol_exchange = self.symbol_exchanges.get(vt_symbol, None)
                    if symbol_exchange and abs(pos) > 1e-8 and vt_symbol not in td_vt_symbols:
                        symbol_exchange.set_direction_condition_and_amount(
                            Direction.FORBID.value, 0)

    def on_tick(self, tick):
        self.write_log("[on_tick] why has this? tick:{}".format(tick.__dict__))

    def on_bbo_tick(self, bbo_ticker):
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

            if order.traded > 0:
                now_pos = self.symbol_exchanges[order.vt_symbol].get_now_pos()
                self.positions_dic[order.vt_symbol] = now_pos
                self.flush_current_positions(self.positions_dic)

        if order.traded > 0 and not order.is_active():
            self.write_important_log(msg)

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        msg = '[trade detail] :{}'.format(trade.__dict__)
        self.write_important_log(msg)
        self.send_ding_msg(msg)
