# coding=utf-8

from datetime import datetime, timedelta
from collections import defaultdict
from copy import copy, deepcopy
import time

from tumbler.constant import Direction, Offset, Interval, Status, TradeOrderSendType
from tumbler.constant import CheckTradeAccountType
from tumbler.function import FilePrint, get_round_order_price, get_from_vt_key
from tumbler.object import OrderData
import tumbler.function.risk as risk
from tumbler.service import ding_talk_service
from tumbler.data import data_client_dict


class CtaSignal(object):
    def __init__(self):
        self.signal_pos = 0

    def on_tick(self, tick):
        pass

    def on_bar(self, bar):
        pass

    def set_signal_pos(self, pos):
        self.signal_pos = pos

    def get_signal_pos(self):
        return self.signal_pos


class OrderSendModule(object):
    """
    需要传入 运行前需要 start,  运行时需要调用 on_tick, on_order 函数
        set_to_target_pos  是设置要买到多少

    """

    def __init__(self, strategy, symbol, exchange, price_tick, volume_tick, init_pos, wait_seconds=10):
        self.strategy = strategy
        self.symbol = symbol
        self.exchange = exchange
        self.price_tick = price_tick
        self.volume_tick = volume_tick

        self.wait_seconds = wait_seconds

        self.order_dict = {}

        self.now_pos = init_pos
        self.target_pos = init_pos
        self.ticker = None

        self.cancel_dict_times_count = defaultdict(int)

        self.retry_cancel_send_num = 5

        self.order_aggr = 0  # 撤销追涨次数

        self.trading = False

        self.pos = 0

        self.avg_price = 0

    @staticmethod
    def init_order_send_module_from_contract(contract, strategy, init_pos, wait_seconds=10):
        return OrderSendModule(strategy, contract.symbol, contract.exchange, contract.price_tick,
                               contract.volume_tick, init_pos, wait_seconds)

    def get_now_pos(self):
        return self.now_pos

    def get_run_pos(self):
        return self.pos

    def get_target_pos(self):
        return self.target_pos

    def set_price_tick(self, price_tick):
        self.price_tick = price_tick

    def set_volume_tick(self, volume_tick):
        self.volume_tick = volume_tick

    def get_already_send_volume(self):
        buy_volume, sell_volume = 0, 0
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                if order.direction == Direction.LONG.value:
                    buy_volume += order.volume - order.traded
                else:
                    sell_volume += order.volume - order.traded
        return buy_volume, sell_volume

    def set_to_target_pos(self, pos):
        # 设置目标仓位
        self.target_pos = pos

    def go_new_trade_pos(self, pos, direct_trade=True):
        self.target_pos = self.target_pos - pos
        if direct_trade:
            self.to_target_pos()

    def cancel_pending_orders(self):
        need_cancel_sets = []
        now = time.time()
        for vt_order_id, order in self.order_dict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            if now - order_time > self.wait_seconds * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.strategy.write_log("[OrderSendModule] [cancel_pending_orders] {} set to cancel!"
                                        .format(order.order_id))
                need_cancel_sets.append(order.vt_order_id)

        self.cancel_sets_order(need_cancel_sets)

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            self.strategy.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.order_dict[vt_order_id]
                    del self.order_dict[vt_order_id]

                    self.strategy.write_log("[OrderSendModule] [cancel_sets_order] {} is removed!".format(vt_order_id))

    def determine_price(self, direction):
        inc_price = 0.001 * self.ticker.ask_prices[0] * self.order_aggr
        max_inc_price = 0.005 * self.ticker.ask_prices[0]

        inc_price = min(max_inc_price, inc_price)
        if direction == Direction.LONG.value:
            price = self.ticker.ask_prices[0] + inc_price
        else:
            price = self.ticker.bid_prices[0] - inc_price

        price = get_round_order_price(price, self.price_tick)
        return price

    def to_target_pos(self):
        if self.trading and self.ticker:
            buy_volume, sell_volume = self.get_already_send_volume()
            chazhi = self.target_pos - self.now_pos

            if chazhi > 0:
                uu_volume = chazhi - buy_volume
                if uu_volume > 0 and not self.order_dict:
                    if self.order_aggr < 10:
                        price = self.determine_price(Direction.LONG.value)
                        volume = get_round_order_price(uu_volume, self.volume_tick)

                        list_orders = self.strategy.buy(self.symbol, self.exchange, price, volume)
                        for vt_order_id, order in list_orders:
                            self.order_dict[vt_order_id] = copy(order)
                    else:
                        msg = f"[to_target_pos] {self.symbol} {self.exchange} order_aggr:{self.order_aggr} error!"
                        self.strategy.write_log(msg)

            elif chazhi < 0:
                uu_volume = chazhi + sell_volume
                if uu_volume < 0 and not self.order_dict:
                    if self.order_aggr < 10:
                        price = self.determine_price(Direction.SHORT.value)
                        volume = get_round_order_price(abs(uu_volume), self.volume_tick)

                        list_orders = self.strategy.sell(self.symbol, self.exchange, price, volume)
                        for vt_order_id, order in list_orders:
                            self.order_dict[vt_order_id] = copy(order)
                    else:
                        msg = f"[to_target_pos] {self.symbol} {self.exchange} order_aggr:{self.order_aggr} error!"
                        self.strategy.write_log(msg)

    def start(self):
        self.trading = True

    def stop(self):
        self.trading = False

    def on_tick(self, tick):
        if tick.bid_prices[0] > 0:
            self.ticker = copy(tick)

        if self.ticker:
            self.cancel_pending_orders()
            self.to_target_pos()

    def on_order(self, order):
        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                    self.now_pos += new_traded

                    self.compute_avg_price(order.price, new_traded, order.direction)

                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                    self.now_pos -= new_traded

                    self.compute_avg_price(order.price, new_traded, order.direction)

                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

            # 这里判断是否是 第一次买卖，通过单子是否是撤单状态
            # 如果是撤单状态，以后单子都是追买状态， price + 0.001 * ticker.price
            # 如果是全部成交状态，以后单子都是 + 0 ticker.price
            if not order.is_active():
                if order.status == Status.CANCELLED.value:
                    self.order_aggr += 1

                elif order.status == Status.ALLTRADED.value:
                    self.order_aggr = 0

            self.strategy.write_log("[OrderSendModule] [on_order] order_aggr:{}".format(self.order_aggr))

    def compute_avg_price(self, new_trade_price, new_trade_volume, new_trade_direction):
        if new_trade_direction == Direction.LONG.value:
            if self.pos >= 0:
                self.avg_price = (self.avg_price * self.pos + new_trade_price * new_trade_volume) / (
                        self.pos + new_trade_volume)
                self.pos += new_trade_volume
            else:
                if abs(self.pos) < new_trade_volume:
                    self.avg_price = new_trade_price
                self.pos += new_trade_volume

        else:
            if self.pos > 0:
                if self.pos < new_trade_volume:
                    self.avg_price = new_trade_price
                self.pos -= new_trade_volume
            else:
                self.avg_price = (self.avg_price * abs(self.pos) + new_trade_price * new_trade_volume) / (
                        abs(self.pos) + new_trade_volume)
                self.pos -= new_trade_volume

        self.strategy.write_log("run_pos:{}, avg_price:{}".format(self.pos, self.avg_price))

    def get_avg_price(self):
        return self.avg_price


class NewOrderSendModule(object):
    """
    需要传入 运行前需要 start,  运行时需要调用 on_tick, on_order 函数
        set_to_target_pos  是设置要买到多少

    """

    def __init__(self, strategy, contract, init_pos, wait_seconds=10,
                 send_order_type=TradeOrderSendType.LIMIT.value,
                 check_account_type=CheckTradeAccountType.NOT_CHECK_ACCOUNT.value):
        self.strategy = strategy
        self.symbol = contract.symbol
        self.exchange = contract.exchange
        self.contract = copy(contract)
        self.send_order_type = send_order_type
        self.check_account_type = check_account_type

        self.wait_seconds = wait_seconds

        self.order_dict = {}

        self.now_pos = init_pos
        self.target_pos = init_pos

        self.ticker = None
        self.last_bar = None
        self.last_price = 0.0

        self.cancel_dict_times_count = defaultdict(int)
        self.retry_cancel_send_num = 5  # 尝试重复发单次数
        self.order_aggr = 0  # 撤销追涨次数

        self.protect_rate = 5  # 追单保护下单百分比
        self.protect_buy_price = 0
        self.protect_sell_price = 0

        self.pos = 0
        self.avg_price = 0

        self.limit_long_pos = 0
        self.limit_short_pos = 0

        self.trading = False

    @staticmethod
    def init_order_send_module_from_contract(contract, strategy, init_pos,
                                             wait_seconds=10,
                                             send_order_type=TradeOrderSendType.LIMIT.value,
                                             check_account_type=CheckTradeAccountType.NOT_CHECK_ACCOUNT.value):
        return NewOrderSendModule(strategy, contract, init_pos, wait_seconds, send_order_type, check_account_type)

    def on_limit_account_long_pos(self, limit_long_pos):
        self.limit_long_pos = limit_long_pos

    def on_limit_account_short_pos(self, limit_short_pos):
        self.limit_short_pos = limit_short_pos

    def get_now_pos(self):
        return self.now_pos

    def get_run_pos(self):
        return self.pos

    def get_target_pos(self):
        return self.target_pos

    def get_already_send_volume(self):
        buy_volume, sell_volume = 0, 0
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                if order.direction == Direction.LONG.value:
                    buy_volume += order.volume - order.traded
                else:
                    sell_volume += order.volume - order.traded
        return buy_volume, sell_volume

    def update_protect_price(self, target_pos, now_pos):
        self.strategy.write_log(f"[update_protect_price] target_pos:{target_pos} {now_pos}")
        if abs(target_pos - now_pos) > 1e-18:
            if self.last_price:
                if target_pos > now_pos:
                    self.protect_buy_price = self.last_price
                elif target_pos < now_pos:
                    self.protect_sell_price = self.last_price
            else:
                self.protect_buy_price = 0
                self.protect_sell_price = 0

    def check_price(self, price, direction):
        if direction == Direction.LONG.value:
            if self.protect_buy_price and price < self.protect_buy_price * (1 + self.protect_rate / 100.0):
                return True
            else:
                self.strategy.write_log(f"[check_price] {price},{direction} "
                                        f"{self.protect_buy_price} "
                                        f"{self.protect_buy_price * (1 + self.protect_rate / 100.0)}")
                return False
        elif direction == Direction.SHORT.value:
            if self.protect_sell_price and price > self.protect_sell_price * (1 - self.protect_rate / 100.0):
                return True
            else:
                self.strategy.write_log(f"[check_price] {price},{direction} "
                                        f"{self.protect_sell_price} "
                                        f"{self.protect_sell_price * (1 - self.protect_rate / 100.0)}")
                return False
        else:
            self.strategy.write_log(f"[check_price] unknown direction! {price},{direction}!")
            return False

    def set_to_target_pos(self, pos):
        # 更新保护价格
        self.update_protect_price(pos, self.now_pos)

        # 设置目标仓位
        self.target_pos = pos

        # debug
        self.strategy.write_log(f"[set_to_target_pos] {self.symbol} {self.target_pos}")

    def go_new_trade_pos(self, pos, direct_trade=True):
        # 更新保护价格
        self.update_protect_price(self.target_pos + pos, self.now_pos)

        self.target_pos = self.target_pos + pos

        # debug
        self.strategy.write_log(f"[go_new_trade_pos] {self.symbol} {self.target_pos}")

        if direct_trade:
            self.to_target_pos()

    def is_trade_finished(self):
        return abs(self.target_pos - self.now_pos) < self.contract.min_volume

    def cancel_pending_orders(self):
        need_cancel_sets = []
        now = time.time()
        for vt_order_id, order in self.order_dict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            if now - order_time > self.wait_seconds * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.strategy.write_log("[OrderSendModule] [cancel_pending_orders] {} set to cancel!"
                                        .format(order.order_id))
                need_cancel_sets.append(order.vt_order_id)

        self.cancel_sets_order(need_cancel_sets)

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            self.strategy.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.order_dict[vt_order_id]
                    del self.order_dict[vt_order_id]

                    self.strategy.write_log("[OrderSendModule] [cancel_sets_order] {} is removed!".format(vt_order_id))

    def determine_price(self, direction):
        if self.send_order_type == TradeOrderSendType.POST_ONLY.value:
            if direction == Direction.LONG.value:
                price = self.ticker.bid_prices[0]
            else:
                price = self.ticker.ask_prices[0]
            return price
        else:
            inc_price = 0.001 * self.ticker.ask_prices[0] * self.order_aggr
            max_inc_price = 0.005 * self.ticker.ask_prices[0]

            inc_price = min(max_inc_price, inc_price)
            if self.send_order_type == TradeOrderSendType.MARKET.value:
                inc_price = max_inc_price
            if direction == Direction.LONG.value:
                price = self.ticker.ask_prices[0] + inc_price
            else:
                price = self.ticker.bid_prices[0] - inc_price

            price = get_round_order_price(price, self.contract.price_tick)
            return price

    def determine_volume(self, volume, direction):
        if volume < 0:
            self.strategy.write_log(f"[determine_volume] [error] "
                                    f"volume:{volume} < 0, {direction} maybe something error!")
            return 0
        # 还需要加入 volume 对于 depth 的检测
        volume = get_round_order_price(volume, self.contract.volume_tick)
        return volume

    def check_account(self, direction, price, volume):
        if self.check_account_type is CheckTradeAccountType.NOT_CHECK_ACCOUNT.value:
            return True
        else:
            buy_volume, sell_volume = self.get_already_send_volume()
            if direction == Direction.LONG.value and 0 < volume + buy_volume <= self.limit_long_pos:
                self.strategy.write_log(f"[check_account] long, 0 < volume:{volume} + buy_volume:{buy_volume} "
                                        f"< limit:{self.limit_long_pos}")
                return True
            elif direction == Direction.SHORT.value and 0 < volume + sell_volume <= self.limit_short_pos:
                self.strategy.write_log(f"[check_account] short, 0 < volume:{volume} + sell_volume:{sell_volume} "
                                        f"< limit:{self.limit_short_pos}")
                return True
            self.strategy.write_log(f"[check_account] failed!{self.symbol},{direction},{price},{volume}")
            return False

    def to_target_pos(self):
        if self.trading and self.ticker:
            buy_volume, sell_volume = self.get_already_send_volume()
            chazhi = self.target_pos - self.now_pos

            # self.strategy.write_log(f"[to_target_pos] symbol:{self.symbol} buy_volume:{buy_volume}"
            #                         f" sell_volume:{sell_volume} chazhi:{chazhi} "
            #                         f" contract.min_volume:{self.contract.min_volume}"
            #                         f" target_pos:{self.target_pos} now_pos:{self.now_pos}")
            if chazhi >= self.contract.min_volume:
                uu_volume = chazhi - buy_volume
                if uu_volume > 0 and not self.order_dict:
                    if self.order_aggr < 10:
                        price = self.determine_price(Direction.LONG.value)
                        volume = self.determine_volume(uu_volume, Direction.LONG.value)

                        if self.check_price(price, Direction.LONG.value):
                            if self.check_account(Direction.LONG.value, price, volume):
                                list_orders = self.strategy.buy(self.symbol, self.exchange, price, volume)
                                for vt_order_id, order in list_orders:
                                    self.order_dict[vt_order_id] = copy(order)
                            else:
                                self.strategy.write_log("[to_target_pos] check account fail! not send orders!")
                        else:
                            self.strategy.write_log("[to_target_pos] check_price fail! not send orders!")
                    else:
                        msg = f"[to_target_pos] {self.symbol} {self.exchange} order_aggr:{self.order_aggr} long error!"
                        self.strategy.write_log(msg)
                        self.strategy.send_ding_msg(msg)

            elif chazhi <= -1 * self.contract.min_volume:
                uu_volume = chazhi + sell_volume
                if uu_volume < 0 and not self.order_dict:
                    if self.order_aggr < 10:
                        price = self.determine_price(Direction.SHORT.value)
                        volume = self.determine_volume(abs(uu_volume), Direction.SHORT.value)

                        if self.check_price(price, Direction.SHORT.value):
                            if self.check_account(Direction.SHORT.value, price, volume):
                                list_orders = self.strategy.sell(self.symbol, self.exchange, price, volume)
                                for vt_order_id, order in list_orders:
                                    self.order_dict[vt_order_id] = copy(order)
                            else:
                                self.strategy.write_log("[to_target_pos] check account fail! not send orders!")
                        else:
                            self.strategy.write_log("[to_target_pos] check_price fail! not send orders!")
                    else:
                        msg = f"[to_target_pos] {self.symbol} {self.exchange} order_aggr:{self.order_aggr} short error!"
                        self.strategy.write_log(msg)
                        self.strategy.send_ding_msg(msg)

    def start(self):
        self.trading = True
        self.strategy.write_log(f"[orderModule][start trading] {self.symbol}, {self.exchange}!")

    def stop(self):
        self.trading = False
        self.strategy.write_log(f"[orderModule][stop trading] {self.symbol}, {self.exchange}!")

    def on_tick(self, tick):
        # debug
        # if self.symbol == "chr_usdt":
        #     self.strategy.write_log(f"[NewOrderSendModule] new_tick:{tick.vt_symbol} "
        #                             f" {tick.bid_prices[0]} {tick.ask_prices[0]}")
        if tick.bid_prices[0] > 0:
            self.ticker = deepcopy(tick)
            self.last_price = tick.bid_prices[0]

        if self.ticker:
            self.cancel_pending_orders()
            self.to_target_pos()

    def on_bar(self, bar):
        self.last_bar = copy(bar)
        self.last_price = bar.close_price

    def on_order(self, order):
        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                    self.now_pos += new_traded

                    self.compute_avg_price(order.price, new_traded, order.direction)

                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded

                if new_traded > 0:
                    self.now_pos -= new_traded

                    self.compute_avg_price(order.price, new_traded, order.direction)

                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

            # 这里判断是否是 第一次买卖，通过单子是否是撤单状态
            # 如果是撤单状态，以后单子都是追买状态， price + 0.001 * ticker.price
            # 如果是全部成交状态，以后单子都是 + 0 ticker.price
            if not order.is_active():
                if order.status == Status.CANCELLED.value:
                    self.order_aggr += 1

                elif order.status == Status.ALLTRADED.value:
                    self.order_aggr = 0

            self.strategy.write_log("[OrderSendModule] [on_order] order_aggr:{}".format(self.order_aggr))

    def compute_avg_price(self, new_trade_price, new_trade_volume, new_trade_direction):
        if new_trade_direction == Direction.LONG.value:
            if self.pos >= 0:
                self.avg_price = (self.avg_price * self.pos + new_trade_price * new_trade_volume) / (
                        self.pos + new_trade_volume)
                self.pos += new_trade_volume
            else:
                if abs(self.pos) < new_trade_volume:
                    self.avg_price = new_trade_price
                self.pos += new_trade_volume

        else:
            if self.pos > 0:
                if self.pos < new_trade_volume:
                    self.avg_price = new_trade_price
                self.pos -= new_trade_volume
            else:
                self.avg_price = (self.avg_price * abs(self.pos) + new_trade_price * new_trade_volume) / (
                        abs(self.pos) + new_trade_volume)
                self.pos -= new_trade_volume

        self.strategy.write_log("run_pos:{}, avg_price:{}".format(self.pos, self.avg_price))

    def get_avg_price(self):
        return self.avg_price


class CtaTemplate(object):
    author = ""
    class_name = "CtaTemplate"

    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"
    vt_symbol = "btc_usdt.BINANCE"
    pos = 0

    exchange_info = {}

    inited = False
    trading = False

    # 订阅行情 
    vt_symbols_subscribe = []

    # 参数列表
    parameters = []

    # 运行时  重点变量列表
    variables = []

    def __init__(self, cta_engine, strategy_name, settings):
        # 设置策略的参数
        if settings:
            d = self.__dict__
            for key in self.parameters:
                if key in settings:
                    d[key] = settings[key]

        self.cta_engine = cta_engine
        self.strategy_name = strategy_name

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")

        self.cache_volume = 0
        self.cache_order_dict = {}

        self.file_print = FilePrint(self.strategy_name + ".log", "strategy_run_log", mode="w")
        self.important_print = FilePrint(self.strategy_name + "_important.log", "important_strategy_log", mode="w")

        self.time_send_ding_msg = risk.TimeWork(60)

    def cache_need_send_order(self, direction, volume):
        if direction == Direction.LONG.value:
            self.cache_volume += volume
        else:
            self.cache_volume -= volume

    def tick_send_order(self, tick):
        if len(self.cache_order_dict.keys()) == 0 and abs(self.cache_volume) > 0:
            if self.cache_volume > 0:
                price = get_round_order_price(tick.last_price * 1.001, self.exchange_info["price_tick"])
                list_orders = self.buy(self.symbol_pair, self.exchange, price, abs(self.cache_volume))
                for new_vt_order_id, order in list_orders:
                    self.cache_order_dict[new_vt_order_id] = copy(order)
            elif self.cache_volume < 0:
                price = get_round_order_price(tick.last_price * 0.999, self.exchange_info["price_tick"])
                list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(self.cache_volume))
                for new_vt_order_id, order in list_orders:
                    self.cache_order_dict[new_vt_order_id] = copy(order)

    def update_order(self, order: OrderData):
        if order.vt_order_id in self.cache_order_dict.keys():
            bef_order = self.cache_order_dict[order.vt_order_id]
            new_traded = order.traded - bef_order.traded
            if new_traded > 0:
                if order.direction == Direction.LONG.value:
                    self.pos += new_traded
                    self.cache_volume -= new_traded
                else:
                    self.pos -= new_traded
                    self.cache_volume += new_traded

            self.cache_order_dict[order.vt_order_id] = copy(order)
            if not order.is_active():
                self.cache_order_dict.pop(order.vt_order_id)

    def update_setting(self, setting):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def write_important_log(self, msg):
        """
        Write important log
        """
        self.important_print.write(msg)

    def write_log(self, msg):
        """
        Write a log message.
        """
        self.file_print.write(
            '{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.strategy_name, msg))

    def load_server_bars(self, vt_symbols: list, days, interval=Interval.DAY.value, callback=None):
        if not callback:
            callback = self.on_bar

        for vt_symbol in vt_symbols:
            self.write_log("[load_bar] vt_symbol:{} interval:{} days:{}".format(vt_symbol, interval, days))
            self.cta_engine.load_bar(vt_symbol, days, interval, callback)

    def load_bar(self, days, interval=Interval.MINUTE.value, callback=None):
        if not callback:
            callback = self.on_bar

        self.write_log("[load_bar] vt_symbol:{} interval:{} days:{}".format(self.vt_symbol, interval, days))
        self.cta_engine.load_bar(self.vt_symbol, days, interval, callback)

    @staticmethod
    def load_bar_online(vt_symbol, days, interval=Interval.MINUTE.value):
        symbol, exchange = get_from_vt_key(vt_symbol)
        data_client = data_client_dict.get(exchange, None)
        if data_client:
            ret_bars = data_client().get_kline(symbol=symbol,
                                               period=interval,
                                               start_datetime=datetime.now() - timedelta(days=days),
                                               end_datetime=datetime.now())
            return ret_bars
        return []

    @staticmethod
    def load_bar_online_between(vt_symbol, interval, start_datetime, end_datetime):
        symbol, exchange = get_from_vt_key(vt_symbol)
        data_client = data_client_dict.get(exchange, None)
        if data_client:
            ret_bars = data_client().get_kline(symbol=symbol,
                                               period=interval,
                                               start_datetime=start_datetime,
                                               end_datetime=end_datetime)
            return ret_bars
        return []

    def get_contract(self, vt_symbol):
        """
        获得合约信息 , 从 engine中
        """
        return self.cta_engine.get_contract(vt_symbol)

    def get_account(self, vt_account_id):
        """
        获得账户的信息 , 从 engine中
        """
        return self.cta_engine.get_account(vt_account_id)

    def batch_orders(self, reqs):
        """
        send batch orders to system
        """
        return []

    def cancel_orders(self, vt_order_ids):
        """
        cancel batch orders 
        """
        pass

    def buy(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send buy order to open a long position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.OPEN.value, price, volume, stop, lock)

    def sell(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send sell order to close a long position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.CLOSE.value, price, volume, stop, lock)

    def short(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send short order to open as short position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume, stop, lock)

    def cover(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send cover order to close a short position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.CLOSE.value, price, volume, stop, lock)

    def send_order(self, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        """
        Send a new order.
        """
        if self.trading:
            if stop:
                vt_order_ids = self.cta_engine.send_order(self, symbol, exchange, direction,
                                                          offset, price, volume, stop=True)
                msg = "[send_stop_order] vt_order_ids:{} info:{},{},{},{},{},{}" \
                    .format(vt_order_ids, symbol, exchange, direction, offset, price, volume)
                self.write_log(msg)
                return vt_order_ids
            else:
                vt_order_ids = self.cta_engine.send_order(self, symbol, exchange, direction, offset, price, volume)
                msg = "[send_order] vt_order_ids:{} info:{},{},{},{},{},{}" \
                    .format(vt_order_ids, symbol, exchange, direction,
                            offset, price, volume)
                self.write_log(msg)
                return vt_order_ids
        else:
            msg = "[send_order] trading condition is false!"
            self.write_log(msg)
            return []

    def cancel_order(self, vt_order_id):
        """
        Cancel an existing order.
        """
        if self.trading:
            msg = "[cancel order] vt_order_id:{}".format(vt_order_id)
            self.write_log(msg)
            self.cta_engine.cancel_order(self, vt_order_id)

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "strategy_name": self.strategy_name,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def put_event(self):
        """
        Put an strategy data event for ui update.
        """
        if self.inited:
            self.cta_engine.put_strategy_event(self)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    def on_merge_tick(self, merge_tick):
        """
        Callback of new merge tick data update.
        """
        pass

    def on_tick(self, tick):
        """
        Callback of new tick data update.
        """
        pass

    def on_bar(self, bar):
        pass

    def on_trade(self, trade):
        """
        Callback of new trade data update.
        """
        pass

    def on_order(self, order):
        """
        Callback of new order data update.
        """
        pass

    def send_ding_msg(self, msg):
        if self.time_send_ding_msg.can_work():
            try:
                msg = '{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), self.strategy_name, msg)
                ding_talk_service.send_msg(msg)
            except Exception as ex:
                self.write_log(f"[send_ding_msg] ex:{ex} msg:{msg}!")
