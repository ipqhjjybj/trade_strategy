# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.calendar_spread.template import (
    CalendarSpreadTemplate
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.constant import Direction, Status, Offset


class FutureCrossV1Strategy(CalendarSpreadTemplate):
    """
    BITMEX 与 OKEX 数值相减 > 某个数值
    """
    author = "ipqhjjybj"
    class_name = "FutureCrossV1Strategy"

    # 参数列表
    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # 订阅的品种
        'target_exchange_info',  # 目标市场的价格
        'base_exchange_info',  # 反向市场的价格
        'target_spread',  # 价差多少开始挂单
        'profit_spread',  # 盈利的价差
        'fixed_size'  # 挂多少单
    ]

    def __init__(self, cs_engine, strategy_name, settings):
        super(FutureCrossV1Strategy, self).__init__(cs_engine, strategy_name, settings)

        self.target_bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM
        self.target_asks = [(0.0, 0.0, "")] * MAX_PRICE_NUM

        self.base_bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM
        self.base_bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM

        self.update_tick_symbol = False
        self.update_base_symbol = False

        self.update_account_flag = False

        self.zs_buy_order_dic = {}
        self.zs_sell_order_dic = {}
        self.hb_buy_order_dic = {}
        self.hb_sell_order_dic = {}

        self.cancel_dict_times_count = defaultdict(int)  # 对一个 vt_order_id 的撤单次数

        self.now_target_exchange_pos = 0
        self.now_base_exchange_pos = 0
        self.target_target_exchange_pos = 0
        self.target_base_exchange_pos = 0

    def update_account(self):
        # target
        acct = self.get_account(self.target_exchange_info["account_key"])
        if acct is not None:
            self.target_exchange_info["account_val"] = acct.balance

        # self.write_log("[update_account] acct :{}".format(acct.__dict__))
        # base
        acct = self.get_account(self.base_exchange_info["account_key"])
        if acct is not None:
            self.base_exchange_info["account_val"] = acct.balance

        self.update_account_flag = True

        # self.write_log("[update_account] acct :{}".format(acct.__dict__))
        # self.write_log("[update_account] base_exchange_info :{}".format(self.base_exchange_info))

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))
        self.update_account()

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def get_frozen_volume(self, dic):
        volume = 0
        for vt_order_id, order in dic.items():
            volume += order.volume - order.traded
        return volume

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            # self.write_log("[cancel sets order] vt_order_id:{}".format(vt_order_id))
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > 3:
                if vt_order_id in self.zs_buy_order_dic.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.zs_buy_order_dic[vt_order_id]
                    del self.zs_buy_order_dic[vt_order_id]

                if vt_order_id in self.zs_sell_order_dic.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.zs_sell_order_dic[vt_order_id]

                    del self.zs_sell_order_dic[vt_order_id]

            if self.cancel_dict_times_count[vt_order_id] > 5:
                if vt_order_id in self.hb_buy_order_dic.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.hb_buy_order_dic[vt_order_id]
                    del self.hb_buy_order_dic[vt_order_id]

                    self.write_log('[already buy set cancel] vt_order_id:{}'.format(vt_order_id))

                if vt_order_id in self.hb_sell_order_dic.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.hb_sell_order_dic[vt_order_id]
                    del self.hb_sell_order_dic[vt_order_id]

                    self.write_log('[already sell set cancel] vt_order_id:{}'.format(vt_order_id))

    def cancel_not_profit_orders(self):
        need_cancel_sets = set([])
        for vt_order_id, order in self.zs_buy_order_dic.items():
            if order.price > self.base_bids[0][0] * (1 - self.profit_spread / 100.0):
                need_cancel_sets.add(order.vt_order_id)

        for vt_order_id, order in self.zs_sell_order_dic.items():
            if order.price < self.base_asks[0][0] * (1 + self.profit_spread / 100.0):
                need_cancel_sets.add(order.vt_order_id)

        self.cancel_sets_order(need_cancel_sets)

    def cancel_too_long_orders(self):
        now = time.time()
        need_cancel_sets = set([])

        for vt_order_id, order in self.zs_buy_order_dic.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            if now - order_time > 60 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.write_log(
                    "[cancel too long time],zs_buy order_id:{} now:{},order_time:{}".format(order.vt_order_id, now,
                                                                                            order_time))
                need_cancel_sets.add(order.vt_order_id)

        for vt_order_id, order in self.zs_sell_order_dic.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 超过180秒的订单撤掉重发 ,测试期间10秒
            if now - order_time > 60 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.write_log(
                    "[cancel too long time],zs_sell order_id:{} now:{},order_time:{}".format(order.vt_order_id, now,
                                                                                             order_time))
                need_cancel_sets.add(order.vt_order_id)

        self.cancel_sets_order(need_cancel_sets)

    def get_live_order_ids(self):
        return list(self.zs_buy_order_dic.keys()) + list(self.zs_sell_order_dic.keys()) + list(
            self.hb_buy_order_dic.keys()) + list(self.hb_sell_order_dic.keys())

    def check_fixed_size(self):
        if (self.fixed_size % self.target_exchange_info["contract_size"]) != 0 or (
                self.fixed_size % self.base_exchange_info["contract_size"]) != 0:
            self.write_log("[put order], contract size , fixed_size is not right!")
            return False
        return True

    def put_order(self):
        if not self.check_fixed_size():
            return

        if self.target_bids[0][0] > 0:
            # buy direction. , then sell
            chazhi = self.target_bids[0][0] - self.base_bids[0][0]
            # print("1", chazhi, chazhi / self.target_bids[0][0] , -1 * self.target_spread / 100.0)
            if chazhi / self.target_bids[0][0] < -1 * self.profit_spread / 100.0:
                self.target_target_exchange_pos = self.fixed_size / self.target_exchange_info["contract_size"]

        if self.target_asks[0][0] > 0:
            # sell direction , then buy
            chazhi = self.target_asks[0][0] - self.base_asks[0][0]
            # print("2", chazhi, chazhi / self.target_bids[0][0] , self.target_spread / 100.0)
            if chazhi / self.target_asks[0][0] > self.profit_spread / 100.0:
                self.target_target_exchange_pos = -1 * self.fixed_size / self.target_exchange_info["contract_size"]

        frozen_zs_buy = self.get_frozen_volume(self.zs_buy_order_dic)
        frozen_zs_sell = self.get_frozen_volume(self.zs_sell_order_dic)

        if frozen_zs_buy > 0 or frozen_zs_sell > 0:
            # if frozen_zs_buy > 0:
            #     print("frozen_zs_buy > 0")
            # if frozen_zs_sell > 0:
            #     print("frozen_zs_sell > 0")
            return

        if self.now_target_exchange_pos < self.target_target_exchange_pos:
            new_volume = self.target_target_exchange_pos - self.now_target_exchange_pos
            price = get_round_order_price(self.base_bids[0][0] * (1 - self.profit_spread / 100.0),
                                          self.target_exchange_info["price_tick"])
            price = min(price, self.target_bids[0][0])
            order_list = self.buy(self.target_exchange_info["symbol"], self.target_exchange_info["exchange"], price,
                                  new_volume)
            for vt_order_id, order in order_list:
                self.zs_buy_order_dic[vt_order_id] = order

        if self.now_target_exchange_pos > self.target_target_exchange_pos:
            new_volume = self.now_target_exchange_pos - self.target_target_exchange_pos
            price = get_round_order_price(self.base_asks[0][0] * (1 + self.profit_spread / 100.0),
                                          self.target_exchange_info["price_tick"])
            price = max(price, self.target_asks[0][0])
            order_list = self.sell(self.target_exchange_info["symbol"], self.target_exchange_info["exchange"], price,
                                   new_volume)
            for vt_order_id, order in order_list:
                self.zs_sell_order_dic[vt_order_id] = order

        print(
            "put_order target_target_exchange_pos:{},now_target_exchange_pos:{}".format(self.target_target_exchange_pos,
                                                                                        self.now_target_exchange_pos))

    def cover_order(self):
        self.target_base_exchange_pos = -1 * get_round_order_price(
            self.now_target_exchange_pos * self.target_exchange_info["contract_size"] / self.base_exchange_info[
                "contract_size"], self.base_exchange_info["volume_tick"])

        frozen_cover_buy = self.get_frozen_volume(self.hb_buy_order_dic)
        frozen_cover_sell = self.get_frozen_volume(self.hb_sell_order_dic)

        if frozen_cover_buy > 0 or frozen_cover_sell > 0:
            # if frozen_cover_buy > 0:
            #     print("frozen_cover_buy > 0")
            # if frozen_cover_sell > 0:
            #     print("frozen_cover_sell > 0")
            return

        if self.target_base_exchange_pos > self.now_base_exchange_pos:
            new_volume = self.target_base_exchange_pos - self.now_base_exchange_pos
            price = get_round_order_price(self.base_asks[0][0] * 1.003, self.base_exchange_info["price_tick"])
            order_list = self.buy(self.base_exchange_info["symbol"], self.base_exchange_info["exchange"], price,
                                  new_volume)

            for vt_order_id, order in order_list:
                self.hb_buy_order_dic[vt_order_id] = order

        elif self.target_base_exchange_pos < self.now_base_exchange_pos:
            new_volume = self.now_base_exchange_pos - self.target_base_exchange_pos
            price = get_round_order_price(self.base_bids[0][0] * 0.997, self.base_exchange_info["price_tick"])
            order_list = self.sell(self.base_exchange_info["symbol"], self.base_exchange_info["exchange"], price,
                                   new_volume)

            for vt_order_id, order in order_list:
                self.hb_sell_order_dic[vt_order_id] = order

    def on_tick(self, tick):
        if tick.vt_symbol == self.target_exchange_info["vt_symbol"]:
            new_target_bids = []
            for i in range(len(tick.bid_prices)):
                if tick.bid_prices[i] > 0:
                    new_target_bids.append((tick.bid_prices[i], tick.bid_volumes[i], tick.exchange))
            new_target_asks = []
            for i in range(len(tick.ask_prices)):
                if tick.ask_prices[i] > 0:
                    new_target_asks.append((tick.ask_prices[i], tick.ask_volumes[i], tick.exchange))

            if new_target_bids:
                self.target_bids = new_target_bids
            if new_target_asks:
                self.target_asks = new_target_asks

            self.update_tick_symbol = True

            if self.update_tick_symbol and self.update_base_symbol:
                if self.trading:
                    # 计算应该的仓位
                    self.cancel_not_profit_orders()
                    self.cancel_too_long_orders()
                    self.put_order()
                    self.cover_order()

        elif tick.vt_symbol == self.base_exchange_info["vt_symbol"]:
            new_base_bids = []
            for i in range(len(tick.bid_prices)):
                if tick.bid_prices[i] > 0:
                    new_base_bids.append((tick.bid_prices[i], tick.bid_volumes[i], tick.exchange))
            new_base_asks = []
            for i in range(len(tick.ask_prices)):
                if tick.ask_prices[i] > 0:
                    new_base_asks.append((tick.ask_prices[i], tick.ask_volumes[i], tick.exchange))

            if new_base_bids:
                self.base_bids = new_base_bids
            if new_base_asks:
                self.base_asks = new_base_asks

            self.update_base_symbol = True

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.is_active:
            if not self.trading:
                self.write_log(
                    "[not trading] now is not in trading condition, cancel order:{}".format(order.vt_order_id))
                self.cancel_order(order.vt_order_id)
            else:
                if order.vt_order_id not in self.get_live_order_ids():
                    self.write_log(
                        "[not in live ids] vt_order_id:{} is not in living ids, cancel it!".format(order.vt_order_id))
                    self.cancel_order(order.vt_order_id)

        if order.status == Status.SUBMITTING.value:
            return

        if order.exchange == self.target_exchange_info["exchange"]:
            if order.direction == Direction.LONG.value:
                bef_order = self.zs_buy_order_dic.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded

                if traded_volume:
                    self.now_target_exchange_pos += traded_volume
                    self.cover_order()
                self.zs_buy_order_dic[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.zs_buy_order_dic.pop(order.vt_order_id)

            else:
                bef_order = self.zs_sell_order_dic.get(order.vt_order_id, None)

                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded

                if traded_volume:
                    self.now_target_exchange_pos -= traded_volume
                    self.cover_order()

                self.zs_sell_order_dic[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.zs_sell_order_dic.pop(order.vt_order_id)

        elif order.exchange == self.base_exchange_info["exchange"]:
            if order.direction == Direction.LONG.value:
                bef_order = self.hb_buy_order_dic.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded

                if traded_volume:
                    self.now_base_exchange_pos += traded_volume

                self.hb_buy_order_dic[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.hb_buy_order_dic.pop(order.vt_order_id)
            else:
                bef_order = self.hb_sell_order_dic.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded

                if traded_volume:
                    self.now_base_exchange_pos -= traded_volume

                self.hb_sell_order_dic[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.hb_sell_order_dic.pop(order.vt_order_id)

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        self.write_log('[trade detail] :{}'.format(trade.__dict__))
