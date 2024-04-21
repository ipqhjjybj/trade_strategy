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


class CalendarSpreadV1Strategy(CalendarSpreadTemplate):
    """
    价差 = (第一个市场/第二个市场 - 1) * 100
    """
    author = "ipqhjjybj"
    class_name = "CalendarSpreadV1Strategy"

    # 参数列表
    parameters = [
                    'strategy_name',            # 策略加载的唯一性名字
                    'class_name',               # 类的名字
                    'author',                   # 作者
                    'vt_symbols_subscribe',     # 订阅的品种
                    'target_exchange_info',     # 目标市场的价格
                    'base_exchange_info',       # 反向市场的价格
                    'spread_down_list',         # 前多后空套利
                    'spread_up_list'            # 前空后多套利
                 ]                              # 挂单差值迭代 , 如果 inc_spread 是, 那么就表示1个price_tick的增加


    def __init__(self, cs_engine, strategy_name, settings):
        super(CalendarSpreadV1Strategy, self).__init__(cs_engine, strategy_name, settings)

        self.target_bids = [(0.0,0.0,"")] * MAX_PRICE_NUM
        self.target_asks = [(0.0,0.0,"")] * MAX_PRICE_NUM

        self.base_bids = [(0.0,0.0,"")] * MAX_PRICE_NUM
        self.base_bids = [(0.0,0.0,"")] * MAX_PRICE_NUM

        self.update_tick_symbol = False
        self.update_base_symbol = False

        self.update_account_flag = False

        self.has_compute_flag = False

        self.zs_buy_order = {}
        self.zs_sell_order = {}
        self.hb_buy_order = {}
        self.hb_sell_order = {}

        self.tot_position = 100
        self.now_target_pos = 0
        self.now_base_pos = 0

        self.need_target_pos = 0
        self.need_base_pos = 0

    def update_account(self):
        # target
        acct = self.get_account(self.target_exchange_info["account_key"])
        if acct is not None:
            self.target_exchange_info["account_val"] = acct.balance

        #self.write_log("[update_account] acct :{}".format(acct.__dict__))
        # base
        acct = self.get_account(self.base_exchange_info["account_key"])
        if acct is not None:
            self.base_exchange_info["account_val"] = acct.balance

        self.update_account_flag = True

        #self.write_log("[update_account] acct :{}".format(acct.__dict__))
        #self.write_log("[update_account] base_exchange_info :{}".format(self.base_exchange_info))

    def compute_tot_position(self):
        if not self.has_compute_flag:
            self.has_compute_flag = True

            target_tot_position = self.target_exchange_info["account_val"] * self.target_bids[0][0] / self.target_exchange_info["contract_size"]
            base_tot_position = self.base_exchange_info["account_val"] * self.base_bids[0][0] / self.base_exchange_info["contract_size"]

            self.tot_position = min(target_tot_position, base_tot_position)

            self.write_log("[compute_tot_position] self.tot_position:{}".format(self.tot_position))

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))
        self.update_account()
    
    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def already_sent_volume(self, dic):
        volumes = 0.0
        for vt_order_id,order in dic.items():
            volumes += order.volume - order.traded
        return volumes

    def check_contract_ok(self):
        return True

    def cancel_too_long_orders(self):
        pass

    def cancel_extra_orders(self):
        pass

    def get_live_order_ids(self):
        return list(self.zs_buy_order.keys()) + list(self.zs_sell_order.keys()) + list(self.hb_buy_order.keys()) + list(self.hb_sell_order.keys())

    def put_orders(self):
        if self.need_target_pos > self.now_target_pos:
            t_need_send_buy_target = self.need_target_pos - self.now_target_pos - self.already_sent_volume(self.zs_buy_order)
            if t_need_send_buy_target > 0:
                # go to cover
                right_volume = abs(t_need_send_buy_target)
                right_volume = get_round_order_price(right_volume, self.target_exchange_info["volume_tick"])
                right_price = get_round_order_price(self.target_asks[0][0], self.target_exchange_info["price_tick"])
                send_order_info_list = self.buy(self.target_exchange_info["symbol"], self.target_exchange_info["exchange"], right_price, right_volume)
                for vt_order_id, order in send_order_info_list:
                    self.zs_buy_order[vt_order_id] = order

        if self.need_target_pos < self.now_target_pos:
            t_need_send_sell_target = self.need_target_pos - self.now_target_pos + self.already_sent_volume(self.zs_sell_order)
            if t_need_send_sell_target < 0:
                # go to sell
                right_volume = abs(t_need_send_sell_target)
                right_volume = get_round_order_price(right_volume, self.target_exchange_info["volume_tick"])
                right_price = get_round_order_price(self.target_bids[0][0], self.target_exchange_info["price_tick"])
                send_order_info_list = self.sell(self.target_exchange_info["symbol"], self.target_exchange_info["exchange"], right_price, right_volume)
                for vt_order_id, order in send_order_info_list:
                    self.zs_sell_order[vt_order_id] = order

        if self.need_base_pos > self.now_base_pos:
            t_need_send_buy_base = self.need_base_pos - self.now_base_pos - self.already_sent_volume(self.hb_buy_order)
            if t_need_send_buy_base > 0:
                # go to buy
                right_volume = abs(t_need_send_buy_base)
                right_volume = get_round_order_price(right_volume, self.base_exchange_info["volume_tick"])
                right_price = get_round_order_price(self.base_asks[0][0], self.base_exchange_info["price_tick"])
                send_order_info_list = self.buy(self.base_exchange_info["symbol"], self.base_exchange_info["exchange"], right_price, right_volume)
                for vt_order_id, order in send_order_info_list:
                    self.hb_buy_order[vt_order_id] = order

        if self.need_base_pos < self.now_base_pos:
            t_need_send_sell_base = self.need_base_pos - self.now_base_pos + self.already_sent_volume(self.hb_buy_order) 
            if t_need_send_sell_base < 0:
                # go to sell
                right_volume = abs(t_need_send_sell_base)
                right_volume = get_round_order_price(right_volume, self.base_exchange_info["volume_tick"])
                right_price = get_round_order_price(self.base_bids[0][0], self.base_exchange_info["price_tick"])
                send_order_info_list = self.sell(self.base_exchange_info["symbol"], self.base_exchange_info["exchange"], right_price, right_volume)
                for vt_order_id, order in send_order_info_list:
                    self.hb_sell_order[vt_order_id] = order

        #print("need_target_pos,now_target_pos:{}-{}".format(self.need_target_pos, self.now_target_pos))
        #print("need_base_pos,now_base_pos:{}-{}".format(self.need_base_pos, self.now_base_pos))

    def compute_pos(self):
        spread = self.base_bids[0][0] / self.target_bids[0][0] - 1
        #print("compute_pos,spread:{}".format(spread))
        tmp_need_target_pos = 0
        tmp_need_base_pos = 0
        for dic in self.spread_up_list:
            if spread > dic["spread"]:
                tmp_need_target_pos += -1 * dic["percent"] * self.tot_position
                tmp_need_base_pos += dic["percent"] * self.tot_position
        for dic in self.spread_down_list:
            if spread < dic["spread"]:
                tmp_need_target_pos +=  dic["percent"] * self.tot_position
                tmp_need_base_pos += -1 * dic["percent"] * self.tot_position

        self.need_target_pos = get_round_order_price(tmp_need_target_pos, self.target_exchange_info["volume_tick"])
        self.need_base_pos = get_round_order_price(tmp_need_base_pos, self.base_exchange_info["volume_tick"])


    def on_tick(self, tick):
        if tick.vt_symbol == self.target_exchange_info["vt_symbol"]:
            new_target_bids = []
            for i in range(len(tick.bid_prices)):
                if tick.bid_prices[i] > 0:
                    new_target_bids.append( (tick.bid_prices[i], tick.bid_volumes[i], tick.exchange))
            new_target_asks = []
            for i in range(len(tick.ask_prices)):
                if tick.ask_prices[i] > 0:
                    new_target_asks.append( (tick.ask_prices[i], tick.ask_volumes[i] , tick.exchange))

            if new_target_bids:
                self.target_bids = new_target_bids
            if new_target_asks:
                self.target_asks = new_target_asks

            self.update_tick_symbol = True

            if self.update_tick_symbol and self.update_base_symbol:
                # 计算应该的仓位
                self.check_contract_ok()
                self.cancel_too_long_orders()
                self.compute_pos()
                self.put_orders()

        elif tick.vt_symbol == self.base_exchange_info["vt_symbol"]:
            new_base_bids = []
            for i in range(len(tick.bid_prices)):
                if tick.bid_prices[i] > 0:
                    new_base_bids.append( (tick.bid_prices[i], tick.bid_volumes[i], tick.exchange))
            new_base_asks = []
            for i in range(len(tick.ask_prices)):
                if tick.ask_prices[i] > 0:
                    new_base_asks.append( (tick.ask_prices[i], tick.ask_volumes[i], tick.exchange))

            if new_base_bids:
                self.base_bids = new_base_bids
            if new_base_asks:
                self.base_asks = new_base_asks

            self.update_base_symbol = True

        if self.update_tick_symbol and self.update_base_symbol and self.update_account_flag:
            if self.inited:
                self.compute_tot_position()

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction ,order.price ,order.volume ,order.traded))

        if order.is_active:
            if not self.trading:
                self.write_log("[not trading] now is not in trading condition, cancel order:{}".format(order.vt_order_id))
                self.cancel_order(order.vt_order_id)
            else:
                if order.vt_order_id not in self.get_live_order_ids():
                    self.write_log("[not in live ids] vt_order_id:{} is not in living ids, cancel it!".format(order.vt_order_id))
                    self.cancel_order(order.vt_order_id)

        if order.status == Status.SUBMITTING.value:
            return

        if order.exchange == self.target_exchange_info["exchange"]:
            if order.direction == Direction.LONG.value:
                bef_order = self.zs_buy_order.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded
                if traded_volume:
                    self.now_target_pos += traded_volume
                self.zs_buy_order[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.zs_buy_order.pop(order.vt_order_id)
            else:
                bef_order = self.zs_sell_order.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded
                if traded_volume:
                    self.now_target_pos -= traded_volume
                self.zs_sell_order[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.zs_sell_order.pop(order.vt_order_id)

        elif order.exchange == self.base_exchange_info["exchange"]:
            if order.direction == Direction.LONG.value:
                bef_order = self.hb_buy_order.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded
                if traded_volume:
                    self.now_base_pos += traded_volume
                self.hb_buy_order[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.hb_buy_order.pop(order.vt_order_id)
            else:
                bef_order = self.hb_sell_order.get(order.vt_order_id, None)
                traded_volume = 0
                if bef_order is not None:
                    traded_volume = order.traded - bef_order.traded
                else:
                    traded_volume = order.traded
                if traded_volume:
                    self.now_base_pos -= traded_volume
                self.hb_sell_order[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.hb_sell_order.pop(order.vt_order_id)

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        self.write_log('[trade detail] :{}'.format(trade.__dict__))

        