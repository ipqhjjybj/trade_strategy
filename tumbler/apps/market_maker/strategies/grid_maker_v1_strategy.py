# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset

system_inside_min_volume = {
    "_usdt": 1.2,
    "_btc": 0.0005
}

binance_inside_min_volume = {
    "_usdt": 10
}


def get_system_inside_min_volume(symbol, price, exchange):
    if price < MIN_FLOAT_VAL:
        return 0.0

    dic = system_inside_min_volume

    if exchange == Exchange.BINANCE.value:
        dic = binance_inside_min_volume

    for key, val in dic.items():
        if symbol.endswith(key):
            return float(val) / price

    return 0.0


class GridMakerV1Strategy(MarketMakerTemplate):
    """
    这个策略负责挂出订单, 在均价之上进行回补
    
    对于多头方向
    1、首先挂出买单，如果有成交，则挂出反方向的卖单

    对于空头方向
    2、首先挂出卖单，如果有成交，则挂出反方向的买单

    不管多空, 对于仓位为空 的情况, 如果发现价格 偏离太多，则撤掉这一方向挂单，重新挂

    """
    author = "ipqhjjybj"
    class_name = "GridMakerV1Strategy"

    symbol_pair = "btc_usdt"

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'long_config',  # 多头管理
                  'short_config',  # 空头管理
                  'exchange_info'  # 交易所信息
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(GridMakerV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.update_failed = False
        self.update_account_flag = False

        self.exchange_info = settings["exchange_info"]
        self.long_config = settings["long_config"]
        self.short_config = settings["short_config"]

        self.zs_buy_order_dict = {}
        self.zs_sell_order_dict = {}

        self.last_zs_buy_time = time.time()
        self.last_zs_sell_time = time.time()

        self.hh_buy_order_dict = {}
        self.hb_sell_order_dict = {}

        self.old_buy_order_dict = {}
        self.old_sell_order_dict = {}

        self.max_min_volume = 0

        self.u_bids = [(0.0, 0.0)] * MAX_PRICE_NUM
        self.u_asks = [(0.0, 0.0)] * MAX_PRICE_NUM

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))
        self.update_exchange_order_info()
        self.update_account()

    def update_exchange_order_info(self):
        """
        下单时，根据交易所订单的要求，更新下单参数
        """
        self.update_failed = False
        target_vt_symbol = get_vt_key(self.symbol_pair, self.exchange_info["exchange_name"])
        contract = self.get_contract(target_vt_symbol)

        if contract is None:
            self.update_failed = True
            update_msg = "target_contract:{} is not found!".format(target_vt_symbol)
            self.write_log(update_msg)
            return

        self.exchange_info["price_tick"] = contract.price_tick
        self.exchange_info["volume_tick"] = contract.volume_tick
        self.exchange_info["min_volume"] = contract.min_volume

        self.max_min_volume = max(self.max_min_volume, contract.min_volume)

        self.write_log("[update_exchange_order_info] exchange_info:{}".format(self.exchange_info))

    def update_account(self):
        """
        加载账户资金情况
        """
        self.update_account_flag = True

        target_symbol, base_symbol = get_two_currency(self.symbol_pair)
        key_acct_ta = get_vt_key(self.exchange_info["exchange_name"], target_symbol)
        key_acct_tb = get_vt_key(self.exchange_info["exchange_name"], base_symbol)

        acct_ta = self.get_account(key_acct_ta)
        acct_tb = self.get_account(key_acct_tb)

        self.exchange_info["pos_target_symbol"] = 0
        self.exchange_info["pos_base_symbol"] = 0
        self.exchange_info["frozen_base_symbol"] = 0
        self.exchange_info["frozen_target_symbol"] = 0

        if acct_ta is not None:
            self.exchange_info["pos_target_symbol"] = acct_ta.available * self.exchange_info[
                "pos_target_symbol_percent_use"] / 100.0
        if acct_tb is not None:
            self.exchange_info["pos_base_symbol"] = acct_tb.available * self.exchange_info[
                "pos_base_symbol_percent_use"] / 100.0

        self.write_log("[update_account] self.exchange_info:{}".format(self.exchange_info))

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def get_inc_price(self, dic, price):
        if dic["inc_spread"] == 0:
            return self.exchange_info["price_tick"]
        else:
            return dic["inc_spread"] * price / 100.0

    def get_live_order_ids(self):
        return list(self.zs_buy_order_dict.keys()) + list(self.zs_sell_order_dict.keys()) \
               + list(self.hh_buy_order_dict.keys()) + list(self.hb_sell_order_dict.keys()) \
               + list(self.old_buy_order_dict.keys()) + list(self.old_sell_order_dict.keys())

    def cancel_sets_order(self, need_cancel_sets):
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def cancel_all_buy_orders(self, dic):
        need_cancel_sets = []
        for key, s_order in self.zs_buy_order_dict.items():
            need_cancel_sets.append(s_order.vt_order_id)
        self.write_log("[cancel_all_buy_orders]")
        self.cancel_sets_order(need_cancel_sets)

    def cancel_all_sell_orders(self, dic):
        need_cancel_sets = []
        for key, s_order in self.zs_sell_order_dict.items():
            need_cancel_sets.append(s_order.vt_order_id)
        self.write_log("[cancel_all_sell_orders]")
        self.cancel_sets_order(need_cancel_sets)

    def put_long_orders(self):
        if len(self.zs_buy_order_dict.keys()) > 0:
            # self.write_log( "[put_long_orders] already has orders!")
            return
        if self.long_config["now_position"] > MIN_FLOAT_VAL:
            # self.write_log( "[put_long_orders] now_position has positions!")
            return

        tmp_ava_base_symbol = self.exchange_info["pos_base_symbol"] - self.exchange_info["frozen_base_symbol"]

        order_list = []
        start_price = self.u_bids[0][0]
        # 后期这个数量，需要加上 亏损部分 以及 需要回补的部分 以及趋势偏重
        start_volume = self.long_config["start_volume"] * self.exchange_info["pos_base_symbol"] / self.u_bids[0][
            0] / 100.0
        inc_volume = self.long_config["inc_volume"] * self.exchange_info["pos_base_symbol"] / self.u_bids[0][0] / 100.0

        # self.write_log("[put_long_orders debug] start_volume:{}, pos_base_symbol:{} start_price:{}".format(self.long_config["start_volume"], self.exchange_info["pos_base_symbol"], start_price))
        # self.write_log("[put_long_orders] start_volume:{} max_min_volume:{}".format(start_volume, self.max_min_volume))
        for i in range(self.long_config["put_order_num"]):
            use_price = get_round_order_price(start_price, self.exchange_info["price_tick"])
            if use_price > start_price:
                use_price -= self.exchange_info["price_tick"]

            use_volume = max(self.max_min_volume, start_volume)
            use_volume = get_round_order_price(use_volume, self.exchange_info["volume_tick"])

            if tmp_ava_base_symbol >= use_price * use_volume * 1.04:
                order_list.append([use_price, use_volume])
                start_volume += inc_volume
                start_price -= self.long_config["inc_spread"] * self.u_bids[0][0] / 100.0

                tmp_ava_base_symbol -= use_price * use_volume
                self.exchange_info["frozen_base_symbol"] += tmp_ava_base_symbol
            else:
                break

        # 发单
        for price, volume in order_list:
            ret_orders = self.send_order(self.symbol_pair, self.exchange_info["exchange_name"], Direction.LONG.value,
                                         Offset.OPEN.value, price, volume)

            for vt_order_id, order in ret_orders:
                if vt_order_id:
                    self.zs_buy_order_dict[vt_order_id] = order
                else:
                    self.write_log("[put_long_orders] vt_order_id is None")

    def put_short_orders(self):
        if len(self.zs_sell_order_dict.keys()) > 0:
            # self.write_log( "[put_short_orders] already has orders!")
            return
        if self.short_config["now_position"] > MIN_FLOAT_VAL:
            # self.write_log( "[put_short_orders] now_position has positions!")
            return

        tmp_ava_target_symbol = self.exchange_info["pos_target_symbol"] - self.exchange_info["frozen_target_symbol"]

        order_list = []
        start_price = self.u_asks[0][0]
        start_volume = self.short_config["start_volume"] * self.exchange_info["pos_target_symbol"] / 100.0
        inc_volume = self.short_config["inc_volume"] * self.exchange_info["pos_target_symbol"] / 100.0

        for i in range(self.short_config["put_order_num"]):
            use_price = get_round_order_price(start_price, self.exchange_info["price_tick"])
            if use_price < start_price:
                use_price += self.exchange_info["price_tick"]
            use_volume = max(self.max_min_volume, start_volume)
            use_volume = get_round_order_price(use_volume, self.exchange_info["volume_tick"])

            if tmp_ava_target_symbol >= use_volume * 1.04:
                order_list.append([use_price, use_volume])

                start_volume += inc_volume
                start_price += self.short_config["inc_spread"] * self.u_asks[0][0] / 100.0

                tmp_ava_target_symbol -= use_volume
                self.exchange_info["frozen_target_symbol"] += use_volume
            else:
                break

        # self.write_log("[put_short_orders]:{}".format(order_list))
        # 发单
        for price, volume in order_list:
            ret_orders = self.send_order(self.symbol_pair, self.exchange_info["exchange_name"], Direction.SHORT.value
                                         , Offset.CLOSE.value, price, volume)

            for vt_order_id, order in ret_orders:
                if vt_order_id:
                    self.zs_sell_order_dict[vt_order_id] = order
                else:
                    self.write_log("[put_short_orders] vt_order_id is None")

    def check_to_cancel_all_buy_orders(self):
        """
        当没有成交仓位，且离买一有比较远的距离时撤单 , 且 60秒没成交
        """
        if self.long_config["now_position"] < MIN_FLOAT_VAL and len(self.zs_buy_order_dict.keys()) > 0:
            now_time = time.time()

            need_cancel_sets = []
            already_price_volumes = []
            for key, s_order in self.zs_buy_order_dict.items():
                already_price_volumes.append((s_order.price, s_order.volume))
                need_cancel_sets.append(s_order.vt_order_id)
            already_price_volumes.sort(reverse=True)

            if already_price_volumes[0][0] + 5 * self.long_config["profit_spread"] < self.u_bids[0][0] and (
                    now_time - self.last_zs_buy_time) > 60:
                self.write_log("[check_to_cancel_all_buy_orders] now go to cancel all orders ")
                self.cancel_sets_order(need_cancel_sets)

    def check_to_cancel_all_sell_orders(self):
        """
        当没有成交仓位, 且离卖一有比较远的距离时撤单， 且60秒没成交
        """
        if self.short_config["now_position"] < MIN_FLOAT_VAL and len(self.zs_sell_order_dict.keys()) > 0:
            now_time = time.time()

            need_cancel_sets = []
            already_price_volumes = []
            for key, s_order in self.zs_sell_order_dict.items():
                already_price_volumes.append((s_order.price, s_order.volume))
                need_cancel_sets.append(s_order.vt_order_id)
            already_price_volumes.sort(reverse=False)

            if already_price_volumes[0][0] - 5 * self.short_config["profit_spread"] > self.u_asks[0][0] and (
                    now_time - self.last_zs_sell_time) > 60:
                self.write_log("[check_to_cancel_all_sell_orders] now go to cancel all orders")
                self.cancel_sets_order(need_cancel_sets)

    def check_to_cancel_all_cover_buy_order(self):
        if self.hh_buy_order_dict:
            need_cancel_sets = []
            all_volume = 0
            for key, s_order in self.hh_buy_order_dict.items():
                all_volume += s_order.volume - s_order.traded
                need_cancel_sets.append(s_order.vt_order_id)

            if abs(all_volume - self.short_config["now_position"]) > MIN_FLOAT_VAL:
                self.write_log("[check_to_cancel_all_cover_buy_order] all_volume:{} now_position:{}".format(all_volume,
                                                                                                            self.short_config[
                                                                                                                "now_position"]))
                self.cancel_sets_order(need_cancel_sets)

    def check_to_cancel_all_cover_sell_order(self):
        if self.hb_sell_order_dict:
            need_cancel_sets = []
            all_volume = 0
            for key, s_order in self.hb_sell_order_dict.items():
                all_volume += s_order.volume - s_order.traded
                need_cancel_sets.append(s_order.vt_order_id)

            if abs(all_volume - self.long_config["now_position"]) > MIN_FLOAT_VAL:
                self.write_log("[check_to_cancel_all_cover_sell_order] all_volume:{} now_position:{}".format(all_volume,
                                                                                                             self.long_config[
                                                                                                                 "now_position"]))
                self.cancel_sets_order(need_cancel_sets)

    def put_cover_buy_orders(self):
        if self.hh_buy_order_dict:
            pass
        else:
            n_price = self.short_config["avg_price"] * (1 - self.short_config["profit_spread"] / 100.0)
            n_volume = self.short_config["now_position"]
            if n_volume:
                price = get_round_order_price(n_price, self.exchange_info["price_tick"])
                volume = get_round_order_price(n_volume, self.exchange_info["volume_tick"])

                if n_price < price:
                    price = price - self.exchange_info["price_tick"]

                ret_orders = self.send_order(self.symbol_pair, self.exchange_info["exchange_name"],
                                             Direction.LONG.value, Offset.OPEN.value, price, volume)

                for vt_order_id, order in ret_orders:
                    if vt_order_id:
                        self.hh_buy_order_dict[vt_order_id] = order
                    else:
                        self.write_log("[put_cover_buy_orders] vt_order_id is None")

    def put_cover_sell_orders(self):
        if self.hb_sell_order_dict:
            pass
        else:
            n_price = self.long_config["avg_price"] * (1 + self.long_config["profit_spread"] / 100.0)
            n_volume = self.long_config["now_position"]

            if n_volume:
                price = get_round_order_price(n_price, self.exchange_info["price_tick"])
                volume = get_round_order_price(n_volume, self.exchange_info["volume_tick"])
                if n_price > price:
                    price = price + self.exchange_info["price_tick"]

                ret_orders = self.send_order(self.symbol_pair, self.exchange_info["exchange_name"],
                                             Direction.SHORT.value, Offset.CLOSE.value, price, volume)

                for vt_order_id, order in ret_orders:
                    if vt_order_id:
                        self.hb_sell_order_dict[vt_order_id] = order
                    else:
                        self.write_log("[put_cover_sell_orders] vt_order_id is None")

    def check_to_put_old_buy_orders(self):
        pass

    def check_to_put_old_sell_orders(self):
        pass

    def on_tick(self, tick):
        # self.write_log("[on_tick] tick.last_price:{}".format(tick.last_price))
        if tick.bid_prices[0] > 0:
            self.max_min_volume = max(self.max_min_volume,
                                      get_system_inside_min_volume(self.symbol_pair, tick.bid_prices[0],
                                                                   self.exchange_info["exchange_name"]))

            new_bids = []
            for i in range(len(tick.bid_prices)):
                if tick.bid_prices[i] > 0.0:
                    new_bids.append([tick.bid_prices[i], tick.bid_volumes[i]])

            new_asks = []
            for i in range(len(tick.ask_prices)):
                if tick.ask_prices[i] > 0.0:
                    new_asks.append([tick.ask_prices[i], tick.ask_volumes[i]])

            if new_bids:
                self.u_bids = copy(new_bids)

            if new_asks:
                self.u_asks = copy(new_asks)

            if self.trading:
                self.check_to_put_old_buy_orders()
                self.check_to_put_old_sell_orders()

                self.check_to_cancel_all_buy_orders()
                self.check_to_cancel_all_sell_orders()

                if self.long_config["run"]:
                    self.put_long_orders()
                if self.short_config["run"]:
                    self.put_short_orders()

                self.check_to_cancel_all_cover_buy_order()
                self.check_to_cancel_all_cover_sell_order()

                self.put_cover_buy_orders()
                self.put_cover_sell_orders()

    def on_bar(self, bar):
        pass

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

        if order.traded > MIN_FLOAT_VAL:
            self.output_important_log()

        # 提交的订单推送，过滤掉
        if order.status == Status.SUBMITTING.value:
            return

        if order.exchange == self.exchange_info["exchange_name"]:
            if order.direction == Direction.LONG.value:
                if order.vt_order_id in self.zs_buy_order_dict.keys():
                    new_traded = 0
                    bef_order = self.zs_buy_order_dict.get(order.vt_order_id, None)
                    if bef_order:
                        new_traded = order.traded - bef_order.traded
                    else:
                        new_traded = order.traded

                    self.write_log("[on_order zs_long] new_traded:{}".format(new_traded))
                    if new_traded > 0:
                        self.last_zs_buy_time = time.time()

                        self.exchange_info["pos_base_symbol"] -= new_traded * order.price
                        self.exchange_info["pos_target_symbol"] += new_traded * 0.998
                        self.exchange_info["frozen_base_symbol"] -= new_traded * order.price

                        self.long_config["avg_price"] = (self.long_config["avg_price"] * self.long_config[
                            "now_position"] + new_traded * order.price) / (
                                                                self.long_config["now_position"] + new_traded)
                        self.long_config["now_position"] += new_traded

                    self.zs_buy_order_dict[order.vt_order_id] = copy(order)
                    if not order.is_active():
                        self.zs_buy_order_dict.pop(order.vt_order_id)
                        self.exchange_info["frozen_base_symbol"] -= (order.volume - order.traded) * order.price

                elif order.vt_order_id in self.hh_buy_order_dict.keys():
                    new_traded = 0
                    bef_order = self.hh_buy_order_dict.get(order.vt_order_id, None)
                    if bef_order:
                        new_traded = order.traded - bef_order.traded
                    else:
                        new_traded = order.traded
                    if new_traded > 0:
                        self.exchange_info["pos_base_symbol"] -= new_traded * order.price
                        self.exchange_info["pos_target_symbol"] += new_traded * 0.998
                        self.exchange_info["frozen_base_symbol"] -= new_traded * order.price

                        self.short_config["now_position"] -= new_traded

                    self.hh_buy_order_dict[order.vt_order_id] = copy(order)
                    if not order.is_active():
                        self.hh_buy_order_dict.pop(order.vt_order_id)
                        self.exchange_info["frozen_base_symbol"] -= (order.volume - order.traded) * order.price

                elif order.vt_order_id in self.old_buy_order_dict.keys():
                    pass
            else:
                if order.vt_order_id in self.zs_sell_order_dict.keys():
                    new_traded = 0
                    bef_order = self.zs_sell_order_dict.get(order.vt_order_id, None)
                    if bef_order:
                        new_traded = order.traded - bef_order.traded
                    else:
                        new_traded = order.traded

                    self.write_log("[on_order zs_sell] new_traded:{}".format(new_traded))
                    if new_traded > 0:
                        self.last_zs_sell_time = time.time()

                        self.exchange_info["pos_target_symbol"] -= new_traded
                        self.exchange_info["frozen_target_symbol"] -= new_traded
                        self.exchange_info["pos_base_symbol"] += new_traded * order.price * 0.998

                        self.short_config["avg_price"] = (self.short_config["avg_price"] * self.short_config[
                            "now_position"] + new_traded * order.price) / (
                                                                 self.short_config["now_position"] + new_traded)
                        self.short_config["now_position"] += new_traded

                    self.zs_sell_order_dict[order.vt_order_id] = copy(order)
                    if not order.is_active():
                        self.zs_sell_order_dict.pop(order.vt_order_id)
                        self.exchange_info["frozen_target_symbol"] -= order.volume - order.traded

                elif order.vt_order_id in self.hb_sell_order_dict.keys():
                    new_traded = 0
                    bef_order = self.hb_sell_order_dict.get(order.vt_order_id, None)
                    if bef_order:
                        new_traded = order.traded - bef_order.traded
                    else:
                        new_traded = order.traded

                    if new_traded > 0:
                        self.exchange_info["pos_target_symbol"] -= new_traded
                        self.exchange_info["frozen_target_symbol"] -= new_traded
                        self.exchange_info["pos_base_symbol"] += new_traded * order.price * 0.998

                        self.long_config["now_position"] -= new_traded

                    self.hb_sell_order_dict[order.vt_order_id] = copy(order)
                    if not order.is_active():
                        self.hb_sell_order_dict.pop(order.vt_order_id)
                        self.exchange_info["frozen_target_symbol"] -= order.volume - order.traded

                elif order.vt_order_id in self.old_sell_order_dict.keys():
                    pass

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        self.write_log('[trade detail] :{}'.format(trade.__dict__))
        self.write_log('[on_trade] end')

        self.write_log('exchange_info:{}'.format(self.exchange_info))
        self.write_log('long_config:{}'.format(self.long_config))
        self.write_log('short_config:{}'.format(self.short_config))
        self.write_log('zs_buy_order_dict:{}'.format(self.zs_buy_order_dict))
        self.write_log('zs_sell_order_dict:{}'.format(self.zs_sell_order_dict))
        self.write_log('hh_buy_order_dict:{}'.format(self.hh_buy_order_dict))
        self.write_log('hb_sell_order_dict:{}'.format(self.hb_sell_order_dict))

        self.put_event()

    def output_important_log(self):
        pass
