# coding=utf-8

import time
from copy import copy
from collections import defaultdict

from tumbler.apps.flash_0x.template import Flash0xTemplate
from tumbler.constant import MAX_PRICE_NUM, Exchange, Direction, Offset, Status, Interval
from tumbler.function import get_vt_key, get_round_order_price, is_number_change
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique
from tumbler.object import TickData

system_inside_min_volume = {
    "_usdt": 8,
    "_btc": 0.0008,
    "_eth": 0.02
}

binance_inside_min_volume = {
    "_usdt": 10
}


def get_system_inside_min_volume(symbol, price, exchange):
    if price < 1e-12:
        return 0.0

    dic = system_inside_min_volume

    if exchange == Exchange.BINANCE.value:
        dic = binance_inside_min_volume

    for key, val in dic.items():
        if symbol.endswith(key):
            return float(val) / price

    return 0.0


def is_price_volume_too_small(symbol, price, volume):
    for key, val in system_inside_min_volume.items():
        if symbol.endswith(key):
            if price * volume < val:
                return True
    return False


class Flash0xStrategy(Flash0xTemplate):
    author = "ipqhjjybj"
    class_name = "Flash0xStrategy"

    """
    默认参数
    """
    symbol_pair = "btm_usdt"
    vt_symbols_subscribe = []
    target_symbol = "btm"
    base_symbol = "usdt"
    target_exchange_info = {}
    base_exchange_info = {}
    ava_rise_below = 1
    ava_rise = 0
    ava_length = 10

    need_cover_buy_volume = 0
    need_cover_sell_volume = 0

    parameters = ['strategy_name',
                  'class_name',
                  'author',
                  'symbol_pair',
                  'vt_symbols_subscribe',
                  'target_symbol',
                  'base_symbol',
                  'target_exchange_info',
                  'base_exchange_info',
                  'profit_spread',
                  'ava_rise_below',
                  'ava_length',
                  'need_cover_buy_volume',
                  'need_cover_sell_volume'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['base_exchange_info'
                ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(Flash0xStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=4, on_window_bar=self.on_window_bar, interval=Interval.HOUR.value,
                               quick_minute=2)
        self.am = ArrayManager(10)

        self.max_price_tick = 0  # 所有交易所中最大的下单价
        self.total_volume_min = 0  # 所有交易中最大的最小交易量

        self.update_exchange_failed = False  # 更新交易所失败flag
        self.target_exchange_updated = True  # 目标更新
        self.base_exchange_updated = False  # 合并行情是否到达
        self.base_exchange_info = settings["base_exchange_info"]

        self.base_bid = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
        self.base_ask = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据

        self.profit_spread = settings.get("profit_spread", 0.6)
        self.base_spread = settings.get("base_spread", 0.8)

        self.hb_buyOrderDict = {}  # 回补发出去的买单
        self.hb_sellOrderDict = {}  # 回补发出去的卖单

        self.need_cover_buy_volume = 0  # 需要对冲回补的购买量
        self.need_cover_sell_volume = 0  # 需要对冲回补的卖出量

        self.cancel_dict_times_count = defaultdict(int)  # 对一个 vt_order_id 的撤单次数

        self.testing_send_flag = False
        self.update_account_flag = False  # 资金是否已经初始化完

        self.before_bid_price = 0
        self.before_ask_price = 0
        self.refresh_bid_time_bef = time.time() - 30
        self.refresh_ask_time_bef = time.time() - 30

        self.working_transfer_request = None

        self.last_flash_buy_price = 0
        self.last_flash_buy_volume = 0
        self.last_flash_sell_price = 0
        self.last_flash_sell_volume = 0

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))
        self.update_exchange_order_info()
        self.update_account()

    def update_exchange_order_info(self):
        """
        下面是下单时，交易所订单的要求
        """
        self.update_exchange_failed = False

        target_vt_symbol = get_vt_key(self.symbol_pair, self.target_exchange_info["exchange_name"])
        self.write_log("[update_exchange_order_info] target_vt_symbol:{}".format(target_vt_symbol))
        contract = self.get_contract(target_vt_symbol)
        if contract is None:
            self.update_exchange_failed = True
            update_msg = "target_contract:{} is not found!".format(target_vt_symbol)
            self.write_log(update_msg)
            return

        self.target_exchange_info["price_tick"] = contract.price_tick
        self.target_exchange_info["volume_tick"] = contract.volume_tick
        self.target_exchange_info["min_volume"] = contract.min_volume

        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]
            base_vt_symbol = get_vt_key(self.symbol_pair, exchange)
            contract = self.get_contract(base_vt_symbol)

            if contract is None:
                update_msg = "base_contract:{} is not found!".format(base_vt_symbol)
                self.update_exchange_failed = True
                self.write_log(update_msg)
                return

            dic["price_tick"] = contract.price_tick
            dic["volume_tick"] = contract.volume_tick
            dic["min_volume"] = contract.min_volume

            self.total_volume_min = max(self.total_volume_min, contract.min_volume)

    def update_account(self):
        self.update_account_flag = True

        # init
        key_acct_te_target_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.target_symbol)
        key_acct_te_base_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.base_symbol)

        acct_te_target = self.get_account(key_acct_te_target_symbol, self.target_exchange_info["address"])
        acct_te_base = self.get_account(key_acct_te_base_symbol, self.target_exchange_info["address"])
        if acct_te_target is not None:
            self.target_exchange_info["pos_target_symbol"] = acct_te_target.balance
        else:
            self.update_account_flag = False
            self.write_log("[update_account] acct_te_target is None, key:{}".format(key_acct_te_target_symbol))

        if acct_te_base is not None:
            self.target_exchange_info["pos_base_symbol"] = acct_te_base.balance
        else:
            self.update_account_flag = False
            self.write_log("[update_account] acct_te_base is None, key:{}".format(key_acct_te_base_symbol))

        self.write_log("[update_account] target_exchange_info:{}".format(self.target_exchange_info))

        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]

            dic["pos_target_symbol"] = 0
            dic["pos_base_symbol"] = 0

            key_target = get_vt_key(exchange, self.target_symbol)
            key_base = get_vt_key(exchange, self.base_symbol)

            acct_be_target = self.get_account(key_target)
            acct_be_base = self.get_account(key_base)

            if acct_be_target is not None:
                dic["pos_target_symbol"] = acct_be_target.balance * dic["target_symbol_percent_use"] / 100.0
            else:
                self.update_account_flag = False
                self.write_log(
                    "[update_account] acct_be_target is None, exchange:{},key_target:{}"
                        .format(exchange, key_target))

            if acct_be_base is not None:
                dic["pos_base_symbol"] = acct_be_base.balance * dic["base_symbol_percent_use"] / 100.0
            else:
                self.update_account_flag = False
                self.write_log(
                    "[update_account] acct_be_base is None, exchange:{},key_target:{}".format(exchange, key_base))

        self.write_log("[update_account] base_exchange_info:{}".format(self.base_exchange_info))

    def on_bar(self, bar):
        self.write_log(
            "[on_bar] [{}] high_price:{},low_price:{}".format(bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                              bar.high_price, bar.low_price))
        self.bg.update_bar(bar)

        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        ava_rise_array = Technique.avg_true_range(am.high_array, am.low_array, am.close_array, self.ava_length)
        self.ava_rise = ava_rise_array[-1] * 1.0 / bar.close_price

        self.write_log("[ava_rise]:{}".format(self.ava_rise))

    def on_window_bar(self, bar):
        pass

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > 5:
                if vt_order_id in self.hb_buyOrderDict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    del self.hb_buyOrderDict[vt_order_id]

                    self.write_log('[already buy set cancel] vt_order_id:{}'.format(vt_order_id))

                if vt_order_id in self.hb_sellOrderDict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    del self.hb_sellOrderDict[vt_order_id]

                    self.write_log('[already sell set cancel] vt_order_id:{}'.format(vt_order_id))

    def cancel_not_cover_order(self):
        """
        撤销掉那些 在回补市场，但是回补时间过长的订单，初步定义10秒不成交就撤单
        :return:
        """
        now = time.time()
        need_cancel_sets = set([])
        for vt_order_id, order in self.hb_buyOrderDict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 回补的订单，超过60秒就撤
            if now - order_time > 60 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                need_cancel_sets.add(order.vt_order_id)
                self.write_log('[prepare buy cover_order] vt_order_id:{},order.order_time:{},old_order_time:{}'
                               .format(vt_order_id, order_time, order.order_time))

        for vt_order_id, order in self.hb_sellOrderDict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 回补的订单，超过60秒就撤
            if now - order_time > 60 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                need_cancel_sets.add(order.vt_order_id)
                self.write_log('[prepare sell cover_order] vt_order_id:{},order.order_time:{},old_order_time:{}'
                               .format(vt_order_id, order_time, order.order_time))

        self.cancel_sets_order(need_cancel_sets)

    def get_frozen_volume(self, dic):
        frozen_send_volume_dict = {}
        all_frozen_send_volume = 0
        for exchange in self.base_exchange_info.keys():
            frozen_send_volume_dict[exchange] = 0

        for vt_order_id, order in dic.items():
            frozen_send_volume_dict[order.exchange] += order.volume - order.traded
            all_frozen_send_volume += order.volume - order.traded

        return frozen_send_volume_dict, all_frozen_send_volume

    def get_live_order_ids(self):
        return list(self.hb_buyOrderDict.keys()) + list(self.hb_sellOrderDict.keys())

    def check_risk(self):
        self.write_log(
            "[check_risk] self.ava_rise:{}, self.ava_rise_below:{}".format(self.ava_rise, self.ava_rise_below))
        if self.ava_rise < self.ava_rise_below:
            return False
        else:
            return True

    def cancel_all_orders(self):
        self.cancel_0x_order(self.symbol_pair, Direction.LONG.value)
        self.cancel_0x_order(self.symbol_pair, Direction.SHORT.value)

        self.write_log("[cancel_all_orders] symbol:{}".format(self.symbol_pair))
        self.write_important_log("[cancel_all_orders] symbol:{}".format(self.symbol_pair))

    def put_orders(self):
        now = time.time()
        frozen_sent_cover_buy_volume_dict, all_frozen_send_buy_volume = self.get_frozen_volume(self.hb_buyOrderDict)
        frozen_sent_cover_sell_volume_dict, all_frozen_send_sell_volume = self.get_frozen_volume(self.hb_sellOrderDict)

        #####
        ## TODO
        ## 这里后期需要优化， 调整到多交易所模式
        tmp_ava_sell_volume = 99999999
        for exchange in self.base_exchange_info.keys():
            # self.write_log(
            #     "tmp_ava_sell_volume:{},pos:{},frozen_sent_cover_sell_volume_dict:{}".format(tmp_ava_sell_volume,
            #              self.base_exchange_info[exchange]["pos_target_symbol"],
            #              frozen_sent_cover_sell_volume_dict[exchange]))
            tmp_ava_sell_volume = min(tmp_ava_sell_volume, (self.base_exchange_info[exchange]["pos_target_symbol"]
                                                            - frozen_sent_cover_sell_volume_dict[exchange]))

        can_huibu_sell_volume = 0
        for i in range(len(self.base_bid)):
            tt_price, tt_volume, tt_exchange = self.base_bid[i]
            if tt_price > 0:
                can_huibu_sell_volume += tt_volume

        # self.write_log(
        #     "pre_price:{}, self.base_bid0:{}, can_huibu_sell_volume:{} tmp_ava_sell_volume:{}".format(pre_price,
        #                                                                                               self.base_bid[0][
        #                                                                                                   0],
        #                                                                                               can_huibu_sell_volume,
        #                                                                                               tmp_ava_sell_volume))

        can_huibu_sell_volume = min(can_huibu_sell_volume, tmp_ava_sell_volume)
        target_exchange_buy_volume = self.target_exchange_info["pos_base_symbol"] / self.base_bid[0][0] * \
                                     self.target_exchange_info["base_symbol_percent_use"] / 100.0
        can_huibu_sell_volume = min(target_exchange_buy_volume, can_huibu_sell_volume)

        price = self.base_bid[0][0] * (1 - self.profit_spread / 100.0 - abs(self.ava_rise))
        price = get_round_order_price(price, self.target_exchange_info["price_tick"])
        if now - self.refresh_bid_time_bef <= 30 and self.before_bid_price:
            price = self.before_bid_price
        else:
            self.refresh_bid_time_bef = now
            self.before_bid_price = price
        volume = can_huibu_sell_volume
        volume = get_round_order_price(volume, self.target_exchange_info["volume_tick"])

        flag = False
        #self.write_log("buy volume:{}, total_volume_min:{}".format(volume, self.total_volume_min))
        if volume > self.total_volume_min:
            if is_number_change(price, self.last_flash_buy_price) or \
                    is_number_change(volume, self.last_flash_buy_volume):
                flag = self.flush_0x_order(self.symbol_pair, Direction.LONG.value, price, volume)
                if flag:
                    self.last_flash_buy_price = price
                    self.last_flash_buy_volume = volume
            # else:
            #     self.write_log("[flush_0x_order] LONG price or volume not change too lot ,last:{},{}!".format(
            #         self.last_flash_buy_price, self.last_flash_buy_volume))
        else:
            cancel_flag = self.cancel_0x_order(self.symbol_pair, Direction.LONG.value)
            if cancel_flag:
                self.write_log("[cancel flag] true, direction:{}".format(Direction.LONG.value))
                self.last_flash_buy_volume = 0
                self.last_flash_buy_price = 0
            else:
                self.write_log("[cancel flag] false, direction:{}".format(Direction.SHORT.value))
        if flag:
            self.write_log(
                "[flush_0x_order] flag:{}, direction:{}, price:{}, volume:{}".format(flag, Direction.LONG.value, price,
                                                                                     volume))
        # else:
        #     self.write_log(
        #         "[flush_0x_order] flag:{}, direction:{}, price:{}, volume:{}".format(flag, Direction.LONG.value, price,
        #                                                                              volume))

        #####
        ## TODO
        ## 这里后期需要优化， 调整到多交易所模式
        tmp_ava_buy_volume = 9999999
        for exchange in self.base_exchange_info.keys():
            # self.write_log(
            #     "tmp_ava_buy_volume:{},pos:{},frozen_sent_cover_buy_volume_dict:{}".format(tmp_ava_buy_volume,
            #     self.base_exchange_info[exchange]["pos_base_symbol"],
            #                             frozen_sent_cover_buy_volume_dict[exchange]))
            tmp_ava_buy_volume = min(tmp_ava_buy_volume,
                                     (self.base_exchange_info[exchange]["pos_base_symbol"] / self.base_bid[0][0]
                                      - frozen_sent_cover_buy_volume_dict[exchange]))

        can_huibu_buy_volume = 0
        for i in range(len(self.base_ask)):
            tt_price, tt_volume, tt_exchange = self.base_ask[i]
            if tt_price > 0:
                can_huibu_buy_volume += tt_volume

        # self.write_log("pre_price:{}, base_ask[0]:{}, can_huibu_buy_volume:{} tmp_ava_buy_volume:{}".format(pre_price,
        #                                                                                                     self.base_ask[
        #                                                                                                         0][0],
        #                                                                                                     can_huibu_buy_volume,
        #                                                                                                     tmp_ava_buy_volume))

        can_huibu_buy_volume = min(can_huibu_buy_volume, tmp_ava_buy_volume)

        target_exchange_sell_volume = self.target_exchange_info["pos_target_symbol"] * self.target_exchange_info[
            "target_symbol_percent_use"] / 100.0
        can_huibu_buy_volume = min(target_exchange_sell_volume, can_huibu_buy_volume)

        price = self.base_ask[0][0] * (1 + self.profit_spread / 100.0 + abs(self.ava_rise))
        price = get_round_order_price(price, self.target_exchange_info["price_tick"])
        if now - self.refresh_ask_time_bef <= 30 and self.before_ask_price:
            price = self.before_ask_price
        else:
            self.refresh_ask_time_bef = now
            self.before_ask_price = price
        volume = can_huibu_buy_volume
        volume = get_round_order_price(volume, self.target_exchange_info["volume_tick"])
        flag = False
        # self.write_log("sell volume:{}, total_volume_min:{}".format(volume, self.total_volume_min))
        if volume > self.total_volume_min:
            if is_number_change(self.last_flash_sell_price, price) or \
                    is_number_change(self.last_flash_sell_volume, volume):
                flag = self.flush_0x_order(self.symbol_pair, Direction.SHORT.value, price, volume)
                if flag:
                    self.last_flash_sell_price = price
                    self.last_flash_sell_volume = volume
            # else:
            #     self.write_log("[flush_0x_order] SHORT price or volume not change too lot!last:{},{}".format(
            #         self.last_flash_sell_price, self.last_flash_sell_volume
            #     ))
        else:
            cancel_flag = self.cancel_0x_order(self.symbol_pair, Direction.SHORT.value)
            if cancel_flag:
                self.last_flash_sell_price = 0
                self.last_flash_sell_volume = 0
                self.write_log("[cancel flag] true, direction:{}".format(Direction.SHORT.value))
            else:
                self.write_log("[cancel flag] false, direction:{}".format(Direction.SHORT.value))

        if flag:
            self.write_log(
                "[flush_0x_order] flag:{}, direction:{}, price:{}, volume:{}".format(flag, Direction.SHORT.value, price,
                                                                                     volume))
        # else:
        #     self.write_log(
        #         "[flush_0x_order] flag:{}, direction:{}, price:{}, volume:{}".format(flag, Direction.SHORT.value, price,
        #                                                                              volume))

    def cover_orders(self):
        if not self.trading:
            return

        """
        在回补市场进行市场价格补单
        """
        frozen_sent_cover_buy_volume_dict, frozen_sent_all_buy_volume = self.get_frozen_volume(self.hb_buyOrderDict)
        frozen_sent_cover_sell_volume_dict, frozen_sent_all_sell_volume = self.get_frozen_volume(self.hb_sellOrderDict)
        buy_order_req = []
        if self.need_cover_buy_volume > frozen_sent_all_buy_volume:
            self.write_log(
                "self.need_cover_buy_volume:{}, frozen_sent_all_buy_volume:{}".format(self.need_cover_buy_volume,
                                                                                      frozen_sent_all_buy_volume))
            tmp_deal_volume = abs(self.need_cover_buy_volume - frozen_sent_all_buy_volume) * 1.002
            if tmp_deal_volume > self.total_volume_min:
                buy_price = self.base_ask[0][0] * 1.004
                # 选择一个价格最好的市场 , 深度减去已经 frozen_send 的值
                tmp_dic = copy(frozen_sent_cover_buy_volume_dict)
                exchange_name = self.base_ask[0][2]
                for i in range(len(self.base_ask)):
                    tt_price, tt_volume, tt_exchange = self.base_ask[i]
                    if tt_price > 0:
                        if tt_exchange != self.target_exchange_info["exchange_name"]:
                            exchange_name = tt_exchange
                            if tmp_dic[exchange_name] < tt_volume:
                                buy_price = tt_price * 1.004
                                break
                            else:
                                tmp_dic[exchange_name] = tmp_dic[exchange_name] - tt_volume

                if exchange_name != self.target_exchange_info["exchange_name"]:
                    buy_price = get_round_order_price(buy_price, self.base_exchange_info[exchange_name]["price_tick"])
                    buy_volume = get_round_order_price(tmp_deal_volume,
                                                       self.base_exchange_info[exchange_name]["volume_tick"])
                    buy_order_req.append([exchange_name, Direction.LONG.value, buy_price, buy_volume])
                else:
                    self.write_log(
                        "cover_orders need_buy ,exchange_name:{} not right, self.base_ask:{}".format(exchange_name,
                                                                                                     self.base_ask))

        sell_order_req = []
        if self.need_cover_sell_volume > frozen_sent_all_sell_volume:
            self.write_log(
                "self.need_cover_sell_volume:{},self.frozen_sent_all_sell_volume:{}".format(
                    self.need_cover_sell_volume, frozen_sent_all_sell_volume))
            tmp_deal_volume = abs(self.need_cover_sell_volume - frozen_sent_all_sell_volume) * 1.002
            if tmp_deal_volume > self.total_volume_min:
                sell_price = self.base_bid[0][0] * 0.996
                # 选择一个价格最好的市场， 深度减去已经 frozen_send 的值
                tmp_dic = copy(frozen_sent_cover_sell_volume_dict)

                exchange_name = self.base_bid[0][2]
                for i in range(len(self.base_bid)):
                    tt_price, tt_volume, tt_exchange = self.base_bid[i]
                    if tt_price > 0:
                        if tt_exchange != self.target_exchange_info["exchange_name"]:
                            exchange_name = tt_exchange
                            if tmp_dic[exchange_name] < tt_volume:
                                sell_price = tt_price * 0.996
                                break
                            else:
                                tmp_dic[exchange_name] = tmp_dic[exchange_name] - tt_volume

                if exchange_name != self.target_exchange_info["exchange_name"]:
                    sell_price = get_round_order_price(sell_price, self.base_exchange_info[exchange_name]["price_tick"])
                    sell_volume = get_round_order_price(tmp_deal_volume,
                                                        self.base_exchange_info[exchange_name]["volume_tick"])
                    sell_order_req.append([exchange_name, Direction.SHORT.value, sell_price, sell_volume])

                else:
                    self.write_log(
                        "cover_orders need_sell ,exchange_name:{} not right, self.base_bid:{}".format(exchange_name,
                                                                                                      self.base_bid))

        # 发单
        for exchange, direction, price, volume in buy_order_req:
            ret_orders = self.send_order(self.symbol_pair, exchange, direction, Offset.OPEN.value, price,
                                         volume)
            for vt_order_id, order in ret_orders:
                self.hb_buyOrderDict[vt_order_id] = copy(order)
                self.write_log(
                    '[send buy cover order] direction:{}, price:{},volume:{},vt_order_id:{},order_time:{},status:{}'
                        .format(order.direction, order.price, order.volume, order.vt_order_id, order.order_time,
                                order.status))

        for exchange, direction, price, volume in sell_order_req:
            ret_orders = self.send_order(self.symbol_pair, exchange, direction, Offset.CLOSE.value, price,
                                         volume)
            for vt_order_id, order in ret_orders:
                self.hb_sellOrderDict[vt_order_id] = copy(order)
                self.write_log(
                    '[send sell cover order] direction:{}, price:{},volume:{},vt_order_id:{},order_time:{},status:{}'
                        .format(order.direction, order.price, order.volume, order.vt_order_id, order.order_time,
                                order.status))

    def on_merge_tick(self, merge_tick):
        try:
            if self.update_exchange_failed:
                self.write_log("contract data not right!")
                return

            if merge_tick.bids[0][0] > 0:
                new_base_bid = []
                for i in range(len(merge_tick.bids)):
                    tt_price, tt_volume, tt_exchange = merge_tick.bids[i]
                    if tt_price > 0 and tt_exchange in self.base_exchange_info.keys():
                        new_base_bid.append([tt_price, tt_volume, tt_exchange])

                if len(new_base_bid) > 0:
                    self.base_bid = new_base_bid

                new_base_ask = []
                for i in range(len(merge_tick.asks)):
                    tt_price, tt_volume, tt_exchange = merge_tick.asks[i]
                    if tt_price > 0 and tt_exchange in self.base_exchange_info.keys():
                        new_base_ask.append([tt_price, tt_volume, tt_exchange])

                if len(new_base_ask) > 0:
                    self.base_ask = new_base_ask

                if len(new_base_bid) > 0:
                    self.base_exchange_updated = True

                tick_data = TickData()
                tick_data.symbol = merge_tick.symbol
                tick_data.datetime = merge_tick.datetime
                tick_data.last_price = self.base_bid[0][0]
                self.bg.update_tick(tick_data)

            else:
                return

            # 表示是正常交易状态
            if self.trading:
                if self.base_exchange_updated and self.target_exchange_updated:
                    self.total_volume_min = max(self.total_volume_min,
                                                get_system_inside_min_volume(self.symbol_pair, self.base_bid[0][0],
                                                                             self.base_bid[0][2]))

                    self.cancel_not_cover_order()
                    self.process_transfer()
                    # if self.check_risk():
                    #     self.cancel_all_orders()
                    # else:
                    #     self.put_orders()
                    self.put_orders()
                    self.cover_orders()
        except Exception as ex:
            self.write_log("[on_merge_tick] [error] ex:{}".format(ex))

    def on_tick(self, tick):
        pass

    def on_flash_account(self, account):
        '''
        :param account:
        :return:
        更新 flash 这边情况信息
        '''
        if account.account_id == self.target_symbol:
            bef_pos_target_symbol = self.target_exchange_info["pos_target_symbol"]
            new_traded_target_symbol = bef_pos_target_symbol - account.balance
            if new_traded_target_symbol > 0:
                self.write_important_log(
                    "[new traded target] new_traded_target_symbol:{}, bef_pos_target_symbol:{}, balance:{}".format(
                        new_traded_target_symbol, bef_pos_target_symbol, account.balance))
                self.need_cover_buy_volume += new_traded_target_symbol
                self.cover_orders()
            self.target_exchange_info["pos_target_symbol"] = account.balance
        elif account.account_id == self.base_symbol:
            bef_pos_base_symbol = self.target_exchange_info["pos_base_symbol"]
            new_traded_base_symbol = bef_pos_base_symbol - account.balance
            if new_traded_base_symbol > 0:
                self.write_important_log(
                    "[new traded base] new_traded_base_symbol:{}, bef_pos_base_symbol:{}, balance:{}".format(
                        new_traded_base_symbol, bef_pos_base_symbol, account.balance))
                self.need_cover_sell_volume += new_traded_base_symbol / self.base_bid[0][0]
                self.cover_orders()
            self.target_exchange_info["pos_base_symbol"] = account.balance

        self.write_log("[on_flash_account] account account_id:{} balance:{} balance:{}".format(account.account_id,
                                                                                               account.available,
                                                                                               account.balance))
        self.update_account()

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.traded > 0:
            self.write_important_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
                order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume,
                order.traded))

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

        # 提交的订单推送，过滤掉
        if order.status == Status.SUBMITTING.value:
            return

        if order.exchange in self.base_exchange_info.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.hb_buyOrderDict.get(order.vt_order_id, None)
                if bef_order is not None:
                    new_traded = order.traded - bef_order.traded
                    self.hb_buyOrderDict[order.vt_order_id] = copy(order)

                    if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value, Status.REJECTED.value]:
                        del self.hb_buyOrderDict[order.vt_order_id]

                    if new_traded > 0:
                        self.need_cover_buy_volume -= new_traded
                        self.base_exchange_info[order.exchange]["pos_base_symbol"] -= new_traded * order.price
                        self.base_exchange_info[order.exchange]["pos_target_symbol"] += new_traded * 0.998

            else:
                bef_order = self.hb_sellOrderDict.get(order.vt_order_id, None)
                if bef_order is not None:
                    new_traded = order.traded - bef_order.traded
                    self.hb_sellOrderDict[order.vt_order_id] = copy(order)

                    if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value, Status.REJECTED.value]:
                        del self.hb_sellOrderDict[order.vt_order_id]

                    if new_traded > 0:
                        self.need_cover_sell_volume -= new_traded
                        self.base_exchange_info[order.exchange]["pos_target_symbol"] -= new_traded
                        self.base_exchange_info[order.exchange]["pos_base_symbol"] += order.price * new_traded * 0.998

        else:
            self.write_log("on_order order exchange not found! vt_order_id:{} ".format(order.vt_order_id))

    def on_trade(self, trade):
        """
                Callback of new trade data update.
                """
        self.write_log('[on_trade] start')
        self.write_log('[trade detail] :{}'.format(trade.__dict__))

        self.write_important_log(
            '[trade] symbol:{},exchange:{},direciton:{},price:{},volume:{}'.format(trade.symbol, trade.exchange,
                                                                                   trade.direction, trade.price,
                                                                                   trade.volume))

    def on_transfer(self, transfer_req):
        msg = "[process_transfer_event] :{}".format(transfer_req.__dict__)
        self.write_important_log(msg)

        if self.working_transfer_request:
            msg = "[process_transfer_event] already has transfer req! drop it!"
            self.write_important_log(msg)
            return

        if transfer_req.from_exchange != Exchange.FLASH.value:
            msg = "[process_transfer_event] from_exchange is not flash ! drop it! "
            self.write_important_log(msg)
            return

        if transfer_req.asset_id != self.target_symbol and transfer_req.asset_id != self.base_symbol:
            msg = "[process_transfer_event] asset_id:{} not existed".format(transfer_req.asset_id)
            self.write_important_log(msg)
            return

        self.working_transfer_request = copy(transfer_req)

        self.process_transfer()

    def go_transfer(self):
        if self.working_transfer_request:
            transfer_id = self.transfer_amount(self.working_transfer_request)
            msg = "[process_transfer] transfer_amount result:{}".format(transfer_id)
            self.write_important_log(msg)
            return transfer_id

    def process_transfer(self):
        if not self.working_transfer_request:
            return

        if self.working_transfer_request:
            now = time.time()
            # 超过一分钟的转账请求，丢弃掉
            if now - self.working_transfer_request.timestamp > 60:
                msg = "[process_transfer] drop working_transfer request for time exceed! a:{},b:{}".format(now,
                                                                                                           self.working_transfer_request.timestamp)
                self.write_important_log(msg)
                self.working_transfer_request = None
                return

        if self.working_transfer_request.asset_id == self.target_symbol:
            transfer_id = self.go_transfer()
            if transfer_id:
                msg = "[process_transfer] before need_cover_buy_volume:{}, transfer_amount:{}".format(
                    self.need_cover_buy_volume, self.working_transfer_request.transfer_amount)
                self.write_important_log(msg)
                self.need_cover_buy_volume -= self.working_transfer_request.transfer_amount
                msg = "[process_transfer] before need_cover_buy_volume:{}, transfer_amount:{}".format(
                    self.need_cover_buy_volume, self.working_transfer_request.transfer_amount)
                self.write_important_log(msg)
                self.working_transfer_request = None
            else:
                now = time.time()
                if now - self.working_transfer_request.timestamp > 20:
                    msg = "[process_transfer] send drop working_transfer request for time exceed! a:{},b:{}".format(
                        now, self.working_transfer_request.timestamp)
                    self.write_important_log(msg)
                    self.working_transfer_request = None
        elif self.working_transfer_request.asset_id == self.base_symbol:
            transfer_id = self.go_transfer()
            if transfer_id:
                msg = "[process_transfer] before need_cover_sell_volume:{}, transfer_amount:{}".format(
                    self.need_cover_sell_volume, self.working_transfer_request.transfer_amount)
                self.write_important_log(msg)
                self.need_cover_sell_volume -= self.working_transfer_request.transfer_amount / self.base_ask[0][0]
                msg = "[process_transfer] after need_cover_sell_volume:{}, transfer_amount:{}".format(
                    self.need_cover_sell_volume, self.working_transfer_request.transfer_amount)
                self.write_important_log(msg)
                self.working_transfer_request = None
            else:
                now = time.time()
                if now - self.working_transfer_request.timestamp > 20:
                    msg = "[process_transfer] send drop working_transfer request for time exceed! a:{},b:{}".format(
                        now, self.working_transfer_request.timestamp)
                    self.write_important_log(msg)
                    self.working_transfer_request = None
