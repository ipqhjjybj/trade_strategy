# coding=utf-8

import time
from copy import copy
from collections import defaultdict

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.constant import MAX_PRICE_NUM, Exchange
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.constant import Direction, Status, Offset
from tumbler.function import get_system_inside_min_volume
from tumbler.function import is_price_volume_too_small


class MarketMakerV1Strategy(MarketMakerTemplate):
    author = "ipqhjjybj"
    class_name = "MarketMakerV1Strategy"

    """
    默认参数
    """
    symbol_pair = "btc_usdt"
    vt_symbols_subscribe = []
    target_symbol = "btc"
    base_symbol = "usdt"
    target_exchange_info = {}
    base_exchange_info = {}
    profit_spread = 0.5
    base_spread = 0.6
    put_order_num = 1
    inc_spread = 0

    retry_cancel_send_num = 3

    fee_rate = 0.1  # 手续费千1
    cover_inc_rate = 0.4  # 市价单回补时，价格多加上这个系数

    need_cover_buy_volume = 0
    need_cover_sell_volume = 0
    frozen_sent_buy_volume = 0
    frozen_sent_sell_volume = 0

    """
    对于 btc_usdt
    target_symbol  指 btc
    base_symbol  指 usdt
    """
    # 参数列表
    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对 如 btc_udst
                  'vt_symbols_subscribe',  # 订阅的交易对，如 ["btc_usdt".GATEIO,"btc_usdt".AGGR]
                  'target_symbol',  # 如btc_usdt中的btc
                  'base_symbol',  # 如btc_usdt中的usdt
                  'target_exchange_info',  # 目标交易所信息
                  'base_exchange_info',  # 回补交易所信息
                  'profit_spread',  # 能盈利的价差
                  'base_spread',  # 挂单相比较对冲市场的基础价格
                  'put_order_num',  # 单侧挂单的挂单数量
                  'need_cover_buy_volume',  # 需要回补的买量
                  'need_cover_sell_volume',  # 需要回补的卖量
                  'frozen_sent_buy_volume',  # 已经挂单出去的回补量
                  'frozen_sent_sell_volume',  # 已经挂单出去的回补量
                  'inc_spread']  # 挂单差值迭代 , 如果 inc_spread 是, 那么就表示1个price_tick的增加

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]
    """
    运行时  
    target_exchange_info = {
        "exchange_name":"GATEIO",
        "pos_target_symbol":0,              # target_symbol总量
        "pos_base_symbol":0,                # base_symbol总量
        "frozen_target_symbol":0,           # target_symbol冻结量
        "frozen_base_symbol:0",             # base_symbol冻结量
        "price_tick":0,                      # 最小交易价格跳动
        "volume_tick":0,                     # 最小交易数量跳动
        "min_volume":0                      # 最小交易数量
    }
    base_exchange_info = {
        "HUOBI":{
            "pos_target_symbol":0,          # 目标品种 target_symbol 当前运行时数量
            "pos_base_symbol":0,            # 目标品种 base_symbol 当前基础数量
            "frozen_sent_buy_volume":0,     # 已经发单出去的购买量 , 防止过多买入
            "frozen_sent_sell_volume":0,    # 已经发单出去的卖出量 , 防止过多卖出
            "price_tick":0,                  # 最小交易价格跳动
            "volume_tick":0,                 # 最小交易数量跳动
            "min_volume":0                  # 最小交易数量
        }
    }
    """
    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['target_exchange_info',
                'base_exchange_info'
                ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MarketMakerV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.max_price_tick = 0  # 所有交易所中最大的下单价
        self.max_min_volume = 0  # 所有交易中最大的最小交易量

        self.update_failed = False  # 初始化是否成功
        self.base_exchange_updated = False  # 合并行情是否到达
        self.target_exchange_updated = False  # 目标交易所行情是否到达

        self.target_exchange_info = settings["target_exchange_info"]  # 交易所运行仓位、下单限制等信息
        self.base_exchange_info = settings["base_exchange_info"]  # 交易所运行仓位、下单限制等信息

        for key in self.base_exchange_info.keys():
            dic = self.base_exchange_info[key]
            dic["frozen_sent_buy_volume"] = 0
            dic["frozen_sent_sell_volume"] = 0

        self.base_bid = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
        self.base_ask = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据

        self.profit_spread = settings.get("profit_spread", 0.5)

        self.target_buy_prices = [0.0] * MAX_PRICE_NUM
        self.target_buy_volumes = [0.0] * MAX_PRICE_NUM
        self.target_ask_prices = [0.0] * MAX_PRICE_NUM
        self.target_ask_volumes = [0.0] * MAX_PRICE_NUM

        self.zs_buyOrderDict = {}  # 维护的挂单买单
        self.zs_sellOrderDict = {}  # 维护的挂单卖单

        self.need_cover_buy_volume = 0  # 需要对冲回补的购买量
        self.need_cover_sell_volume = 0  # 需要对冲回补的卖出量

        self.frozen_sent_buy_volume = 0  # 冻结发出的量
        self.frozen_sent_sell_volume = 0  # 冻结发出的量

        self.hb_buyOrderDict = {}  # 回补发出去的买单
        self.hb_sellOrderDict = {}  # 回补发出去的卖单

        self.dirty_order_dict = {}  # 已经被强制准备删除的订单

        self.total_volume_min = 0  # 最大的最小交易数量

        self.cancel_dict_times_count = defaultdict(int)  # 对一个 vt_order_id 的撤单次数

        self.testing_send_flag = False
        self.update_account_flag = False  # 资金是否已经初始化完

        self.working_transfer_request = None

        self.mov_xishu_buy = 1
        self.mov_xishu_sell = 1

    def on_init(self):
        self.update_exchange_order_info()
        self.update_account()

    def update_exchange_order_info(self):
        """
        下面是加载下单时，交易所订单的要求
        """
        self.update_failed = False
        update_msg = ""

        target_vt_symbol = get_vt_key(self.symbol_pair, self.target_exchange_info["exchange_name"])
        contract = self.get_contract(target_vt_symbol)

        if contract is None:
            self.update_failed = True
            update_msg = "target_contract:{} is not found!".format(target_vt_symbol)
            self.write_log(update_msg)
            return

        self.target_exchange_info["price_tick"] = contract.price_tick
        self.target_exchange_info["volume_tick"] = contract.volume_tick
        self.target_exchange_info["min_volume"] = contract.min_volume

        self.total_volume_min = max(self.total_volume_min, contract.min_volume)

        # 如果回补市场有 挂单市场这个交易所，删掉 ,不是很重要
        if self.target_exchange_info["exchange_name"] in self.base_exchange_info.keys():
            del self.base_exchange_info[self.target_exchange_info["exchange_name"]]

        for exchange_name in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange_name]
            base_vt_symbol = get_vt_key(self.symbol_pair, exchange_name)

            contract = self.get_contract(base_vt_symbol)

            if contract is None:
                update_msg = "base_contract:{} is not found!".format(base_vt_symbol)
                self.write_log(update_msg)
                return

            dic["price_tick"] = contract.price_tick
            dic["volume_tick"] = contract.volume_tick
            dic["min_volume"] = contract.min_volume

            self.total_volume_min = max(self.total_volume_min, contract.min_volume)

    def check_is_account_needed_enough(self):
        """
        检测 账户的资产，是否小于最低 资产需求。 如果小于， 应该是要发邮件告知了
        """
        flag = True
        if self.target_exchange_info["pos_target_symbol"] < self.target_exchange_info["target_symbol_min_need"]:
            flag = False
            self.write_log("[check_is_account_needed_enough] pos_target_symbol:{} ,pos:{} is smaller than:{}" \
                           .format(self.target_symbol, self.target_exchange_info["pos_target_symbol"], \
                                   self.target_exchange_info["target_symbol_min_need"]))

        if self.target_exchange_info["pos_base_symbol"] < self.target_exchange_info["base_symbol_min_need"]:
            flag = False
            self.write_log("[check_is_account_needed_enough] pos_base_symbol:{} ,pos:{} is smaller than:{}" \
                           .format(self.base_symbol, self.target_exchange_info["pos_base_symbol"], \
                                   self.target_exchange_info["base_symbol_min_need"]))

        for exchange_name in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange_name]
            if dic["pos_target_symbol"] < dic["target_symbol_min_need"]:
                flag = False
                self.write_log("[check_is_account_needed_enough] exchange:{},symbol:{},pos:{} is small than min_need:{}" \
                               .format(exchange_name, self.target_symbol, dic["pos_target_symbol"],
                                       dic["target_symbol_min_need"]))
                break

            if dic["pos_base_symbol"] < dic["base_symbol_min_need"]:
                flag = False
                self.write_log("[check_is_account_needed_enough] exchange:{},symbol:{},pos:{} is small than min_need:{}" \
                               .format(exchange_name, self.target_symbol, dic["pos_base_symbol"],
                                       dic["base_symbol_min_need"]))

        return flag

    def update_account(self):
        """
        下面是加载当前的资金情况
        """
        # update target_exchange_info
        self.update_account_flag = True
        # init
        self.target_exchange_info["pos_target_symbol"] = 0
        self.target_exchange_info["pos_base_symbol"] = 0

        key_acct_te_target_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.target_symbol)
        key_acct_te_base_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.base_symbol)

        acct_te_target = self.get_account(key_acct_te_target_symbol)
        acct_te_base = self.get_account(key_acct_te_base_symbol)

        if acct_te_target is not None:
            self.target_exchange_info["pos_target_symbol"] = acct_te_target.available * self.target_exchange_info[
                "target_symbol_percent_use"] / 100.0
        else:
            self.update_account_flag = False
            self.write_log("[update_account] acct_te_target is None, key:{}".format(key_acct_te_target_symbol))

        if acct_te_base is not None:
            self.target_exchange_info["pos_base_symbol"] = acct_te_base.available * self.target_exchange_info[
                "base_symbol_percent_use"] / 100.0
        else:
            self.update_account_flag = False
            self.write_log("[update_account] acct_te_base is None, key:{}".format(key_acct_te_base_symbol))

        if self.target_exchange_info["exchange_name"] == Exchange.MOV.value:
            already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
                self.zs_buyOrderDict)
            self.target_exchange_info["pos_base_symbol"] += already_cover_base_volume
            already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
                self.zs_sellOrderDict)
            self.target_exchange_info["pos_target_symbol"] += already_cover_target_volume

        for exchange_name in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange_name]

            dic["pos_target_symbol"] = 0
            dic["pos_base_symbol"] = 0
            dic["frozen_target_symbol"] = 0
            dic["frozen_base_symbol"] = 0

            key_target = get_vt_key(exchange_name, self.target_symbol)
            key_base = get_vt_key(exchange_name, self.base_symbol)

            acct_be_target = self.get_account(key_target)
            acct_be_base = self.get_account(key_base)

            if acct_be_target is not None:
                dic["pos_target_symbol"] = acct_be_target.available * dic["target_symbol_percent_use"] / 100.0
            else:
                self.update_account_flag = False
                self.write_log(
                    "[update_account] acct_be_target is None, exchange:{},key_target:{}".format(exchange_name,
                                                                                                key_target))

            if acct_be_base is not None:
                dic["pos_base_symbol"] = acct_be_base.available * dic["base_symbol_percent_use"] / 100.0
            else:
                self.update_account_flag = False
                self.write_log(
                    "[update_account] acct_be_base is None, exchange:{},key_target:{}".format(exchange_name, key_base))

        self.write_log("[update_account] target_exchange_info:{}".format(self.target_exchange_info))
        self.write_log("[update_account] base_exchange_info:{}".format(self.base_exchange_info))

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))

        self.put_event()

    def get_live_order_ids(self):
        return list(self.hb_buyOrderDict.keys()) + list(self.hb_sellOrderDict.keys()) + list(
            self.zs_buyOrderDict.keys()) + list(self.zs_sellOrderDict.keys())

    def cancel_not_cover_order(self):
        """
        撤销掉那些 在回补市场，但是回补时间过长的订单，初步定义10秒不成交就撤单
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

    def cancel_time_too_long_order(self):
        """
        撤销掉那些 时间很长的挂单 ,因为可能是 
        """
        # 对于买单
        now = time.time()
        need_cancel_sets = set([])
        for vt_order_id, order in self.zs_buyOrderDict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 超过180秒的订单撤掉重发 ,测试期间10秒
            if now - order_time > 300 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.write_log(
                    "[cancel too long time],zs_buy order_id:{} now:{},order_time:{}".format(order.vt_order_id, now,
                                                                                            order_time))
                need_cancel_sets.add(order.vt_order_id)

        for vt_order_id, order in self.zs_sellOrderDict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 超过180秒的订单撤掉重发 ,测试期间10秒
            if now - order_time > 300 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.write_log(
                    "[cancel too long time],zs_sell order_id:{} now:{},order_time:{}".format(order.vt_order_id, now,
                                                                                             order_time))
                need_cancel_sets.add(order.vt_order_id)

        self.cancel_sets_order(need_cancel_sets)

        # 对于垃圾单, 直接删掉超过1小时的那种
        need_delete_order_ids = []
        for vt_order_id, order in self.dirty_order_dict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))
            if now - order_time > 3600:
                self.write_log("[cancel dirty order] order_id:{}".format(vt_order_id))
                need_delete_order_ids.append(vt_order_id)

        for vt_order_id in need_delete_order_ids:
            if vt_order_id in self.dirty_order_dict.keys():
                self.dirty_order_dict.pop(vt_order_id)

    def cancel_not_profit_orders(self):
        """
        撤掉那些不盈利的单子
        """
        # 对于买单
        need_cancel_sets = set([])
        for vt_order_id, order in self.zs_buyOrderDict.items():
            if order.status != Status.SUBMITTING.value:
                if order.price >= self.base_bid[0][0] * (1 - self.profit_spread / 100.0):
                    self.write_log("[not profit price] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

                can_make_cover_volume = 0
                for i in range(len(self.base_bid)):
                    tt_price, tt_volume, tt_exchange = self.base_bid[i]
                    if tt_price > 0 and tt_price * (1 - self.profit_spread / 100.0) > order.price:
                        can_make_cover_volume += tt_volume

                if order.volume - order.traded >= can_make_cover_volume:
                    self.write_log("[lack cover volume] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

        # 对于卖单
        for vt_order_id, order in self.zs_sellOrderDict.items():
            if order.status != Status.SUBMITTING.value:
                if order.price <= self.base_ask[0][0] * (1 + self.profit_spread / 100.0):
                    self.write_log("[not profit price] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

                can_make_cover_volume = 0
                for i in range(len(self.base_ask)):
                    tt_price, tt_volume, tt_exchange = self.base_ask[i]
                    if tt_price > 0 and tt_price * (1 + self.profit_spread / 100.0) < order.price:
                        can_make_cover_volume += tt_volume

                if order.volume - order.traded >= can_make_cover_volume:
                    self.write_log("[lack cover volume] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

        self.cancel_sets_order(need_cancel_sets)

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.zs_buyOrderDict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.zs_buyOrderDict[vt_order_id]
                    del self.zs_buyOrderDict[vt_order_id]

                    self.dirty_order_dict[vt_order_id] = copy(order)
                if vt_order_id in self.zs_sellOrderDict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.zs_sellOrderDict[vt_order_id]
                    del self.zs_sellOrderDict[vt_order_id]

                    self.dirty_order_dict[vt_order_id] = copy(order)

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.hb_buyOrderDict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.hb_buyOrderDict[vt_order_id]
                    self.frozen_sent_buy_volume -= order.volume - order.traded
                    del self.hb_buyOrderDict[vt_order_id]

                    dic = self.base_exchange_info.get(order.exchange, {})
                    if dic:
                        dic["frozen_sent_buy_volume"] -= order.volume - order.traded

                    self.write_log('[already buy set cancel] vt_order_id:{}'.format(vt_order_id))

                if vt_order_id in self.hb_sellOrderDict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.hb_sellOrderDict[vt_order_id]
                    self.frozen_sent_sell_volume -= order.volume - order.traded
                    del self.hb_sellOrderDict[vt_order_id]

                    dic = self.base_exchange_info.get(order.exchange, {})
                    if dic:
                        dic["frozen_sent_sell_volume"] -= order.volume - order.traded

                    self.write_log('[already sell set cancel] vt_order_id:{}'.format(vt_order_id))

    def get_inc_price(self, price):
        if self.inc_spread == 0:
            return self.target_exchange_info["price_tick"]
        else:
            return self.inc_spread * price / 100.0

    def get_make_cover_volume(self, dic):
        already_need_make_cover_target_volume = 0
        already_need_make_cover_base_volume = 0
        already_price_volumes = []
        for key, s_order in dic.items():
            volume = s_order.volume - s_order.traded
            already_price_volumes.append((s_order.price, volume))
            already_need_make_cover_target_volume += volume
            already_need_make_cover_base_volume += s_order.price * volume
        return already_need_make_cover_target_volume, already_need_make_cover_base_volume, already_price_volumes

    def get_max_buy_order_price(self):
        _, _, already_price_volumes = self.get_make_cover_volume(self.zs_buyOrderDict)
        already_price_volumes.sort(reverse=True)
        if len(already_price_volumes) > 0:
            return already_price_volumes[0][0]
        else:
            return 0

    def get_min_sell_order_price(self):
        _, _, already_price_volumes = self.get_make_cover_volume(self.zs_sellOrderDict)
        already_price_volumes.sort()
        if len(already_price_volumes) > 0:
            return already_price_volumes[0][0]
        else:
            return 999999999

    def put_orders(self):
        """
        在挂单市场进行下单
        1.检测余额是否充足
        2.下单价格是否盈利
        3.跟前面下的单子相比，间距是否合理
        """
        ##########################################
        # 下买单
        already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
            self.zs_buyOrderDict)
        already_price_volumes.sort(reverse=True)
        n_buy_nums = len(already_price_volumes)
        left_buy_num = self.put_order_num - n_buy_nums

        tmp_ava_base_volume = self.target_exchange_info["pos_base_symbol"] - already_cover_base_volume

        #### 这里先粗略的，取回补市场的所有最小值, 后期可以优化
        for exchange_name in self.base_exchange_info.keys():
            tmp_ava_base_volume = min(tmp_ava_base_volume,
                                      (self.base_exchange_info[exchange_name]["pos_target_symbol"] -
                                       already_cover_target_volume - self.need_cover_sell_volume -
                                       self.base_exchange_info[exchange_name]["frozen_target_symbol"]) *
                                      self.base_bid[0][0] / (1 + self.fee_rate / 100.0))

        if tmp_ava_base_volume > self.total_volume_min * self.base_bid[0][0] * left_buy_num:
            if left_buy_num > 0:
                tmp_ava_base_volume /= left_buy_num

                if self.target_exchange_info["exchange_name"] == Exchange.MOV.value:
                    tmp_ava_base_volume /= self.mov_xishu_buy

        bef_sum_send_order_volume = 0
        accu_volume = 0
        start_buy_price = self.base_bid[0][0] * (1 - self.base_spread / 100.0)

        run_count = 0
        new_req_buy_orders = []  # [ (Direction.value.buy), price, volume]

        while left_buy_num > 0 and tmp_ava_base_volume > self.max_min_volume * start_buy_price * 1.05:
            # 确定价格
            while n_buy_nums > 0:
                if start_buy_price - self.get_inc_price(start_buy_price) >= already_price_volumes[0][0]:
                    break
                else:
                    n_buy_nums = n_buy_nums - 1
                    old_price, old_volume = already_price_volumes.pop(0)
                    start_buy_price = min(start_buy_price, old_price - self.get_inc_price(old_price))
                    accu_volume += old_volume

            # 确定数量
            can_make_cover_volume = 0
            for i in range(len(self.base_bid)):
                tt_price, tt_volume, tt_exchange = self.base_bid[i]
                if tt_price > 0:
                    if tt_price * (1 - self.profit_spread / 100.0) > start_buy_price:
                        can_make_cover_volume += tt_volume

            can_make_cover_volume = can_make_cover_volume / 2.0 - accu_volume - bef_sum_send_order_volume
            # 没回补的量，不进行了
            if can_make_cover_volume <= 0:
                break

            # 挂单价太小，忽略
            if start_buy_price < 1e-12:
                break

            can_make_cover_volume = min(can_make_cover_volume, tmp_ava_base_volume / start_buy_price)

            start_buy_price = get_round_order_price(start_buy_price, self.target_exchange_info["price_tick"])
            can_make_cover_volume = get_round_order_price(can_make_cover_volume,
                                                          self.target_exchange_info["volume_tick"])

            # 如果上面round后的值，反而不能盈利了，就减去一个price_tick
            if start_buy_price > self.base_bid[0][0] * (1 - self.profit_spread / 100.0):
                start_buy_price -= self.target_exchange_info["price_tick"]

            # 发单需要大于最小发单量
            if can_make_cover_volume > self.total_volume_min:
                # 价格跟数量太小了， 这时候不需要继续 step计算下去了
                if is_price_volume_too_small(self.symbol_pair, start_buy_price, can_make_cover_volume):
                    msg = "[buy check] is_price_volume_too_small,symbol_pair:{},price:{},volume:{}".format(
                        self.symbol_pair, start_buy_price, can_make_cover_volume)
                    break

                min_ask_order_price = self.get_min_sell_order_price()
                use_price = min(start_buy_price, self.target_ask_prices[0])
                use_price = min(use_price, min_ask_order_price - self.target_exchange_info["price_tick"])
                new_req_buy_orders.append((Direction.LONG.value, use_price, can_make_cover_volume))

                bef_sum_send_order_volume += can_make_cover_volume
                left_buy_num = left_buy_num - 1
                tmp_ava_base_volume = tmp_ava_base_volume - start_buy_price * can_make_cover_volume

            start_buy_price = start_buy_price - self.get_inc_price(start_buy_price)
            run_count += 1

            # 计算价格太多次数了， 强制结束
            if run_count > 3 * self.put_order_num:
                break

        # self.write_log("[new_req_buy_orders]:{} n_buy_nums:{}".format(new_req_buy_orders, n_buy_nums))
        if len(new_req_buy_orders) + n_buy_nums < self.put_order_num:
            self.write_log("[put_order] len(new_req_buy_orders) + n_buy_nums < self.put_order_num")
            self.update_account()
        ###########################

        # 下卖单
        already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
            self.zs_sellOrderDict)
        already_price_volumes.sort()
        n_sell_nums = len(already_price_volumes)
        left_sell_num = self.put_order_num - n_sell_nums

        tmp_ava_target_volume = self.target_exchange_info["pos_target_symbol"] - already_cover_target_volume

        # 这里先粗略的取所有回补市场的最小值
        for exchange_name in self.base_exchange_info.keys():
            tmp_ava_target_volume = min(tmp_ava_target_volume,
                                        (self.base_exchange_info[exchange_name]["pos_base_symbol"] -
                                         already_cover_base_volume -
                                         self.base_exchange_info[exchange_name]["frozen_base_symbol"]) /
                                        self.base_bid[0][0] / (1 + self.fee_rate / 100.0) - self.need_cover_buy_volume)

        if tmp_ava_target_volume > self.total_volume_min * left_sell_num:
            if left_sell_num > 0:
                tmp_ava_target_volume /= left_sell_num

                if self.target_exchange_info["exchange_name"] == Exchange.MOV.value:
                    tmp_ava_target_volume /= self.mov_xishu_sell

        bef_sum_send_order_volume = 0
        accu_volume = 0
        start_sell_price = self.base_ask[0][0] * (1 + self.base_spread / 100.0)

        run_count = 0
        new_req_sell_orders = []  # [ (Direction.value.sell), price, volume]
        while left_sell_num > 0 and tmp_ava_target_volume > self.max_min_volume:
            # 确定价格
            while n_sell_nums > 0:
                if start_sell_price + self.get_inc_price(start_sell_price) <= already_price_volumes[0][0]:
                    break
                else:
                    n_sell_nums = n_sell_nums - 1
                    old_price, old_volume = already_price_volumes.pop(0)
                    start_sell_price = max(start_sell_price, old_price + self.get_inc_price(old_price))
                    accu_volume += old_volume

            # 确定数量
            can_make_cover_volume = 0
            for i in range(len(self.base_ask)):
                tt_price, tt_volume, tt_exchange = self.base_ask[i]
                if tt_price > 0:
                    if tt_price * (1 + self.profit_spread / 100.0) < start_sell_price:
                        can_make_cover_volume += tt_volume

            can_make_cover_volume = can_make_cover_volume / 2.0 - accu_volume - bef_sum_send_order_volume

            # 没回补的量，不进行了
            if can_make_cover_volume <= 0:
                break

            # 挂单价太小，表示出问题了，先break
            if start_sell_price < 1e-12:
                break

            can_make_cover_volume = min(can_make_cover_volume, tmp_ava_target_volume)

            start_sell_price = get_round_order_price(start_sell_price, self.target_exchange_info["price_tick"])
            can_make_cover_volume = get_round_order_price(can_make_cover_volume,
                                                          self.target_exchange_info["volume_tick"])

            # 如果上面round后的价格，反而不能盈利了。那么特殊处理一下
            if start_sell_price < self.base_ask[0][0] * (1 + self.profit_spread / 100.0):
                start_sell_price += self.target_exchange_info["price_tick"]

            # 发单需要大于最小发单量
            if can_make_cover_volume > self.total_volume_min:
                max_order_buy_price = self.get_max_buy_order_price()
                use_price = max(self.target_buy_prices[0], start_sell_price)
                use_price = max(use_price, max_order_buy_price + self.target_exchange_info["price_tick"])
                new_req_sell_orders.append((Direction.SHORT.value, use_price, can_make_cover_volume))

                bef_sum_send_order_volume += can_make_cover_volume
                left_sell_num = left_sell_num - 1
                tmp_ava_target_volume = tmp_ava_target_volume - can_make_cover_volume

            start_sell_price = start_sell_price + self.get_inc_price(start_sell_price)
            run_count += 1
            if run_count > self.put_order_num * 3:
                break

        if len(new_req_sell_orders) + n_sell_nums < self.put_order_num:
            self.write_log("[put_order] len(new_req_sell_orders) + n_sell_nums < self.put_order_num")
            self.update_account()

        ##########################################
        # 进行发单
        # 买单
        has_error_buy_flag = False
        for (d, price, volume) in new_req_buy_orders:
            ret_orders = self.send_order(self.symbol_pair, self.target_exchange_info["exchange_name"], \
                                         Direction.LONG.value, Offset.OPEN.value, price, volume)

            for vt_order_id, order in ret_orders:
                if vt_order_id is not None and order is not None and order.is_active():
                    self.zs_buyOrderDict[vt_order_id] = order

                elif vt_order_id is None:
                    has_error_buy_flag = True

        if has_error_buy_flag:
            self.mov_xishu_buy *= 2
        else:
            self.mov_xishu_buy = 1

        has_error_sell_flag = False
        # 卖单
        for (d, price, volume) in new_req_sell_orders:
            ret_orders = self.send_order(self.symbol_pair, self.target_exchange_info["exchange_name"], \
                                         Direction.SHORT.value, Offset.CLOSE.value, price, volume)
            for vt_order_id, order in ret_orders:
                if vt_order_id is not None and order is not None and order.is_active():
                    self.zs_sellOrderDict[vt_order_id] = order
                elif vt_order_id is None:
                    has_error_sell_flag = True

        if has_error_sell_flag:
            self.mov_xishu_sell *= 2
        else:
            self.mov_xishu_sell = 1

    def cover_orders(self):
        """
        在回补市场进行市场价格补单
        """
        # 买单
        buy_order_req = []  # (exchange, direction , price, volume)
        if self.need_cover_buy_volume > self.frozen_sent_buy_volume:
            tmp_deal_volume = abs(self.need_cover_buy_volume - self.frozen_sent_buy_volume) * (
                    1 + self.fee_rate / 100.0)
            if tmp_deal_volume > self.total_volume_min:
                buy_price = self.base_ask[0][0] * (1 + self.cover_inc_rate / 100.0)
                # 选择一个价格最好的市场 , 深度减去已经 frozen_send 的值
                tmp_dic = {}
                for exchange_name in self.base_exchange_info.keys():
                    tmp_dic[exchange_name] = self.base_exchange_info[exchange_name]["frozen_sent_buy_volume"]

                exchange_name = self.base_ask[0][2]
                for i in range(len(self.base_ask)):
                    tt_price, tt_volume, tt_exchange = self.base_ask[i]
                    if tt_price > 0:
                        if tt_exchange != self.target_exchange_info["exchange_name"]:
                            exchange_name = tt_exchange
                            if tmp_dic[exchange_name] < tt_volume:
                                buy_price = tt_price * (1 + self.cover_inc_rate / 100.0)
                                break
                            else:
                                tmp_dic[exchange_name] = tmp_dic[exchange_name] - tt_volume

                if exchange_name != self.target_exchange_info["exchange_name"]:
                    buy_price = get_round_order_price(buy_price, self.base_exchange_info[exchange_name]["price_tick"])
                    buy_volume = get_round_order_price(tmp_deal_volume,
                                                       self.base_exchange_info[exchange_name]["volume_tick"])
                    buy_order_req.append([exchange_name, Direction.LONG.value, buy_price, buy_volume])

                    self.base_exchange_info[exchange_name]["frozen_sent_buy_volume"] += buy_volume
                    self.frozen_sent_buy_volume += buy_volume
                else:
                    self.write_log(
                        "cover_orders need_buy ,exchange_name:{} not right, self.base_ask:{}".format(exchange_name,
                                                                                                     self.base_ask))

        # 卖单
        sell_order_req = []  # (exchange, direction, price, volume)
        if self.need_cover_sell_volume > self.frozen_sent_sell_volume:
            tmp_deal_volume = abs(self.need_cover_sell_volume - self.frozen_sent_sell_volume) * (
                    1 + self.fee_rate / 100.0)
            if tmp_deal_volume > self.total_volume_min:
                sell_price = self.base_bid[0][0] * (1 - self.cover_inc_rate / 100.0)
                # 选择一个价格最好的市场， 深度减去已经 frozen_send 的值
                tmp_dic = {}
                for exchange_name in self.base_exchange_info.keys():
                    tmp_dic[exchange_name] = self.base_exchange_info[exchange_name]["frozen_sent_sell_volume"]

                exchange_name = self.base_bid[0][2]
                for i in range(len(self.base_bid)):
                    tt_price, tt_volume, tt_exchange = self.base_bid[i]
                    if tt_price > 0:
                        if tt_exchange != self.target_exchange_info["exchange_name"]:
                            exchange_name = tt_exchange
                            if tmp_dic[exchange_name] < tt_volume:
                                sell_price = tt_price * (1 - self.cover_inc_rate / 100.0)
                                break
                            else:
                                tmp_dic[exchange_name] = tmp_dic[exchange_name] - tt_volume

                if exchange_name != self.target_exchange_info["exchange_name"]:
                    sell_price = get_round_order_price(sell_price, self.base_exchange_info[exchange_name]["price_tick"])
                    sell_volume = get_round_order_price(tmp_deal_volume,
                                                        self.base_exchange_info[exchange_name]["volume_tick"])
                    sell_order_req.append([exchange_name, Direction.SHORT.value, sell_price, sell_volume])

                    self.base_exchange_info[exchange_name]["frozen_sent_sell_volume"] += sell_volume
                    self.frozen_sent_sell_volume += sell_volume
                else:
                    self.write_log(
                        "cover_orders need_sell ,exchange_name:{} not right, self.base_bid:{}".format(exchange_name,
                                                                                                      self.base_bid))

        # 发单
        for exchange, direction, price, volume in buy_order_req:
            ret_orders = self.send_order(self.symbol_pair, exchange, direction, Offset.OPEN.value, price, volume)
            for vt_order_id, order in ret_orders:
                self.hb_buyOrderDict[vt_order_id] = order
                self.write_log(
                    '[send buy cover order] direction:{}, price:{},volume:{},vt_order_id:{},order_time:{},status:{}'
                        .format(order.direction, order.price, order.volume, order.vt_order_id, order.order_time,
                                order.status))

        for exchange, direction, price, volume in sell_order_req:
            ret_orders = self.send_order(self.symbol_pair, exchange, direction, Offset.CLOSE.value, price, volume)
            for vt_order_id, order in ret_orders:
                self.hb_sellOrderDict[vt_order_id] = order
                self.write_log(
                    '[send sell cover order] direction:{}, price:{},volume:{},vt_order_id:{},order_time:{},status:{}'
                        .format(order.direction, order.price, order.volume, order.vt_order_id, order.order_time,
                                order.status))

    def detect_trading_condition(self):
        """
        检测交易状态是否正常
        """
        pass

    def terminate(self):
        """
        终止程序
        """
        pass

    def on_merge_tick(self, merge_tick):
        self.write_log(
            "[on_merge_tick] merge_tick.bids[0][0]:{},self.trading:{}".format(merge_tick.bids[0][0], self.trading))
        """
        收到合并行情后的处理
        """
        if self.update_failed:
            self.write_log("contract data not right!")
            return

        # 过滤 0数据
        if merge_tick.bids[0][0] > 0:
            # 因为聚合行情，是多个交易所的数据， 所以这里需要 过滤掉不在回补市场中的交易数据
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

        else:
            return

        # 表示是正常交易状态
        if self.trading:
            if self.base_exchange_updated and self.target_exchange_updated:
                self.total_volume_min = max(self.total_volume_min,
                                            get_system_inside_min_volume(self.symbol_pair, self.base_bid[0][0],
                                                                         self.base_bid[0][2]))

                # 检测当前各个交易所账户，资金是否足够
                flag = self.check_is_account_needed_enough()

                # 撤销时间太长的订单 
                self.cancel_not_cover_order()
                self.cancel_time_too_long_order()
                # 撤掉不盈利的单子 
                self.cancel_not_profit_orders()

                if self.working_transfer_request:
                    # 转账时就别挂单了。。
                    self.process_transfer()
                else:
                    # 发出挂单
                    self.put_orders()

                # 在其他市场回补
                self.cover_orders()

                # 检测程序运行状态
                self.detect_trading_condition()

        else:
            # 表示进入异常情况
            self.terminate()

    def on_tick(self, tick):
        """
        收到目标市场 行情时的处理
        """
        if self.update_failed:
            self.write_log("contract data not right!")
            return

        self.target_exchange_updated = True
        # 过滤 0数据

        if tick.bid_prices[0] > 0:
            self.target_buy_prices = copy(tick.bid_prices)
            self.target_buy_volumes = copy(tick.bid_volumes)
            self.target_ask_prices = copy(tick.ask_prices)
            self.target_ask_volumes = copy(tick.ask_volumes)

        else:
            return

    def on_order(self, order):
        """
        Callback of new order data update.
        """
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.is_active():
            if not self.trading:
                self.write_log(
                    "[not trading] now is not in trading condition, cancel order:{}".format(order.vt_order_id))
                self.cancel_order(order.vt_order_id)
            else:
                if order.vt_order_id not in self.get_live_order_ids():
                    self.write_log(
                        "[not in live ids] vt_order_id:{} is not in living ids, cancel it!".format(order.vt_order_id))
                    self.cancel_order(order.vt_order_id)

        if order.exchange in self.base_exchange_info.keys():
            self.output_important_log()
        elif order.traded > 1e-12:
            self.output_important_log()

        if order.traded > 0 and not order.is_active():
            self.write_important_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
                order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume,
                order.traded))

        # 提交的订单推送，过滤掉
        if order.status == Status.SUBMITTING.value:
            return

        if order.exchange == self.target_exchange_info["exchange_name"]:
            if order.direction == Direction.LONG.value:
                # 先判断是否是拒单
                if order.status == Status.REJECTED.value:
                    if order.vt_order_id in self.zs_buyOrderDict.keys():
                        del self.zs_buyOrderDict[order.vt_order_id]
                    elif order.vt_order_id in self.dirty_order_dict.keys():
                        del self.dirty_order_dict[order.vt_order_id]
                else:
                    bef_order = self.zs_buyOrderDict.get(order.vt_order_id, None)
                    if bef_order:
                        new_traded = order.traded - bef_order.traded

                        if new_traded > 0:
                            self.need_cover_sell_volume += new_traded
                            self.target_exchange_info["pos_target_symbol"] += new_traded * (1 - self.fee_rate / 100.0)
                            self.target_exchange_info["pos_base_symbol"] -= order.price * new_traded

                        self.zs_buyOrderDict[order.vt_order_id] = copy(order)
                        if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value]:
                            del self.zs_buyOrderDict[order.vt_order_id]

                        if new_traded > 0:
                            self.cover_orders()

                    if not bef_order:
                        bef_order = self.dirty_order_dict.get(order.vt_order_id, None)
                        if bef_order:
                            new_traded = order.traded - bef_order.traded

                            if new_traded:
                                self.need_cover_sell_volume += new_traded
                                self.target_exchange_info["pos_target_symbol"] += new_traded * (1 - self.fee_rate / 100.0)
                                self.target_exchange_info["pos_base_symbol"] -= order.price * new_traded

                            self.dirty_order_dict[order.vt_order_id] = copy(order)
                            if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value]:
                                del self.dirty_order_dict[order.vt_order_id]

                            if new_traded > 0:
                                self.cover_orders()

            else:
                # 先判断是否是拒单
                if order.status == Status.REJECTED.value:
                    if order.vt_order_id in self.zs_sellOrderDict.keys():
                        del self.zs_sellOrderDict[order.vt_order_id]
                else:
                    bef_order = self.zs_sellOrderDict.get(order.vt_order_id, None)
                    if bef_order:
                        new_traded = order.traded - bef_order.traded

                        if new_traded > 0:
                            self.need_cover_buy_volume += new_traded
                            self.target_exchange_info["pos_target_symbol"] -= new_traded
                            self.target_exchange_info["pos_base_symbol"] += new_traded * order.price * (1 - self.fee_rate / 100.0)

                            self.zs_sellOrderDict[order.vt_order_id] = copy(order)
                        if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value]:
                            del self.zs_sellOrderDict[order.vt_order_id]

                        if new_traded > 0:
                            self.cover_orders()

                    if not bef_order:
                        bef_order = self.dirty_order_dict.get(order.vt_order_id, None)
                        if bef_order:
                            new_traded = order.traded - bef_order.traded

                            if new_traded > 0:
                                self.need_cover_buy_volume += new_traded
                                self.target_exchange_info["pos_target_symbol"] -= new_traded
                                self.target_exchange_info["pos_base_symbol"] += new_traded * order.price * (1 - self.fee_rate / 100.0)

                                self.dirty_order_dict[order.vt_order_id] = copy(order)

                            if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value]:
                                del self.dirty_order_dict[order.vt_order_id]

                            if new_traded > 0:
                                self.cover_orders()

        elif order.exchange in self.base_exchange_info.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.hb_buyOrderDict.get(order.vt_order_id, None)
                if bef_order is not None:
                    if order.status == Status.REJECTED.value:
                        del self.hb_buyOrderDict[order.vt_order_id]
                        self.frozen_sent_buy_volume -= order.volume - order.traded

                        dic = self.base_exchange_info.get(order.exchange, {})
                        if dic:
                            dic["frozen_sent_buy_volume"] -= order.volume - order.traded
                    else:
                        new_traded = order.traded - bef_order.traded
                        if new_traded > 0:
                            self.need_cover_buy_volume -= new_traded
                            self.frozen_sent_buy_volume -= new_traded
                            self.base_exchange_info[order.exchange]["pos_base_symbol"] -= new_traded * order.price
                            self.base_exchange_info[order.exchange]["pos_target_symbol"] += new_traded * (1 - self.fee_rate / 100.0)
                            self.base_exchange_info[order.exchange]["frozen_sent_buy_volume"] -= new_traded

                            self.hb_buyOrderDict[order.vt_order_id] = copy(order)

                        if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value]:
                            self.frozen_sent_buy_volume -= order.volume - order.traded
                            dic = self.base_exchange_info.get(order.exchange, {})
                            if dic:
                                dic["frozen_sent_buy_volume"] -= order.volume - order.traded

                            del self.hb_buyOrderDict[order.vt_order_id]

            else:
                bef_order = self.hb_sellOrderDict.get(order.vt_order_id, None)
                if bef_order is not None:
                    if order.status == Status.REJECTED.value:
                        del self.hb_sellOrderDict[order.vt_order_id]
                        self.frozen_sent_sell_volume -= order.volume - order.traded

                        dic = self.base_exchange_info.get(order.exchange, {})
                        if dic:
                            dic["frozen_sent_sell_volume"] -= order.volume - order.traded
                    else:
                        new_traded = order.traded - bef_order.traded
                        if new_traded > 0:
                            self.need_cover_sell_volume -= new_traded
                            self.frozen_sent_sell_volume -= new_traded
                            self.base_exchange_info[order.exchange][
                                "pos_base_symbol"] += order.price * new_traded * (1 - self.fee_rate / 100.0)
                            self.base_exchange_info[order.exchange]["pos_target_symbol"] -= new_traded
                            self.base_exchange_info[order.exchange]["frozen_sent_sell_volume"] -= new_traded

                            self.hb_sellOrderDict[order.vt_order_id] = copy(order)

                        if order.status in [Status.ALLTRADED.value, Status.CANCELLED.value]:
                            self.frozen_sent_sell_volume -= order.volume - order.traded
                            dic = self.base_exchange_info.get(order.exchange, {})
                            if dic:
                                dic["frozen_sent_sell_volume"] -= order.volume - order.traded
                            del self.hb_sellOrderDict[order.vt_order_id]

        else:
            self.write_log("on_order order exchange not found! vt_order_id:{} ".format(order.vt_order_id))

        if order.exchange in self.base_exchange_info.keys():
            self.output_important_log()
            self.write_log("[on_order end info]")
        elif order.traded > 1e-12:
            self.output_important_log()
            self.write_log("[on_order end info]")

        if order.traded > 0:
            self.update_account()

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
        """
        :param transfer_req:
        :return:
        """
        msg = "[process_transfer_event] :{}".format(transfer_req.__dict__)
        self.write_important_log(msg)

        if self.working_transfer_request:
            msg = "[process_transfer_event] already has transfer req! drop it!"
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
                msg = "[process_transfer] before drop working_transfer request for time exceed! a:{},b:{}".format(now,
                                                             self.working_transfer_request.timestamp)
                self.write_important_log(msg)
                self.working_transfer_request = None
                return

        if self.working_transfer_request.asset_id == self.target_symbol:
            if self.target_exchange_info["pos_target_symbol"] >= self.working_transfer_request.transfer_amount:
                already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
                    self.zs_sellOrderDict)

                if self.target_exchange_info[
                    "pos_target_symbol"] >= self.working_transfer_request.transfer_amount + already_cover_target_volume:
                    transfer_id = self.go_transfer()
                    if transfer_id:
                        self.target_exchange_info["pos_target_symbol"] -= self.working_transfer_request.transfer_amount
                        self.working_transfer_request = None
                    else:
                        now = time.time()
                        if now - self.working_transfer_request.timestamp > 20:
                            msg = "[process_transfer] send drop working_transfer request for time exceed! a:{},b:{}".format(
                                now, self.working_transfer_request.timestamp)
                            self.write_important_log(msg)
                            self.working_transfer_request = None
                else:
                    need_cancel_sets = set([])
                    for vt_order_id, order in self.zs_sellOrderDict.items():
                        need_cancel_sets.add(order.vt_order_id)
                    self.cancel_sets_order(need_cancel_sets)
            else:
                msg = "[process_transfer] target_symbol inside_pos:{} not enough!".format(
                    self.target_exchange_info["pos_target_symbol"])
                self.working_transfer_request = None
                self.write_important_log(msg)

        elif self.working_transfer_request.asset_id == self.base_symbol:
            if self.target_exchange_info["pos_base_symbol"] >= self.working_transfer_request.transfer_amount:
                already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
                    self.zs_buyOrderDict)
                if self.target_exchange_info[
                    "pos_base_symbol"] >= self.working_transfer_request.transfer_amount + already_cover_base_volume:
                    transfer_id = self.go_transfer()
                    if transfer_id:
                        self.target_exchange_info["pos_base_symbol"] -= self.working_transfer_request.transfer_amount
                        self.working_transfer_request = None
                    else:
                        now = time.time()
                        if now - self.working_transfer_request.timestamp > 20:
                            msg = "[process_transfer] send drop working_transfer request for time exceed! a:{},b:{}".format(
                                now, self.working_transfer_request.timestamp)
                            self.write_important_log(msg)
                            self.working_transfer_request = None
                else:
                    need_cancel_sets = set([])
                    for vt_order_id, order in self.zs_buyOrderDict.items():
                        need_cancel_sets.add(order.vt_order_id)
                    self.cancel_sets_order(need_cancel_sets)
            else:
                msg = "[process_transfer] base_symbol inside_pos:{} not enough!".format(
                    self.target_exchange_info["pos_base_symbol"])
                self.working_transfer_request = None
                self.write_important_log(msg)

    def output_important_log(self):
        self.write_log(
            '[inside need] need_cover_buy_volume:{},need_cover_sell_volume:{}'.format(self.need_cover_buy_volume,
                                                                                      self.need_cover_sell_volume))
        self.write_log('[inside frozen_sent] frozen_sent_buy_volume:{},frozen_sent_sell_volume:{}'.format(
            self.frozen_sent_buy_volume, self.frozen_sent_sell_volume))

        self.write_log(
            '[target] pos_target_symbol:{},pos_base_symbol:{}'.format(
                self.target_exchange_info["pos_target_symbol"], self.target_exchange_info["pos_base_symbol"]))

        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]
            self.write_log(
                '[base] [{}] frozen_target_symbol:{}, frozen_base_symbol:{},pos_target_symbol:{},pos_base_symbol:{}'.format(
                    exchange, dic["frozen_target_symbol"], dic["frozen_base_symbol"],
                    dic["pos_target_symbol"], dic["pos_base_symbol"]))
