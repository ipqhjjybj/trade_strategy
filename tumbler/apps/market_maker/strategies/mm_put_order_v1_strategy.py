# coding=utf-8
import time
from copy import copy
from collections import defaultdict

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.apps.data_third_part.base import get_diff_type_exchange_name
from tumbler.constant import MQSubscribeType
from tumbler.function import get_vt_key, get_round_order_price, get_from_vt_key
from tumbler.constant import Direction, Offset, Exchange, TradeType, MQCommonInfo, Status
from tumbler.object import MergeTickData, TickData, TradeData, DictAccountData
from tumbler.function import is_price_volume_too_small


class MarketMakerSpotPutOrderV1(MarketMakerTemplate):
    author = "ipqhjjybj"
    class_name = "MarketMakerSpotPutOrderV1"

    """
    默认参数
    """
    symbol_pair = "btc_usdt"
    target_symbol = "btc"
    base_symbol = "usdt"
    target_exchange_info = {}
    base_exchange_info = {}
    profit_spread = 0.5
    base_spread = 0.6
    put_order_num = 1
    inc_spread = 0
    fee_rate = 0.1

    retry_cancel_send_num = 1

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'vt_symbols_subscribe',  # 订阅的数据类型
        "vt_account_name_subscribe",  # 订阅的账户 account
        'vt_transfer_assets',  # 需要加载的转移资产
        'class_name',  # 类的名字
        'author',  # 作者
        'symbol_pair',  # 交易对 如 btc_udst
        'target_symbol',  # btc
        'base_symbol',  # usdt
        'target_exchange_info',  # 目标交易所信息
        'base_exchange_info',  # 回补交易所信息
        'profit_spread',  # 能盈利的价差
        'base_spread',  # 挂单相比较对冲市场的基础价格
        'inc_spread',  # 挂单差值迭代 , 如果 inc_spread 是, 那么就表示1个price_tick的增加
        'put_order_num',  # 单侧挂单的挂单数量
        'fee_rate'  # 交易所手续费
    ]

    variables = [
        'inited',
        'trading'
    ]

    syncList = [
        'target_exchange_info',
        'base_exchange_info'
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MarketMakerSpotPutOrderV1, self).__init__(mm_engine, strategy_name, settings)

        self.target_bids = []
        self.target_asks = []

        self.base_bids = []
        self.base_asks = []

        self.zs_buy_order_dict = {}  # 维护的挂单买单
        self.zs_sell_order_dict = {}  # 维护的挂单卖单
        self.dirty_order_dict = {}  # 已经被强制准备删除的订单

        self.cancel_dict_times_count = defaultdict(int)  # 对一个 vt_order_id 的撤单次数

        self.cover_asset_dict = defaultdict(float)

        self.flag_update_account = False  # 账户资金是否初始化完
        self.flag_update_exchange = False  # 交易所是否初始化完毕
        self.flag_update_target_tick = False  # 是否更新target的tick
        self.flag_update_base_tick = False  # 是否更新完base tick

        self.mov_xishu_buy = 1  # 用于处理 mov utxo问题
        self.mov_xishu_sell = 1  # 用于处理 mov utxo问题

        self.trade_id = int(time.time() * 1000)

        self.target_exchange_info["pos_base_symbol"] = 0
        self.target_exchange_info["pos_target_symbol"] = 0

    def update_exchange_info(self):
        target_vt_symbol = get_vt_key(self.symbol_pair, self.target_exchange_info["exchange_name"])
        if self.update_single_exchange_info(self.target_exchange_info, target_vt_symbol):
            self.flag_update_exchange = True

    def update_account(self):
        pass
        # key_acct_te_target_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.target_symbol)
        # key_acct_te_base_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.base_symbol)
        #
        # acct_te_target = self.get_account(key_acct_te_target_symbol)
        # acct_te_base = self.get_account(key_acct_te_base_symbol)
        #
        # if acct_te_target and acct_te_base:
        #     self.target_exchange_info["pos_target_symbol"] = acct_te_target.available * self.target_exchange_info[
        #         "percent_use"] / 100.0
        #
        #     self.target_exchange_info["pos_base_symbol"] = acct_te_base.available * self.target_exchange_info[
        #         "percent_use"] / 100.0
        # else:
        #     self.write_log("[update_account] acct_te_target:{} self.acct_te_base:{}".
        #                    format(acct_te_target, acct_te_base))
        #     return
        # if not self.flag_update_account:
        #     self.flag_update_account = True
        #
        # self.write_log("[update_account] target_exchange_info:{}".format(self.target_exchange_info))
        # self.write_log("[update_account] base_exchange_info:{}".format(self.base_exchange_info))

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def get_live_order_ids(self):
        return list(self.zs_buy_order_dict.keys()) + list(self.zs_sell_order_dict.keys()) + list(
            self.dirty_order_dict.keys())

    def get_inc_price(self, price):
        if self.inc_spread == 0:
            return self.target_exchange_info["price_tick"]
        else:
            return self.inc_spread * price / 100.0

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.zs_buy_order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.zs_buy_order_dict[vt_order_id]
                    del self.zs_buy_order_dict[vt_order_id]

                    self.dirty_order_dict[vt_order_id] = copy(order)
                if vt_order_id in self.zs_sell_order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.zs_sell_order_dict[vt_order_id]
                    del self.zs_sell_order_dict[vt_order_id]
                    self.dirty_order_dict[vt_order_id] = copy(order)

    def cancel_not_profit_orders(self):
        """
        撤掉那些不盈利的单子
        """
        # 对于买单
        need_cancel_sets = set([])
        for vt_order_id, order in self.zs_buy_order_dict.items():
            if order.status != Status.SUBMITTING.value:
                if order.price >= self.base_bids[0][0] * (1 - self.profit_spread / 100.0):
                    self.write_log("[not profit price] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

                can_make_cover_volume = 0
                for i in range(len(self.base_bids)):
                    tt_price, tt_volume, tt_exchange = self.base_bids[i]
                    if tt_price > 0 and tt_price * (1 - self.profit_spread / 100.0) > order.price:
                        can_make_cover_volume += tt_volume

                if order.volume - order.traded >= can_make_cover_volume:
                    self.write_log("[lack cover volume] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

        # 对于卖单
        for vt_order_id, order in self.zs_sell_order_dict.items():
            if order.status != Status.SUBMITTING.value:
                if order.price <= self.base_asks[0][0] * (1 + self.profit_spread / 100.0):
                    self.write_log("[not profit price] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

                can_make_cover_volume = 0
                for i in range(len(self.base_asks)):
                    tt_price, tt_volume, tt_exchange = self.base_asks[i]
                    if tt_price > 0 and tt_price * (1 + self.profit_spread / 100.0) < order.price:
                        can_make_cover_volume += tt_volume

                if order.volume - order.traded >= can_make_cover_volume:
                    self.write_log("[lack cover volume] now go to cancel :{}".format(order.vt_order_id))
                    need_cancel_sets.add(order.vt_order_id)
                    continue

        self.cancel_sets_order(need_cancel_sets)

    def cancel_time_too_long_order(self):
        """
        撤销掉那些 时间很长的挂单 ,因为可能是
        """
        # 对于买单
        now = time.time()
        need_cancel_sets = set([])
        for vt_order_id, order in self.zs_buy_order_dict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 超过180秒的订单撤掉重发 ,测试期间10秒
            if now - order_time > 300 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                self.write_log(
                    "[cancel too long time],zs_buy order_id:{} now:{},order_time:{}".format(order.vt_order_id, now,
                                                                                            order_time))
                need_cancel_sets.add(order.vt_order_id)

        for vt_order_id, order in self.zs_sell_order_dict.items():
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

    def get_sum_cover_base_volume(self):
        # return self.cover_asset_dict[self.base_symbol]
        self.write_log("[get_sum_cover_base_volume] :{}".format(self.cover_asset_dict[self.base_symbol]))
        return 100000000

    def get_sum_cover_target_volume(self):
        # return self.cover_asset_dict[self.target_symbol]
        self.write_log("[get_sum_cover_target_volume] :{}".format(self.cover_asset_dict[self.target_symbol]))
        return 1000000000

    def get_need_cover_sell(self):
        return 0

    def get_need_cover_buy(self):
        return 0

    def get_max_buy_order_price(self):
        _, _, already_price_volumes = self.get_make_cover_volume(self.zs_buy_order_dict)
        already_price_volumes.sort(reverse=True)
        if len(already_price_volumes) > 0:
            return already_price_volumes[0][0]
        else:
            return 0

    def get_min_sell_order_price(self):
        _, _, already_price_volumes = self.get_make_cover_volume(self.zs_sell_order_dict)
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
            self.zs_buy_order_dict)
        already_price_volumes.sort(reverse=True)
        n_buy_nums = len(already_price_volumes)
        left_buy_num = self.put_order_num - n_buy_nums

        tmp_ava_base_volume = self.target_exchange_info["pos_base_symbol"] - already_cover_base_volume
        sum_cover_target_volume = self.get_sum_cover_target_volume()
        tmp_ava_base_volume = min(tmp_ava_base_volume, sum_cover_target_volume - already_cover_target_volume
                                  - self.get_need_cover_sell() * self.base_bids[0][0])

        if tmp_ava_base_volume > self.target_exchange_info["min_volume"] * self.base_bids[0][0] * left_buy_num:
            if left_buy_num > 0:
                tmp_ava_base_volume /= left_buy_num

                if self.target_exchange_info["exchange_name"] in [Exchange.MOV.value]:
                    tmp_ava_base_volume /= self.mov_xishu_buy

        bef_sum_send_order_volume = 0
        accu_volume = 0
        start_buy_price = self.base_bids[0][0] * (1 - self.base_spread / 100.0)

        run_count = 0
        new_req_buy_orders = []  # [ (Direction.value.buy), price, volume]

        while left_buy_num > 0 and tmp_ava_base_volume > self.target_exchange_info[
            "min_volume"] * start_buy_price * 1.05:
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
            for i in range(len(self.base_bids)):
                tt_price, tt_volume, tt_exchange = self.base_bids[i]

                if tt_price > 0 and tt_exchange != self.target_exchange_info["exchange_name"]:
                    if tt_price * (1 - self.profit_spread / 100.0) > start_buy_price:
                        can_make_cover_volume += tt_volume

            can_make_cover_volume = can_make_cover_volume - accu_volume - bef_sum_send_order_volume
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
            if start_buy_price > self.base_bids[0][0] * (1 - self.profit_spread / 100.0):
                start_buy_price -= self.target_exchange_info["price_tick"]

            # 发单需要大于最小发单量
            if can_make_cover_volume > self.target_exchange_info["min_volume"]:
                # 价格跟数量太小了， 这时候不需要继续 step计算下去了
                if is_price_volume_too_small(self.symbol_pair, start_buy_price, can_make_cover_volume):
                    msg = "[buy check] is_price_volume_too_small,symbol_pair:{},price:{},volume:{}".format(
                        self.symbol_pair, start_buy_price, can_make_cover_volume)
                    break

                min_ask_order_price = self.get_min_sell_order_price()
                use_price = min(start_buy_price, self.target_asks[0][0])
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

        ###########################
        # 下卖单
        already_cover_target_volume, already_cover_base_volume, already_price_volumes = self.get_make_cover_volume(
            self.zs_sell_order_dict)
        already_price_volumes.sort()
        n_sell_nums = len(already_price_volumes)
        left_sell_num = self.put_order_num - n_sell_nums

        tmp_ava_target_volume = self.target_exchange_info["pos_target_symbol"] - already_cover_target_volume

        sum_cover_base_volume = self.get_sum_cover_base_volume()
        tmp_ava_target_volume = min(tmp_ava_target_volume, (sum_cover_base_volume - already_cover_base_volume)
                                    / self.base_bids[0][0] - self.get_need_cover_sell())

        if tmp_ava_target_volume > self.target_exchange_info["min_volume"] * left_sell_num:
            if left_sell_num > 0:
                tmp_ava_target_volume /= left_sell_num

                if self.target_exchange_info["exchange_name"] == Exchange.MOV.value:
                    tmp_ava_target_volume /= self.mov_xishu_sell

        bef_sum_send_order_volume = 0
        accu_volume = 0
        start_sell_price = self.base_asks[0][0] * (1 + self.base_spread / 100.0)

        # self.write_log("left_sell_num:{} tmp_ava_target_volume:{}".format(left_sell_num, tmp_ava_target_volume))
        run_count = 0
        new_req_sell_orders = []  # [ (Direction.value.sell), price, volume]
        while left_sell_num > 0 and tmp_ava_target_volume > self.target_exchange_info["min_volume"]:
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
            for i in range(len(self.base_asks)):
                tt_price, tt_volume, tt_exchange = self.base_asks[i]
                if tt_price > 0 and tt_exchange != self.target_exchange_info["exchange_name"]:
                    if tt_price * (1 + self.profit_spread / 100.0) < start_sell_price:
                        can_make_cover_volume += tt_volume

            can_make_cover_volume = can_make_cover_volume / 2.0 - accu_volume - bef_sum_send_order_volume

            # self.write_log("can_make_cover_volume:{}".format(can_make_cover_volume))
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
            if start_sell_price < self.base_asks[0][0] * (1 + self.profit_spread / 100.0):
                start_sell_price += self.target_exchange_info["price_tick"]

            # self.write_log("start_sell_price:{}".format(start_sell_price))
            # 发单需要大于最小发单量
            if can_make_cover_volume > self.target_exchange_info["min_volume"]:
                max_order_buy_price = self.get_max_buy_order_price()
                use_price = max(self.target_bids[0][0], start_sell_price)
                use_price = max(use_price, max_order_buy_price + self.target_exchange_info["price_tick"])
                new_req_sell_orders.append((Direction.SHORT.value, use_price, can_make_cover_volume))

                bef_sum_send_order_volume += can_make_cover_volume
                left_sell_num = left_sell_num - 1
                tmp_ava_target_volume = tmp_ava_target_volume - can_make_cover_volume

            start_sell_price = start_sell_price + self.get_inc_price(start_sell_price)
            run_count += 1
            if run_count > self.put_order_num * 3:
                break

        ##########################################
        # self.write_log("[put_orders] start_sell_price:{}".format(start_sell_price))
        # 进行发单
        # 买单
        has_error_buy_flag = False
        for (d, price, volume) in new_req_buy_orders:
            ret_orders = self.send_order(self.symbol_pair, self.target_exchange_info["exchange_name"],
                                         Direction.LONG.value, Offset.OPEN.value, price, volume)

            for vt_order_id, order in ret_orders:
                if vt_order_id is not None and order is not None and order.is_active():
                    self.zs_buy_order_dict[vt_order_id] = order

                elif vt_order_id is None:
                    has_error_buy_flag = True

        if has_error_buy_flag:
            self.mov_xishu_buy *= 2
        else:
            self.mov_xishu_buy = 1

        has_error_sell_flag = False
        # 卖单
        for (d, price, volume) in new_req_sell_orders:
            ret_orders = self.send_order(self.symbol_pair, self.target_exchange_info["exchange_name"],
                                         Direction.SHORT.value, Offset.CLOSE.value, price, volume)
            for vt_order_id, order in ret_orders:
                if vt_order_id is not None and order is not None and order.is_active():
                    self.zs_sell_order_dict[vt_order_id] = order
                elif vt_order_id is None:
                    has_error_sell_flag = True

        if has_error_sell_flag:
            self.mov_xishu_sell *= 2
        else:
            self.mov_xishu_sell = 1

    def on_merge_tick(self, merge_tick: MergeTickData):
        # self.write_log("[on_merge_tick] :{}".format(merge_tick.__dict__))
        if merge_tick.bids[0][0] > 0:
            self.base_bids, self.base_asks = merge_tick.get_depth()
            self.flag_update_base_tick = True
        else:
            return

        if self.trading:
            if not self.flag_update_exchange:
                self.update_exchange_info()
                return

            if self.flag_update_base_tick and self.flag_update_target_tick:
                self.cancel_time_too_long_order()
                self.cancel_not_profit_orders()
                self.put_orders()

    def on_tick(self, tick: TickData):
        super(MarketMakerSpotPutOrderV1, self).on_tick(tick)

        if tick.bid_prices[0] > 0:
            self.target_bids, self.target_asks = tick.get_depth()
            self.flag_update_target_tick = True

    def update_order_trade(self, order, new_traded):
        if order.direction == Direction.LONG.value:
            self.target_exchange_info["pos_target_symbol"] += new_traded * (1 - self.fee_rate / 100.0)
            self.target_exchange_info["pos_base_symbol"] -= order.price * new_traded
        else:
            self.target_exchange_info["pos_target_symbol"] -= new_traded
            self.target_exchange_info["pos_base_symbol"] += new_traded * order.price * (1 - self.fee_rate / 100.0)

        self.trade_id += 1
        trade = order.make_trade_data(self.trade_id, new_traded, TradeType.PUT_ORDER.value)
        self.send_put_trades(trade)

    def cancel_order_dict(self, dic: dict):
        need_cancel_sets = set([])
        for vt_order_id, order in dic.items():
            need_cancel_sets.add(order.vt_order_id)
        self.cancel_sets_order(need_cancel_sets)

    def has_prepare_transfer(self, req):
        flag = True
        if req.asset_id == self.target_symbol:
            if self.target_exchange_info["pos_target_symbol"] < self.working_transfer_request.transfer_amount:
                self.cancel_order_dict(self.zs_sell_order_dict)
                flag = False
        elif req.asset_id == self.base_symbol:
            if self.target_exchange_info["pos_base_symbol"] < self.working_transfer_request.transfer_amount:
                self.cancel_order_dict(self.zs_buy_order_dict)
                flag = False
        return flag

    def after_transfer_asset(self, req):
        if req.asset_id == self.target_symbol:
            self.target_exchange_info["pos_target_symbol"] -= req.transfer_amount
        elif req.asset_id == self.base_symbol:
            self.target_exchange_info["pos_base_symbol"] -= req.transfer_amount
        else:
            self.write_log("[has transfer other symbols] req:{}".format(req.__dict__))

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
                    return

        if order.exchange == self.target_exchange_info["exchange_name"]:
            if order.direction == Direction.LONG.value:
                bef_order = self.update_order(self.zs_buy_order_dict, order, self.update_order_trade)
            else:
                bef_order = self.update_order(self.zs_sell_order_dict, order, self.update_order_trade)

            if not bef_order:
                bef_order = self.update_order(self.dirty_order_dict, order, self.update_order_trade)
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

    # def on_account(self, acct: AccountData):
    #     """
    #     传递账户信息
    #     :param acct:
    #     :return:
    #     """
    #     self.write_log("on_account acct:{}".format(acct.__dict__))
    #     asset, exchange = get_from_vt_key(acct.vt_account_id)
    #     if exchange == self.base_exchange_info["exchange_name"]:
    #         self.cover_asset_dict[asset] = acct.balance
    #     elif exchange == self.target_exchange_info["exchange_name"]:
    #         if asset == self.target_symbol:
    #             self.target_exchange_info["pos_target_symbol"] = acct.balance
    #         elif asset == self.base_symbol:
    #             self.target_exchange_info["pos_base_symbol"] = acct.balance
    #     else:
    #         self.write_log("maybe error:{}".format(acct.__dict__))

    def on_dict_account(self, dict_acct: DictAccountData):
        # self.write_log("on_dict_account:{}".format(dict_acct.__dict__))
        if dict_acct.account_name == MQCommonInfo.COVER_ALL_ACCOUNT.value:
            for asset, dic in dict_acct.account_dict.items():
                self.cover_asset_dict[asset] = dic["balance"]
        elif dict_acct.account_name == self.target_exchange_info["account_name"]:
            for vt_account_id, dic in dict_acct.account_dict.items():
                asset, exchange = get_from_vt_key(vt_account_id)

                if asset == self.target_symbol:
                    self.target_exchange_info["pos_target_symbol"] = dic["balance"]
                    if self.target_exchange_info["exchange_name"] in [Exchange.MOV.value, Exchange.FLASH.value]:
                        already_cover_target_volume, _, _ = self.get_make_cover_volume(self.zs_sell_order_dict)
                        self.target_exchange_info["pos_target_symbol"] += already_cover_target_volume
                elif asset == self.base_symbol:
                    self.target_exchange_info["pos_base_symbol"] = dic["balance"]
                    if self.target_exchange_info["exchange_name"] in [Exchange.MOV.value, Exchange.FLASH.value]:
                        _, already_cover_base_volume, _ = self.get_make_cover_volume(self.zs_buy_order_dict)
                        self.target_exchange_info["pos_base_symbol"] += already_cover_base_volume

                self.write_log("target_exchange_info:{}".format(self.target_exchange_info))

        if not self.flag_update_account:
            self.flag_update_account = True
        #self.write_log("cover_asset_dict:{}".format(self.cover_asset_dict))

    def output_important_log(self):
        self.write_log(
            '[target] pos_target_symbol:{},pos_base_symbol:{}'.format(
                self.target_exchange_info["pos_target_symbol"], self.target_exchange_info["pos_base_symbol"]))

        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]
            self.write_log(
                '[base] [{}] frozen_target_symbol:{}, frozen_base_symbol:{},pos_target_symbol:{},pos_base_symbol:{}'.format(
                    exchange, dic["frozen_target_symbol"], dic["frozen_base_symbol"],
                    dic["pos_target_symbol"], dic["pos_base_symbol"]))
