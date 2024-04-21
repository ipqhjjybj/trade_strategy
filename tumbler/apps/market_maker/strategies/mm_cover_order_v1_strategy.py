# coding=utf-8
import time
from copy import copy
from collections import defaultdict

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.apps.data_third_part.base import get_diff_type_exchange_name
from tumbler.function import get_vt_key, get_two_currency
from tumbler.constant import Direction, Offset, Status
from tumbler.object import BBOTickData, OrderData, CoverOrderRequest


class MarketMakerSpotCoverV1(MarketMakerTemplate):
    author = "ipqhjjybj"
    class_name = "MarketMakerSpotCoverV1"

    """
    默认参数
    """
    symbol_pair = "btc_usdt"
    target_symbol = "btc"
    base_symbol = "usdt"
    symbol_pair_list = []
    base_exchange_info = {}
    cover_inc_rate = 0.4

    retry_cancel_send_num = 3

    parameters = [
        "strategy_name",  # 策略加载的唯一性名字
        "class_name",  # 类的名字
        'vt_symbols_subscribe',  # 订阅的数据类型
        "vt_account_name_subscribe",  # 订阅的账户 account
        "author",  # 作者
        "symbol_pair_list",  # 交易对
        "base_exchange_info"  # 交易所信息
    ]

    # 需要保存的运行时变量
    variables = [
        'inited',
        'trading'
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MarketMakerSpotCoverV1, self).__init__(mm_engine, strategy_name, settings)

        self.hb_order_dict = {}
        self.price_info = {}

        self.need_work_volume_compute = {}

        self.cancel_dict_times_count = {}

        self.flag_update_exchange = False

    def update_exchange_info(self):
        target_vt_symbol = get_vt_key(self.symbol_pair, self.base_exchange_info["exchange_name"])
        if self.update_single_exchange_info(self.base_exchange_info, target_vt_symbol):
            self.flag_update_exchange = True

    def update_account(self):
        for symbol_pair in self.symbol_pair_list:
            target_symbol, base_symbol = get_two_currency(symbol_pair)

            key_acct_te_target_symbol = get_vt_key(self.base_exchange_info["exchange_name"], target_symbol)
            key_acct_te_base_symbol = get_vt_key(self.base_exchange_info["exchange_name"], base_symbol)

            acct_te_target = self.get_account(key_acct_te_target_symbol)
            acct_te_base = self.get_account(key_acct_te_base_symbol)

            self.need_work_volume_compute[symbol_pair] = {
                Direction.LONG.value: {
                    "need_cover_target_volume": 0
                },
                Direction.SHORT.value: {
                    "need_cover_target_volume": 0
                }
            }

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def get_live_order_ids(self):
        return list(self.hb_order_dict.keys()) + list(self.hb_order_dict.keys())

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > self.retry_cancel_send_num:
                if vt_order_id in self.hb_order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    # order = self.hb_order_dict[vt_order_id]
                    del self.hb_order_dict[vt_order_id]

                    self.write_log('[already set cancel] vt_order_id:{}'.format(vt_order_id))

    def cover_orders(self):
        order_req = []
        for symbol, detail_work_info_dic in self.need_work_volume_compute.items():
            for direction, work_dic in detail_work_info_dic.items():
                work_volume = work_dic["need_cover_target_volume"]
                if work_volume > 100:
                    if direction == Direction.LONG.value:
                        price = self.price_info[symbol]["ask"][0] * (1 + self.cover_inc_rate / 100.0)
                    else:
                        price = self.price_info[symbol]["bid"][0] * (1 - self.cover_inc_rate / 100.0)
                    order_req.append((direction, price, work_volume))
                    work_dic["need_cover_target_volume"] -= work_volume

        for direction, price, work_volume in order_req:
            ret_orders = self.send_order(self.symbol_pair, self.base_exchange_info["exchange_name"], direction,
                                         Offset.OPEN.value, price, work_volume)
            for vt_order_id, order in ret_orders:
                self.hb_order_dict[vt_order_id] = copy(order)
                self.write_log(
                    '[send buy cover order] direction:{}, price:{},volume:{},vt_order_id:{},order_time:{},status:{}'
                        .format(order.direction, order.price, order.volume, order.vt_order_id, order.order_time,
                                order.status))

    def on_bbo_tick(self, tick: BBOTickData):
        #self.write_log("[on_bbo_tick] tick:{}".format(tick.__dict__))
        self.price_info = copy(tick.symbol_dic)
        self.cover_orders()

    def on_cover_order_request(self, cover_req: CoverOrderRequest):
        self.write_log("on_cover_order_request req:{}".format(cover_req.__dict__))
        dic = self.need_work_volume_compute.get(cover_req.symbol, {})
        if dic:
            dic[cover_req.direction]["need_cover_target_volume"] += float(cover_req.volume)

        self.cover_orders()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        if order.vt_order_id in self.hb_order_dict.keys():
            bef_order = self.hb_order_dict[order.vt_order_id]
            if order.traded >= bef_order.traded:
                self.hb_order_dict[order.vt_order_id] = copy(order)

                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    dic = self.need_work_volume_compute.get(order.symbol, {})
                    if dic:
                        dic[order.direction]["need_cover_target_volume"] -= new_traded
            if not order.is_active():
                if order.status == Status.REJECTED.value:
                    self.send_reject_cover_order_req(copy(order.make_reject_cover_order_req()))
                self.hb_order_dict.pop(order.vt_order_id)

    def on_trade(self, trade):
        pass

    def output_important_log(self):
        self.write_log("need_work_volume_compute:{}".format(self.need_work_volume_compute))


