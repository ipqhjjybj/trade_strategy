# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
import threading

from tumbler.apps.maker_0x.template import Maker0xTemplate
from tumbler.constant import EMPTY_STRING, EMPTY_INT, EMPTY_FLOAT, EMPTY_UNICODE
from tumbler.constant import MAX_PRICE_NUM, Exchange, Direction, Offset, Status
from tumbler.function import FilePrint
from tumbler.function import get_vt_key, get_round_order_price

from tumbler.service.log_service import log_service_manager

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


class TokenlonReq(object):
    def __init__(self):
        self.symbol = EMPTY_STRING
        self.uniqId = EMPTY_STRING
        self.quoteId = EMPTY_STRING
        self.amount = EMPTY_FLOAT
        self.direction = EMPTY_STRING
        self.req_time = time.time()

    def toString(self):
        return "req: symbol:{},uniqID:{},quoteId:{},amount:{},direction:{},req_time:{}".format(self.symbol, self.uniqId,
                                                                                               self.quoteId,
                                                                                               self.amount,
                                                                                               self.direction,
                                                                                               self.req_time)


class Maker0xV1Strategy(Maker0xTemplate):
    author = ""
    class_name = "Maker0xV1Strategy"
    symbol = "btc_usdt"

    """
    默认参数
    """
    symbol_pair = "btc_usdt"
    vt_symbols_subscribe = []
    target_symbol = "btc"
    base_symbol = "usdt"
    base_exchange_info = {}
    base_spread = 0.6

    frozen_sent_buy_volume = 0
    frozen_sent_sell_volume = 0
    need_lock_buy_volume = 0
    need_lock_sell_volume = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对 如 btc_udst
                  'vt_symbols_subscribe',  # 订阅的交易对，如 ["btc_usdt".GATEIO,"btc_usdt".AGGR]
                  'target_symbol',  # 如btc_usdt中的btc
                  'base_symbol',  # 如btc_usdt中的usdt
                  'base_exchange_info',  # 回补交易所信息
                  'base_spread',  # 挂单相比较对冲市场的基础价格
                  ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(Maker0xV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.update_account_flag = False
        self.update_exchange_flag = False
        self.recent_datetime = None
        self.base_bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据
        self.base_asks = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # 缓存的深度数据

        self.lock = threading.RLock()

        self.ret_buy = {"minAmount": 0, "maxAmount": 0.0, "price": 0}
        self.ret_sell = {"minAmount": 0, "maxAmount": 0.0, "price": 0}

        self.order_dict = {}
        self.req_dict = {}

        self.total_volume_min = 0

        self.need_cover_buy_volume = 0
        self.need_cover_sell_volume = 0
        self.need_lock_buy_volume = 0
        self.need_lock_sell_volume = 0

        self.uniqId_lock_money_req_dict = {}

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S")) * 10000
        self.order_count = 0

        self.cancel_dict_times_count = defaultdict(int)  # 对一个 vt_order_id 的撤单次数

        self.lock_file_print = FilePrint(self.strategy_name + ".log", "strategy_lock_run_log", mode="w")

    def update_exchange_info(self):
        self.update_exchange_flag = True
        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]
            base_vt_symbol = get_vt_key(self.symbol_pair, exchange)
            contract = self.get_contract(base_vt_symbol)
            if contract is None:
                update_msg = "base_contract:{} is not found!".format(base_vt_symbol)
                self.write_log(update_msg)
                return

            dic["price_tick"] = contract.price_tick
            dic["volume_tick"] = contract.volume_tick
            dic["min_volume"] = contract.min_volume
            self.total_volume_min = max(self.total_volume_min, contract.min_volume)

    def update_account(self):
        self.update_account_flag = True
        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]
            dic["pos_target_symbol"] = 0
            dic["pos_base_symbol"] = 0

            key_target = get_vt_key(exchange, self.target_symbol)
            key_base = get_vt_key(exchange, self.base_symbol)

            acct_be_target = self.get_account(key_target)
            acct_be_base = self.get_account(key_base)

            if acct_be_target is not None:
                dic["pos_target_symbol"] = acct_be_target.available * dic["target_symbol_percent_use"] / 100.0
            else:
                self.update_account_flag = False
                self.write_log(
                    "[update_account] acct_be_target is None, exchange:{},key_target:{}".format(exchange, key_target))

            if acct_be_base is not None:
                dic["pos_base_symbol"] = acct_be_base.available * dic["base_symbol_percent_use"] / 100.0
            else:
                self.update_account_flag = False
                self.write_log(
                    "[update_account] acct_be_base is None, exchange:{},key_target:{}".format(exchange, key_base))

        self.write_log("[update_account] base_exchange_info:{}".format(self.base_exchange_info))

    def write_lock_log_msg(self, msg):
        """
        Write a log message.
        with lock
        """
        self.lock.acquire()
        self.lock_file_print.write(
            '{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), self.strategy_name, msg))
        self.lock.release()

    def get_frozen_volume_from_dict(self, dic):
        self.lock.acquire()
        frozen_sent_buy_volume = 0
        frozen_sent_sell_volume = 0
        for vt_order_id, order in dic.items():
            if order.direction == Direction.LONG.value:
                frozen_sent_buy_volume += order.volume - order.traded
            else:
                frozen_sent_sell_volume += order.volume - order.traded
        self.lock.release()
        return frozen_sent_buy_volume, frozen_sent_sell_volume

    def generate_quote_id(self, symbol):
        self.lock.acquire()
        self.order_count += 1
        order_id = "{}_{}".format(symbol, str(self.connect_time + self.order_count))
        self.lock.release()
        return order_id

    def get_frozen_volume_from_exchange(self):
        buy_frozen_volume_dict = {}
        sell_frozen_volume_dict = {}
        for exchange in self.base_exchange_info.keys():
            buy_frozen_volume_dict[exchange] = 0
            sell_frozen_volume_dict[exchange] = 0
        self.lock.acquire()
        for vt_order_id, order in self.order_dict.items():
            if order.is_active() and order.exchange in self.base_exchange_info.keys():
                if order.direction == Direction.LONG.value:
                    buy_frozen_volume_dict[order.exchange] += order.volume - order.traded
                else:
                    sell_frozen_volume_dict[order.exchange] += order.volume - order.traded
        self.lock.release()
        return buy_frozen_volume_dict, sell_frozen_volume_dict

    def get_volumes(self):
        buy_frozen_volume_dict, sell_frozen_volume_dict = self.get_frozen_volume_from_exchange()
        self.lock.acquire()
        bid_price, bid_volume, exchange = self.base_bids[0]
        ask_price, ask_volume, exchange = self.base_asks[0]
        for i in range(1, 5):
            bid_volume += self.base_bids[i][1]
            ask_volume += self.base_asks[i][1]

        # log_service_manager.write_log("self.base_bids:{}".format(self.base_bids))
        # log_service_manager.write_log("self.base_asks:{}".format(self.base_asks))
        # log_service_manager.write_log("bid_volume:{}".format(bid_volume))
        # log_service_manager.write_log("ask_volume:{}".format(ask_volume))

        pos_target = 0.0
        pos_base = 0.0
        for exchange in self.base_exchange_info.keys():
            dic = self.base_exchange_info[exchange]
            pos_target += dic["pos_target_symbol"]
            pos_target -= sell_frozen_volume_dict[exchange]
            pos_base += dic["pos_base_symbol"]
            pos_base -= buy_frozen_volume_dict[exchange] * bid_price

        max_buy_amount = min(pos_base / ask_price - self.need_lock_buy_volume - self.need_cover_buy_volume,
                             ask_volume / 2.0)
        max_buy_amount = max(max_buy_amount, 0)
        min_buy_amount = get_system_inside_min_volume(self.symbol_pair, ask_price, exchange)
        if max_buy_amount < min_buy_amount:
            max_buy_amount = 0
            min_buy_amount = 0

        # max_buy_amount = min(pos_target - self.need_lock_sell_volume - self.need_cover_sell_volume, bid_volume / 2.0)
        # max_buy_amount = max(max_buy_amount, 0)
        # min_buy_amount = get_system_inside_min_volume(self.symbol_pair, ask_price, exchange)
        # if max_buy_amount < min_buy_amount:
        #     max_buy_amount = 0
        #     min_buy_amount = 0
        #
        self.ret_buy["minAmount"] = min_buy_amount
        self.ret_buy["maxAmount"] = max_buy_amount
        self.ret_buy["price"] = ask_price * (1 + self.base_spread / 100.0)

        max_sell_amount = min(pos_target - self.need_lock_sell_volume - self.need_cover_sell_volume, bid_volume / 2.0)
        max_sell_amount = max(max_sell_amount, 0)
        min_sell_amount = get_system_inside_min_volume(self.symbol_pair, bid_price, exchange)
        if max_sell_amount < min_sell_amount:
            max_sell_amount = 0
            min_sell_amount = 0

        # max_sell_amount = min(pos_base / ask_price - self.need_lock_buy_volume - self.need_cover_buy_volume, ask_volume / 2.0)
        # max_sell_amount = max(max_sell_amount, 0)
        # min_sell_amount = get_system_inside_min_volume(self.symbol_pair, bid_price, exchange)
        # if max_sell_amount < min_sell_amount:
        #     max_sell_amount = 0
        #     min_sell_amount = 0
        #
        self.ret_sell["minAmount"] = min_sell_amount
        self.ret_sell["maxAmount"] = max_sell_amount
        self.ret_sell["price"] = bid_price * (1 - self.base_spread / 100.0)

        r_buy = copy(self.ret_buy)
        r_sell = copy(self.ret_sell)
        self.lock.release()
        return r_buy, r_sell

    def get_real_uniqId(self, uniqId):
        return uniqId.split('-')[0]

    def get_tokenlon_req(self, symbol, amount, uniqId, direction, quoteId):
        tokenlon_req = TokenlonReq()
        tokenlon_req.symbol = symbol
        tokenlon_req.uniqId = uniqId
        tokenlon_req.amount = amount
        tokenlon_req.direction = direction
        tokenlon_req.quoteId = quoteId
        tokenlon_req.req_time = time.time()
        return tokenlon_req

    def judge_and_then_lock_money(self, params, ret_info, uniqId):
        real_uniqId = self.get_real_uniqId(uniqId)
        self.lock.acquire()
        before_lock_money_req = self.uniqId_lock_money_req_dict.get(real_uniqId, None)
        if before_lock_money_req is not None:
            if before_lock_money_req.direction == Direction.LONG.value:
                self.need_lock_buy_volume -= before_lock_money_req.amount
            else:
                self.need_lock_sell_volume -= before_lock_money_req.amount
            del self.uniqId_lock_money_req_dict[real_uniqId]
            self.lock.release()

            ret_info = self.getResponsePriceInfo(params)
        else:
            self.lock.release()

        return ret_info

    def lock_money(self, symbol, amount, uniqId, direction, quoteId):
        real_uniqId = self.get_real_uniqId(uniqId)
        tokenlon_req = self.get_tokenlon_req(symbol, amount, uniqId, direction, quoteId)

        self.lock.acquire()
        before_lock_money_req = self.uniqId_lock_money_req_dict.get(real_uniqId, None)
        if before_lock_money_req is not None:
            if before_lock_money_req.direction == Direction.LONG.value:
                self.need_lock_buy_volume -= before_lock_money_req.amount
            else:
                self.need_lock_sell_volume -= before_lock_money_req.amount

        self.uniqId_lock_money_req_dict[real_uniqId] = copy(tokenlon_req)

        if direction == Direction.LONG.value:
            self.need_lock_buy_volume += amount
        else:
            self.need_lock_sell_volume += amount

        self.req_dict[tokenlon_req.quoteId] = copy(tokenlon_req)
        self.lock.release()

        log_service_manager.write_log(
            "[lock_money] need_lock_buy_volume:{},need_lock_sell_volume:{}".format(self.need_lock_buy_volume,
                                                                                   self.need_lock_sell_volume))

    def check_to_remove_before_req(self):
        now_time = time.time()
        all_msg_arr = []
        self.lock.acquire()
        all_keys = list(self.req_dict.keys())
        for key in all_keys:
            req = self.req_dict[key]
            if now_time - req.req_time > 35:
                del self.req_dict[key]
                if req.direction == Direction.LONG.value:
                    self.need_lock_buy_volume -= req.amount
                else:
                    self.need_lock_sell_volume -= req.amount

                real_uniqId = self.get_real_uniqId(req.uniqId)
                if real_uniqId in self.uniqId_lock_money_req_dict.keys():
                    del self.uniqId_lock_money_req_dict[real_uniqId]
                all_msg_arr.append(req.toString())
        self.lock.release()
        if len(all_msg_arr) > 0:
            self.write_lock_log_msg('..'.join(all_msg_arr))

    def on_init(self):
        self.write_log("on_init")
        self.update_exchange_info()
        self.update_account()

    def cancel_not_cover_order(self):
        """
        撤销掉那些 在回补市场，但是回补时间过长的订单，初步定义10秒不成交就撤单
        """
        now = time.time()
        self.lock.acquire()
        need_cancel_sets = set([])
        for vt_order_id, order in self.order_dict.items():
            order_time = time.mktime(time.strptime(order.order_time, "%Y-%m-%d %H:%M:%S"))

            # 回补的订单，超过60秒就撤
            if now - order_time > 60 * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                need_cancel_sets.add(order.vt_order_id)
                self.write_log('[prepare buy cover_order] vt_order_id:{},order.order_time:{},old_order_time:{}'
                               .format(vt_order_id, order_time, order.order_time))
        self.lock.release()
        if len(need_cancel_sets) > 0:
            self.cancel_sets_order(need_cancel_sets)

    def cancel_sets_order(self, need_cancel_sets):
        # 发出撤单
        for vt_order_id in need_cancel_sets:
            # self.write_log("[cancel sets order] vt_order_id:{}".format(vt_order_id))
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1

            if self.cancel_dict_times_count[vt_order_id] > 5:
                if vt_order_id in self.order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.order_dict[vt_order_id]

                    dic = self.base_exchange_info.get(order.exchange, {})
                    del self.order_dict[vt_order_id]
                    self.write_log('[cancel_sets_order] vt_order_id:{}'.format(vt_order_id))

    def cover_orders(self):
        frozen_sent_buy_volume, frozen_sent_sell_volume = self.get_frozen_volume_from_dict(self.order_dict)
        self.lock.acquire()
        buy_order_req = []
        if self.need_cover_buy_volume > frozen_sent_buy_volume:
            log_service_manager.write_log(
                "frozen_sent_buy_volume:{},frozen_sent_sell_volume:{}".format(frozen_sent_buy_volume,
                                                                              frozen_sent_sell_volume))
            log_service_manager.write_log("need_cover_buy_volume:{}".format(self.need_cover_buy_volume))
            tmp_deal_volume = abs(self.need_cover_buy_volume - frozen_sent_buy_volume) * 1.002
            if tmp_deal_volume > self.total_volume_min:
                buy_price = self.base_asks[0][0] * 1.004
                exchange = self.base_asks[0][2]
                buy_price = get_round_order_price(buy_price, self.base_exchange_info[exchange]["price_tick"])
                buy_volume = get_round_order_price(tmp_deal_volume, self.base_exchange_info[exchange]["volume_tick"])
                if buy_volume > get_system_inside_min_volume(self.symbol_pair, self.base_asks[0][0], exchange):
                    buy_order_req.append([exchange, Direction.LONG.value, buy_price, buy_volume])

        sell_order_req = []
        if self.need_cover_sell_volume > frozen_sent_sell_volume:
            log_service_manager.write_log(
                "frozen_sent_buy_volume:{},frozen_sent_sell_volume:{}".format(frozen_sent_buy_volume,
                                                                              frozen_sent_sell_volume))
            log_service_manager.write_log("need_cover_sell_volume:{}".format(self.need_cover_sell_volume))

            tmp_deal_volume = abs(self.need_cover_sell_volume - frozen_sent_sell_volume) * 1.002
            if tmp_deal_volume > self.total_volume_min:
                sell_price = self.base_bids[0][0] * 0.996
                exchange = self.base_bids[0][2]
                sell_price = get_round_order_price(sell_price, self.base_exchange_info[exchange]["price_tick"])
                sell_volume = get_round_order_price(tmp_deal_volume, self.base_exchange_info[exchange]["volume_tick"])
                if sell_volume > get_system_inside_min_volume(self.symbol_pair, self.base_bids[0][0], exchange):
                    sell_order_req.append([exchange, Direction.SHORT.value, sell_price, sell_volume])

        self.lock.release()
        if len(buy_order_req) > 0:
            self.send_list_order(buy_order_req)
        if len(sell_order_req) > 0:
            self.send_list_order(sell_order_req)

    def send_list_order(self, req_list):
        self.write_log("[send_list_order] req_list:{}".format(req_list))
        info_msg_array = []
        self.lock.acquire()
        for exchange, direction, price, volume in req_list:
            if direction == Direction.LONG.value:
                offset = Offset.OPEN.value
            else:
                offset = Offset.CLOSE.value
            ret_orders = self.send_order(self.symbol_pair, exchange, direction, offset, price, volume)
            self.write_log("[ret_orders] :{}".format(len(ret_orders)))
            for vt_order_id, order in ret_orders:
                self.order_dict[vt_order_id] = order
                info_msg_array.append(
                    "[send_order] direction:{},price:{},volume:{},vt_order_id:{},order_time:{},status:{}".format(
                        order.direction, order.price, order.volume, vt_order_id, order.order_time, order.status))
        self.lock.release()
        for msg in info_msg_array:
            self.write_lock_log_msg(msg)

    def cover_quote_req(self, quoteId, tmp_need_cover_buy_volume, tmp_need_cover_sell_volume):
        self.lock.acquire()
        req = self.req_dict.get(quoteId, None)
        log_service_manager.write_log(
            "cover_quote_req quoteId:{}, tmp_need_cover_buy_volume:{}, tmp_need_cover_sell_volume:{}".format(quoteId,
                                                                                                             tmp_need_cover_buy_volume,
                                                                                                             tmp_need_cover_sell_volume))
        if req:
            if req.direction == Direction.LONG.value:
                self.need_lock_buy_volume -= req.amount
            else:
                self.need_lock_sell_volume -= req.amount
            del self.req_dict[quoteId]

            self.need_cover_buy_volume += tmp_need_cover_buy_volume
            self.need_cover_sell_volume += tmp_need_cover_sell_volume

            log_service_manager.write_log(
                "cover_quote_req quoteId:{}, self.need_cover_buy_volume:{}, self.need_cover_sell_volume:{}".format(
                    quoteId, self.need_cover_buy_volume, self.need_cover_sell_volume))

            real_uniqId = self.get_real_uniqId(req.uniqId)
            if real_uniqId in self.uniqId_lock_money_req_dict.keys():
                del self.uniqId_lock_money_req_dict[real_uniqId]
            self.lock.release()

            # go to cover
            exchange = Exchange.HUOBI.value
            send_order_req = []
            self.lock.acquire()
            if req.direction == Direction.LONG.value:
                price = get_round_order_price(self.base_asks[0][0] * 1.004,
                                              self.base_exchange_info[exchange]["price_tick"])

                volume = get_round_order_price(tmp_need_cover_buy_volume,
                                               self.base_exchange_info[exchange]["volume_tick"])
                if volume > get_system_inside_min_volume(self.symbol_pair, self.base_asks[0][0], exchange):
                    send_order_req.append([exchange, req.direction, price, volume])

            else:
                price = get_round_order_price(self.base_bids[0][0] * 0.996,
                                              self.base_exchange_info[exchange]["price_tick"])
                volume = get_round_order_price(tmp_need_cover_sell_volume,
                                               self.base_exchange_info[exchange]["volume_tick"])
                if volume > get_system_inside_min_volume(self.symbol_pair, self.base_bids[0][0], exchange):
                    send_order_req.append([exchange, req.direction, price, volume])

            log_service_manager.write_log("send_req: price:{},amount:{}".format(price, req.amount))
            self.lock.release()
            if len(send_order_req) > 0:
                self.send_list_order(send_order_req)
        else:
            self.lock.release()
            self.write_lock_log_msg("[Error cover_quote_req] why quoteId:{} is not here".format(quoteId))

    def on_tick(self, tick):
        if tick.bid_prices[0] > 0:
            self.lock.acquire()
            self.recent_datetime = tick.datetime
            for i in range(len(tick.bid_prices)):
                if tick.bid_prices[i] > 0.0 and i < MAX_PRICE_NUM:
                    self.base_bids[i] = (tick.bid_prices[i], tick.bid_volumes[i], Exchange.HUOBI.value)

            for i in range(len(tick.ask_prices)):
                if tick.ask_prices[i] > 0.0 and i < MAX_PRICE_NUM:
                    self.base_asks[i] = (tick.ask_prices[i], tick.ask_volumes[i], Exchange.HUOBI.value)

            self.lock.release()
            self.cancel_not_cover_order()
            self.cover_orders()

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        self.lock.acquire()
        if order.exchange in self.base_exchange_info.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id, None)
                if bef_order is not None:
                    new_traded = order.traded - bef_order.traded
                    self.need_cover_buy_volume -= new_traded
                    self.order_dict[order.vt_order_id] = copy(order)
                    self.base_exchange_info[order.exchange]["pos_base_symbol"] -= new_traded * order.price
                    self.base_exchange_info[order.exchange]["pos_target_symbol"] += new_traded * 0.998

                    if not order.is_active():
                        self.order_dict.pop(order.vt_order_id)
            else:
                bef_order = self.order_dict.get(order.vt_order_id, None)
                if bef_order is not None:
                    new_traded = order.traded - bef_order.traded
                    self.need_cover_sell_volume -= new_traded
                    self.order_dict[order.vt_order_id] = copy(order)
                    self.base_exchange_info[order.exchange]["pos_base_symbol"] += new_traded * order.price * 0.998
                    self.base_exchange_info[order.exchange]["pos_target_symbol"] -= new_traded

                    if not order.is_active():
                        self.order_dict.pop(order.vt_order_id)
        self.lock.release()

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        self.write_log('[trade detail] :{}'.format(trade.__dict__))

        self.write_important_log(
            '[trade] symbol:{},exchange:{},direciton:{},price:{},volume:{}'.format(trade.symbol, trade.exchange,
                                                                                   trade.direction, trade.price,
                                                                                   trade.volume))

    def getResponsePriceInfo(self, params):
        direction = params["direction"]
        amount = float(params["amount"])
        has_reverse = params["has_reverse"]

        ret_info = {
            "result": False,
            "exchangeable": False,
            "minAmount": 0.002,
            "maxAmount": 100,
            "message": "money is not enough!"
        }
        r_buy, r_sell = self.get_volumes()
        if amount > 0 and has_reverse:
            r_buy["minAmount"] = r_buy["minAmount"] * r_buy["price"]
            r_buy["maxAmount"] = r_buy["maxAmount"] * r_buy["price"]
            r_buy["price"] = 1.0 / r_buy["price"]
            r_sell["minAmount"] = r_sell["minAmount"] * r_sell["price"]
            r_sell["maxAmount"] = r_sell["maxAmount"] * r_sell["price"]
            r_sell["price"] = 1.0 / r_sell["price"]

        log_service_manager.write_log("getResponsePriceInfo params:{}".format(params))
        log_service_manager.write_log("getResponsePriceInfo r_buy:{}".format(r_buy))
        log_service_manager.write_log("getResponsePriceInfo r_sell:{}".format(r_sell))
        if amount == 0:
            if direction == Direction.LONG.value:
                if r_buy["maxAmount"] <= 1e-8:
                    ret_info = {"result": False, "message": "insufficient balance!", "exchangeable": False,
                                "price": r_buy["price"], "minAmount": r_buy["minAmount"],
                                "maxAmount": r_buy["maxAmount"]}
                else:
                    ret_info = {"result": True, "message": "success!", "exchangeable": True, "price": r_buy["price"],
                                "minAmount": r_buy["minAmount"], "maxAmount": r_buy["maxAmount"]}
            else:
                if r_sell["maxAmount"] <= 1e-8:
                    ret_info = {"result": False, "message": "insufficient balance!", "exchangeable": False,
                                "price": r_sell["price"], "minAmount": r_sell["minAmount"],
                                "maxAmount": r_sell["maxAmount"]}
                else:
                    ret_info = {"result": True, "message": "success!", "exchangeable": True, "price": r_sell["price"],
                                "minAmount": r_sell["minAmount"], "maxAmount": r_sell["maxAmount"]}
        else:
            if direction == Direction.LONG.value:
                if r_buy["minAmount"] <= amount <= r_buy["maxAmount"]:
                    ret_info = {"result": True, "message": "success!", "exchangeable": True,
                                "minAmount": r_buy["minAmount"],
                                "maxAmount": r_buy["maxAmount"], "price": r_buy["price"]}
                else:
                    ret_info = {"result": False, "message": "money is not enough!", "exchangeable": False,
                                "minAmount": r_buy["minAmount"],
                                "maxAmount": r_buy["maxAmount"], "price": r_buy["price"]}
            else:
                if r_sell["minAmount"] <= amount <= r_sell["maxAmount"]:
                    ret_info = {"result": True, "message": "success!", "exchangeable": True,
                                "minAmount": r_sell["minAmount"],
                                "maxAmount": r_sell["maxAmount"], "price": r_sell["price"]}
                else:
                    ret_info = {"result": False, "message": "money is not enough!", "exchangeable": False,
                                "minAmount": r_sell["minAmount"],
                                "maxAmount": r_sell["maxAmount"], "price": r_sell["price"]}
        return ret_info

    def indicativePrice(self, params):
        self.write_lock_log_msg("[indicativePrice] indicativePrice:{}".format(params))
        ret_info = self.getResponsePriceInfo(params)

        log_service_manager.write_log("indicativePrice ret_info:{}".format(ret_info))
        return ret_info

    def price(self, params):
        self.write_lock_log_msg("[price] params:{}".format(params))
        uniqId = params["uniqId"]
        direction = params["direction"]
        amount = float(params["amount"])
        symbol = params["symbol"]
        quoteId = self.generate_quote_id(symbol)

        ret_info = self.getResponsePriceInfo(params)
        if ret_info["result"]:
            ret_info["quoteId"] = quoteId
            self.lock_money(symbol, amount, uniqId, direction, quoteId)
            log_service_manager.write_log("price ret_info:{}".format(ret_info))
        else:
            # 需要处理锁仓更新这种情况
            # print("not deal")
            ret_info = self.judge_and_then_lock_money(params, ret_info, uniqId)
            if ret_info["result"]:
                ret_info["quoteId"] = quoteId
                self.lock_money(symbol, amount, uniqId, direction, quoteId)
            log_service_manager.write_log("judge_and_then_lock_money ret_info:{}".format(ret_info))

        return ret_info

    def deal(self, params):
        self.write_lock_log_msg("[deal] params:{}".format(params))
        quoteID = params["quoteId"]
        makerToken = params["makerToken"].lower()
        makerTokenAmount = float(params["makerTokenAmount"])
        takerTokenAmount = float(params["takerTokenAmount"])

        tmp_need_cover_buy_volume = 0
        tmp_need_cover_sell_volume = 0
        if makerToken == self.target_symbol:
            tmp_need_cover_buy_volume += makerTokenAmount
        else:
            tmp_need_cover_sell_volume += takerTokenAmount

        self.cover_quote_req(quoteID, tmp_need_cover_buy_volume, tmp_need_cover_sell_volume)
        log_service_manager.write_log("deal: result True")
        return {"result": True}
