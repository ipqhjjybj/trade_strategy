# coding=utf-8

import time
from copy import copy
from collections import defaultdict

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.constant import TradeType, MQCommonInfo
from tumbler.object import BBOTickData, OrderData, TradeData, RejectCoverOrderRequest, CoverOrderRequest
from tumbler.object import DictAccountData
from tumbler.constant import Direction, MakerControlType
from tumbler.function import get_two_currency, get_from_vt_key, get_vt_key


def parse_cover_control_info(cover_control_info):
    all_exchanges = []
    all_assets = []
    cover_order_dict = {}
    frozen_order_dict = {}
    for symbol in cover_control_info.keys():
        dic = cover_control_info[symbol]

        target_asset, base_asset = get_two_currency(symbol)
        all_assets.extend([target_asset, base_asset])
        all_exchanges.extend(dic["exchanges"])

    all_assets = list(set(all_assets))
    all_exchanges = list(set(all_exchanges))
    for exchange in all_exchanges:
        cover_order_dict[exchange] = defaultdict(float)
        frozen_order_dict[exchange] = defaultdict(float)
        for asset in all_assets:
            cover_order_dict[exchange][asset] = 0
            frozen_order_dict[exchange][asset] = 0
    return copy(cover_order_dict), copy(frozen_order_dict), all_exchanges, all_assets


def convert_direction(s):
    if s == "buy":
        return Direction.LONG.value
    else:
        return Direction.SHORT.value


def reverse_direction(s):
    if s == Direction.LONG.value:
        return Direction.SHORT.value
    else:
        return Direction.LONG.value


class MarketMakerControllerStrategy(MarketMakerTemplate):
    """
    Do:
    接收 成交单子的信息， 然后计算得出回补仓位，然后发到 cover 控制器
    """
    author = "ipqhjjybj"
    class_name = "MarketMakerControllerStrategy"

    listen_markets_info = {}
    cover_control_info = {}
    """
    默认参数
    """

    parameters = [
        "strategy_name",  # 策略加载的唯一性名字
        "class_name",  # 类的名字
        "author",  # 作者
        "vt_bbo_subscribe",  # bbo ticker
        "vt_symbols_subscribe",  # 订阅的交易对
        "vt_account_name_subscribe",  # 订阅的账户 account
        "listen_markets_info",  # 订阅的各个账户的 交易对信息
        "cover_control_info",  # 控制信息
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MarketMakerControllerStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.price_info = {}
        self.recevice_trade_data = {}
        self.cover_exchanges_dict = {}

        for bbo_exchange in self.vt_bbo_subscribe:
            self.price_info[bbo_exchange] = {}

        self.cover_order_dict, self.frozen_order_dict, self.cover_exchanges, self.all_assets = \
            parse_cover_control_info(self.cover_control_info)

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def on_bbo_tick(self, tick: BBOTickData):
        #self.write_log("[on_bbo_tick] tick:{}".format(tick.__dict__))
        self.price_info[tick.exchange] = copy(tick.symbol_dic)

    def clean_old_orders(self):
        # 对于已经完成的订单, 直接删掉超过30分钟的那种
        now = time.time()
        for vt_order_id, trade in self.recevice_trade_data.items():
            trade_time = time.mktime(time.strptime(trade.trade_time, "%Y-%m-%d %H:%M:%S"))
            if now - trade_time > 1800:
                self.recevice_trade_data.pop(vt_order_id)

    def on_order(self, order: OrderData):
        pass
        # if order.vt_order_id in self.cover_order_dict.keys():
        #     order = self.cover_order_dict[order.vt_order_id]

    def is_asset_enough(self, exchange, asset, volume):
        return self.cover_order_dict[exchange][asset] - self.frozen_order_dict[exchange][asset] > volume

    def choose_best_exchange_and_price(self, symbol, exchanges, direction, volume):
        """
        :param symbol:
        :param exchanges:
        :param direction:  trade direction
        :return:
        """
        self.write_log("[choose_best_exchange_and_price] price_info:{}".format(self.price_info))
        ret = None
        bef_price = None
        target_asset, base_asset = get_two_currency(symbol)
        for exchange in exchanges:
            if direction == Direction.LONG.value:
                if symbol in self.price_info[exchange].keys():
                    price = self.price_info[exchange][symbol]["ask"][0]
                    if not bef_price or bef_price > price > 0 and self.is_asset_enough(exchange, base_asset,
                                                                                       price * volume):
                        bef_price = price
                        ret = exchange

            else:
                if symbol in self.price_info[exchange].keys():
                    price = self.price_info[exchange][symbol]["bid"][0]
                    if not bef_price or bef_price < price and self.is_asset_enough(exchange, target_asset, volume):
                        bef_price = price
                        ret = exchange

        return ret, bef_price

    def get_cover_orders_from_direct_type(self, control_dict: dict, trade: TradeData):
        cover_order_reqs = []
        req = CoverOrderRequest()
        choose_exchange, price = self.choose_best_exchange_and_price(trade.symbol, control_dict["exchanges"],
                                                                     trade.direction, trade.volume)
        req.symbol = trade.symbol
        req.exchange = choose_exchange
        req.vt_symbol = get_vt_key(req.symbol, req.exchange)
        req.direction = trade.direction
        req.price = price
        req.volume = trade.volume
        cover_order_reqs.append(copy(req))
        #self.write_log("[get_cover_orders_from_direct_type] req:{}".format(req.__dict__))
        return cover_order_reqs

    def get_cover_order_from_two_type(self, control_dict, trade: TradeData):
        #self.write_log("[get_cover_order_from_two_type] control_dict:{}".format(control_dict))
        cover_order_reqs = []
        target_volume = trade.volume
        base_volume = trade.volume * trade.price

        if trade.direction == Direction.LONG.value:
            target_direction = trade.direction
            base_direction = reverse_direction(trade.direction)
        else:
            target_direction = reverse_direction(trade.direction)
            base_direction = trade.direction

        target_exchange, target_price = self.choose_best_exchange_and_price(control_dict["target"],
                                                                            control_dict["exchanges"], target_direction,
                                                                            trade.volume)
        base_exchange, base_price = self.choose_best_exchange_and_price(control_dict["base"], control_dict["exchanges"],
                                                                        base_direction, trade.volume)

        req = CoverOrderRequest()
        req.symbol = control_dict["target"]
        req.exchange = target_exchange
        req.vt_symbol = get_vt_key(req.symbol, req.exchange)
        req.direction = target_direction
        req.volume = target_volume
        cover_order_reqs.append(copy(req))

        self.write_log("[get_cover_order_from_two_type] req1:{}".format(req.__dict__))
        req = CoverOrderRequest()
        req.symbol = control_dict["base"]
        req.exchange = base_exchange
        req.vt_symbol = get_vt_key(req.symbol, req.exchange)
        req.direction = base_direction
        req.price = base_price
        req.volume = base_volume
        cover_order_reqs.append(copy(req))

        self.write_log("[get_cover_order_from_two_type] req2:{}".format(req.__dict__))
        return cover_order_reqs

    def on_trade(self, trade: TradeData):
        self.write_log("[on_trade]:{}".format(trade.__dict__))
        if trade.trade_type in [TradeType.PUT_ORDER.value, TradeType.COVER_ORDER.value]:
            if trade.vt_trade_id not in self.recevice_trade_data.keys():
                self.write_log("trade.vt_trade_id:{} is in".format(trade.vt_trade_id))
                self.recevice_trade_data[trade.vt_trade_id] = copy(trade)

                control_dict = self.cover_control_info[trade.symbol]
                if trade.trade_type == TradeType.PUT_ORDER.value:
                    cover_type = control_dict["type"]
                    if cover_type == MakerControlType.DIRECT.value:
                        cover_orders = self.get_cover_orders_from_direct_type(control_dict, trade)
                        self.write_log("[DIRECT] cover_orders:{}".format(cover_orders))
                    else:
                        cover_orders = self.get_cover_order_from_two_type(control_dict, trade)
                        self.write_log("[BUY_SELL] cover_orders:{}".format(cover_orders))

                    for cover_order in cover_orders:
                        self.send_cover_order_req(copy(cover_order))
                else:
                    # COVER_ORDER
                    self.write_log("has cover order:{}".format(trade.__dict__))
                    target_asset, base_asset = get_two_currency(self.target_symbol)
                    if trade.direction == Direction.LONG.value:
                        self.frozen_order_dict[trade.exchange][base_asset] -= trade.price * trade.volume
                    else:
                        self.frozen_order_dict[trade.exchange][target_asset] += trade.volume

    def on_reject_cover_order_request(self, req: RejectCoverOrderRequest):
        self.write_log("[receive reject order] req:{}".format(req.__dict__))
        self.send_cover_order_req(req.make_cover_order_req())

    def send_cover_order_req(self, req: CoverOrderRequest):
        target_asset, base_asset = get_two_currency(req.symbol)
        if req.direction == Direction.LONG.value:
            self.frozen_order_dict[req.exchange][base_asset] += req.volume * req.price
        else:
            self.frozen_order_dict[req.exchange][target_asset] += req.volume
        super(MarketMakerControllerStrategy, self).send_cover_order_req(req)
        self.write_log("[end send_cover_order_req]")

    def on_dict_account(self, dict_acct: DictAccountData):
        for vt_account_id, dict in dict_acct.account_dict.items():
            asset, exchange = get_from_vt_key(vt_account_id)
            self.cover_order_dict[exchange][asset] = dict["balance"]

        to_send_account = DictAccountData()
        to_send_account.account_name = MQCommonInfo.COVER_ALL_ACCOUNT.value
        for asset in self.all_assets:
            max_asset = 0.0
            for exchange in self.cover_exchanges:
                max_asset = max(max_asset, self.cover_order_dict[exchange][asset])
                to_send_account.account_dict[asset] = {"frozen": 0, "balance": max_asset}

        self.send_cover_account_dict(to_send_account)

    def output_important_log(self):
        self.write_log("[output important log]")
