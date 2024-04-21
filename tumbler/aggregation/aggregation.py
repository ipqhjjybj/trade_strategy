# coding=utf-8

import time
from threading import Thread
from copy import copy
from datetime import datetime, timedelta

from tumbler.gate import RabbitmqGateway
from tumbler.gateway import rest_exchange_map, ws_exchange_map

from tumbler.constant import Exchange
from tumbler.constant import MAX_PRICE_NUM
import tumbler.config as config
from tumbler.object import (
    MergeTickData,
    TickData,
    BBOTickData,
    SubscribeRequest
)
from tumbler.function import get_vt_key, load_json, datetime_bigger
from tumbler.service import MQSender
from tumbler.constant import MQSubscribeType
from tumbler.apps.data_third_part.base import get_diff_type_exchange_name
from tumbler.service.log_service import log_service_manager
from tumbler.event import (
    EVENT_TICK_REST,
    EVENT_TICK_WS,
    EVENT_MERGE_TICK,
    Event,
    EVENT_BBO_TICK
)


class Aggregation(object):
    """ 接收行情 并聚合 , 之后存储聚合的行情 """

    def __init__(self, event_engine, filename="aggregation.json"):
        self.event_engine = event_engine

        self.mq_aggr_sender_dict = {}
        self.mq_single_sender_dict = {}
        self.mq_bbo_sender_dict = {}
        self.symbols = []
        self.gateways = {}

        self.in_server_stop_exchanges = []  # 这个交易所行情在维护

        self.websockets = {}  # {exchange:websocket_data}
        self.rests = {}  # {exchange:restData}

        self.recent_datetime_ticks = {}  # 单个交易所最近的时间，datetime.now()
        self.s_ticks = {}  # single_exchange tick
        self.merge_ticks = {}  # {"btc_usdt.MERGE":MergeTickData.object()}
        self.tot_array_ticks = {}  # {"btc_usdt":{"HUOBI":tick.object()}, "eth_usdt":{"HUOBI":tick.object()}}

        self.has_start = False
        self.active = False
        self._thread = None

        data = load_json(filename)

        self.flag_produce_merge = data.get("produce_merge", False)
        self.flag_produce_bbo = data.get("produce_bbo", False)
        self.flag_produce_single_ticks = data.get("produce_ticks", False)
        self.flag_send_ws_ticks = data.get("produce_direct_ticks", False)

        self.symbols = data["symbols"]
        self.exchanges_dict = data["exchanges"]
        self.in_server_stop_exchanges = data.get("in_server_stop_exchanges", [])

        for symbol in self.symbols.keys():
            tick = MergeTickData()
            tick.symbol = symbol
            tick.vt_symbol = get_vt_key(tick.symbol, Exchange.AGGREGATION.value)
            tick.datetime = datetime.now()
            tick.gateway_name = Exchange.AGGREGATION.value
            self.merge_ticks[symbol] = tick

            if self.flag_produce_merge:
                queue_exchange_name = get_diff_type_exchange_name(MQSubscribeType.MERGE_TICKER.value, tick.vt_symbol)
                sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                  user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                  exchange=queue_exchange_name)
                self.mq_aggr_sender_dict[tick.vt_symbol] = sender

        if self.flag_produce_bbo:
            for exchange in self.exchanges_dict.keys():
                queue_exchange_name = get_diff_type_exchange_name(MQSubscribeType.BBO_TICKER.value, exchange)
                sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                  user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                  exchange=queue_exchange_name)
                self.mq_bbo_sender_dict[exchange] = sender

        for symbol in self.symbols.keys():
            list_exchanges = self.symbols[symbol]
            od = {}
            for exchange_name in self.exchanges_dict.keys():
                if exchange_name in list_exchanges:
                    tick = TickData()
                    tick.symbol = symbol
                    tick.exchange = exchange_name
                    tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
                    tick.datetime = datetime.now()
                    tick.gateway_name = exchange_name
                    od[exchange_name] = copy(tick)

                    self.s_ticks[tick.vt_symbol] = copy(tick)

                    # compute queue_exchange
                    # 这个ticker 跟以前的相比，差别在于，是只传超过前一秒的数据 (不会20个tick一起)
                    if self.flag_produce_single_ticks:
                        queue_exchange_name = get_diff_type_exchange_name(MQSubscribeType.TIKER.value, tick.vt_symbol)
                        sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                          user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                          exchange=queue_exchange_name)
                        self.mq_single_sender_dict[tick.vt_symbol] = sender
                        self.recent_datetime_ticks[tick.vt_symbol] = datetime.now()

            self.tot_array_ticks[symbol] = od

        for exchange_name in self.exchanges_dict.keys():
            dic = self.exchanges_dict[exchange_name]
            use_rest = dic["use_rest"]
            use_ws = dic["use_ws"]

            g = RabbitmqGateway(self.event_engine, exchange_name, flag_sender=self.flag_send_ws_ticks)

            if use_rest:
                rest_obj = rest_exchange_map.get(exchange_name, None)
                if rest_obj:
                    self.rests[exchange_name] = rest_obj(g)

            if use_ws:
                websocket_obj = ws_exchange_map.get(exchange_name, None)
                if websocket_obj:
                    self.websockets[exchange_name] = websocket_obj(g)

        self.register_event()

    def register_event(self):
        self.event_engine.register(EVENT_TICK_REST, self.process_rest_tick_event)
        self.event_engine.register(EVENT_TICK_WS, self.process_ws_tick_event)

    def _start(self):
        if self.has_start:
            return

        self.active = True
        self.has_start = True
        self._thread = Thread(target=self.produce_ticks())
        self._thread.start()

    def connect(self):
        for exchange_name in self.websockets.keys():
            ws = self.websockets[exchange_name]

            dic = self.exchanges_dict.get(exchange_name, {})
            api_key = dic.get("key", "")
            secret_key = dic.get("secret_key", "")
            passphrase = dic.get("passphrase", "")

            if exchange_name == Exchange.OKEX.value:
                ws.connect(api_key, secret_key, passphrase)
            else:
                ws.connect(api_key, secret_key)

        for exchange_name in self.rests.keys():
            rest = self.rests[exchange_name]
            rest.connect()

        # sleep 3 seconds , waitting for the market data
        time.sleep(3)

        for symbol in self.symbols.keys():
            list_exchanges = self.symbols[symbol]

            for exchange_name in self.websockets.keys():
                if exchange_name in list_exchanges:
                    ws = self.websockets[exchange_name]

                    sub = SubscribeRequest()
                    sub.symbol = symbol
                    sub.exchange = exchange_name
                    sub.vt_symbol = get_vt_key(symbol, exchange_name)

                    ws.subscribe(sub)

            for exchange_name in self.rests.keys():
                if exchange_name in list_exchanges:
                    rest = self.rests[exchange_name]

                    sub = SubscribeRequest()
                    sub.symbol = symbol
                    sub.exchange = exchange_name
                    sub.vt_symbol = get_vt_key(symbol, exchange_name)

                    rest.subscribe(sub)

        self._start()

    def process_ticks(self, tick: TickData):
        if tick.exchange in self.in_server_stop_exchanges:
            log_service_manager.write_log("tick.symbol:{} exchange is not in serve time!".format(tick.vt_symbol))
            return

        sn_ticker = self.s_ticks.get(tick.vt_symbol, None)
        if sn_ticker:
            if tick.datetime > sn_ticker.datetime and tick.bid_prices[0] > 0:
                self.s_ticks[tick.vt_symbol] = copy(tick)

                pre_datetime = self.recent_datetime_ticks.get(tick.vt_symbol, None)
                if pre_datetime and tick.datetime >= pre_datetime + timedelta(seconds=1):
                    if self.flag_produce_single_ticks:
                        sender = self.mq_single_sender_dict[tick.vt_symbol]
                        sender.send("", tick.get_mq_msg())

                        log_service_manager.write_log("[process_ticks] tick:{}".format(tick.get_mq_msg()))

                    self.recent_datetime_ticks[tick.vt_symbol] = tick.datetime

        if tick.symbol in self.tot_array_ticks.keys():
            merge_tick = self.merge_ticks.get(tick.symbol, None)
            if merge_tick is None:
                return

            exchange_list = self.symbols[tick.symbol]
            if tick.exchange in exchange_list:
                dic = self.tot_array_ticks[tick.symbol]
                bef_tick = dic[tick.exchange]

                if datetime_bigger(tick.datetime, bef_tick.datetime):
                    dic[tick.exchange] = copy(tick)

    def produce_ticks(self):
        while self.active:
            if self.flag_produce_merge:
                self.produce_merge_ticks()
            if self.flag_produce_bbo:
                self.produce_bbo_ticks()
            time.sleep(1)

    def produce_bbo_ticks(self):
        b_ticks = {}  # bbo_ticks
        for exchange in self.exchanges_dict.keys():
            bbo = BBOTickData()
            bbo.exchange = exchange
            for vt_symbol, ticker in self.s_ticks.items():
                if vt_symbol.endswith(exchange):
                    if ticker.bid_prices[0] > 0:
                        bbo.symbol_dic[ticker.symbol] = {
                            "bid": [ticker.bid_prices[0], ticker.bid_volumes[0]],
                            "ask": [ticker.ask_prices[0], ticker.ask_volumes[0]]
                        }
            b_ticks[exchange] = copy(bbo)

        for exchange, bbo in b_ticks.items():
            e = Event(EVENT_BBO_TICK, copy(bbo))
            self.event_engine.put(e)
            self.process_record_bbo_tick(bbo)

    def produce_merge_ticks(self):
        for symbol in self.tot_array_ticks.keys():
            merge_tick = self.merge_ticks.get(symbol, None)
            if merge_tick is None:
                continue

            last_datetime = None
            dic = self.tot_array_ticks[symbol]

            to_sort_bids_arr = []  # (price,volume,exchange)
            to_sort_asks_arr = []  # (price,volume,exchange)
            for exchange in dic.keys():
                vt = dic[exchange]
                if not last_datetime or datetime_bigger(vt.datetime, last_datetime):
                    last_datetime = vt.datetime

                if vt.bid_prices[0] > 0:
                    ll = len(vt.bid_prices)
                    for i in range(ll):
                        if vt.bid_prices[i] > 0:
                            to_sort_bids_arr.append((vt.bid_prices[i], vt.bid_volumes[i], vt.exchange))

                        if vt.ask_prices[i] > 0:
                            to_sort_asks_arr.append((vt.ask_prices[i], vt.ask_volumes[i], vt.exchange))

            if len(to_sort_bids_arr) == 0:
                continue

            to_sort_bids_arr.sort(reverse=True)
            to_sort_asks_arr.sort(reverse=False)

            for i in range(min(MAX_PRICE_NUM, len(to_sort_bids_arr))):
                merge_tick.bids[i] = to_sort_bids_arr[i]

            for i in range(min(MAX_PRICE_NUM, len(to_sort_asks_arr))):
                merge_tick.asks[i] = to_sort_asks_arr[i]

            merge_tick.datetime = last_datetime
            self.merge_ticks[symbol] = merge_tick

            e = Event(EVENT_MERGE_TICK, copy(merge_tick))
            self.event_engine.put(e)

            self.process_record_merge_tick(merge_tick)

    def process_record_bbo_tick(self, bbo_ticker: BBOTickData):
        if self.flag_produce_bbo:
            sender = self.mq_bbo_sender_dict[bbo_ticker.exchange]
            sender.send("", bbo_ticker.get_mq_msg())

    def process_record_merge_tick(self, merge_tick: MergeTickData):
        if self.flag_produce_merge:
            sender = self.mq_aggr_sender_dict.get(merge_tick.vt_symbol, None)
            if sender is not None:
                sender.send("", merge_tick.get_mq_msg())

    def process_rest_tick_event(self, event):
        tick = event.data
        self.process_ticks(tick)

    def process_ws_tick_event(self, event):
        tick = event.data
        self.process_ticks(tick)
