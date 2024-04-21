# coding=utf-8

import time
from copy import copy
from datetime import datetime

from tumbler.function import get_vt_key, parse_timestamp
from tumbler.constant import MAX_PRICE_NUM
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData,
    SubscribeRequest
)
from tumbler.function import simplify_tick
from .base import WEBSOCKET_MARKET_HOST, change_from_system_to_bitmex, change_from_bitmex_to_system
from .ws_api_base import BitmexWsApiBase


class BitmexWsMarketApi(BitmexWsApiBase):
    """
    BITMEX WS MARKET API
    """

    def __init__(self, gateway):
        super(BitmexWsMarketApi, self).__init__(gateway)

        self.set_all_symbols = set([])

        self.url = WEBSOCKET_MARKET_HOST
        self.ticks = {}
        self.callbacks = {
            "trade": self.on_tick,
            "orderBook10": self.on_depth
            # "orderBookL2_25": self.on_depth
        }

        self.bids_dict_all = {}
        self.asks_dict_all = {}

        self.before_push_time = {}

        self.flag_connected = False

    def on_connected(self):
        self.gateway.write_log("BitmexWsMarketApi connected success!")
        self.flag_connected = True

        self.subscribe_topic()

    def on_disconnected(self):
        self.gateway.write_log("BitmexWsMarketApi disconnected!")
        self.flag_connected = False

        self.ticks.clear()

    def subscribe(self, req):
        symbol = req.symbol
        self.gateway.write_log("[subscribe] symbol:{}".format(symbol))
        self.set_all_symbols.add(symbol)
        if self.flag_connected:
            if req.symbol not in self.ticks.keys():
                # Create tick data buffer
                tick = TickData()
                tick.symbol = symbol
                tick.name = symbol.replace('_', '/')
                tick.exchange = Exchange.BITMEX.value
                tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
                tick.datetime = datetime.now()
                tick.gateway_name = self.gateway_name

                self.ticks[req.symbol] = tick

                self.bids_dict_all[req.symbol] = {}
                self.asks_dict_all[req.symbol] = {}

                self.before_push_time[req.symbol] = time.time()

                bitmex_symbol = change_from_system_to_bitmex(symbol)
                req = {
                    "op": "subscribe",
                    "args": [
                        "trade:{}".format(bitmex_symbol),
                        "orderBook10:{}".format(bitmex_symbol)
                        # "orderBookL2_25:{}".format(bitmex_symbol)
                    ],
                }
                self.send_packet(req)

    def subscribe_topic(self):
        """
        Subscribe to all private topics.
        """
        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.BITMEX.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def on_data(self, packet):
        if "table" in packet:
            name = packet["table"]
            if name == "orderBookL2_25":
                action = packet["action"]
                # for d in packet["data"]:
                #     self.on_l2_depth(d, action)
            else:
                callback = self.callbacks.get(name, None)
                if callback:
                    if isinstance(packet["data"], list):
                        for d in packet["data"]:
                            callback(d)
                    else:
                        callback(packet["data"])

    def on_tick(self, d):
        symbol = change_from_bitmex_to_system(d["symbol"])
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        tick.last_price = d["price"]
        tick.datetime = parse_timestamp(d["timestamp"])

        if tick.bid_prices[0]:
            self.gateway.on_ws_tick(copy(tick))

    # def on_l2_depth(self, dic, action=None):
    #     now_time = time.time()
    #     symbol = change_from_bitmex_to_system(dic["symbol"])
    #     tick = self.ticks.get(symbol, None)
    #     bids_dict = self.bids_dict_all.get(symbol, {})
    #     asks_dict = self.asks_dict_all.get(symbol, {})
    #
    #     self.bids_dict_all[symbol] = bids_dict
    #     self.asks_dict_all[symbol] = asks_dict
    #
    #     before_time = self.before_push_time[symbol]
    #
    #     if not tick:
    #         return
    #
    #     method = action
    #     if method in ["partial"]:
    #         b_id = dic["id"]
    #         b_side = dic["side"]
    #         b_size = dic["size"]
    #         b_price = dic["price"]
    #
    #         if b_side == "Buy":
    #             bids_dict[b_id] = {"price": b_price, "size": b_size}
    #         else:
    #             asks_dict[b_id] = {"price": b_price, "size": b_size}
    #
    #     elif method == "update":
    #         b_id = dic["id"]
    #         b_side = dic["side"]
    #         b_size = dic["size"]
    #
    #         if b_side == "Buy":
    #             if b_id in bids_dict.keys():
    #                 bids_dict[b_id]["size"] = b_size
    #         else:
    #             if b_id in asks_dict.keys():
    #                 asks_dict[b_id]["size"] = b_size
    #
    #     elif method == "delete":
    #         b_id = dic["id"]
    #         b_side = dic["side"]
    #
    #         if b_side == "Buy":
    #             if b_id in bids_dict.keys():
    #                 del bids_dict[b_id]
    #         else:
    #             if b_id in asks_dict.keys():
    #                 del asks_dict[b_id]
    #
    #     elif method == "insert":
    #         b_id = dic["id"]
    #         b_side = dic["side"]
    #         b_size = dic["size"]
    #         b_price = dic["price"]
    #
    #         if b_side == "Buy":
    #             bids_dict[b_id] = {"price": b_price, "size": b_size}
    #         else:
    #             asks_dict[b_id] = {"price": b_price, "size": b_size}
    #
    #     bids = bids_dict.values()
    #     asks = asks_dict.values()
    #
    #     bids = [(d["price"], d["size"]) for d in bids]
    #     asks = [(d["price"], d["size"]) for d in asks]
    #
    #     bids.sort()
    #     asks.sort()
    #
    #     bids.reverse()
    #
    #     max_num = min(MAX_PRICE_NUM, len(bids))
    #     for n in range(max_num):
    #         price, volume = bids[n]
    #         tick.bid_prices[n] = float(price)
    #         tick.bid_volumes[n] = float(volume)
    #
    #     max_num = min(MAX_PRICE_NUM, len(asks))
    #     for n in range(max_num):
    #         price, volume = asks[n]
    #         tick.ask_prices[n] = float(price)
    #         tick.ask_volumes[n] = float(volume)
    #
    #     if tick.last_price:
    #         if now_time - before_time > 0.1:
    #             if tick.bid_prices[0] + 0.6 > tick.ask_prices[0] and tick.bid_prices[0] > 0.5:
    #                 self.gateway.on_ws_tick(copy(tick))
    #                 self.before_push_time[symbol] = now_time

    def on_depth(self, d):
        symbol = change_from_bitmex_to_system(d["symbol"])
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        simplify_tick(tick, d["bids"], d["asks"])
        self.gateway.on_ws_tick(copy(tick))
