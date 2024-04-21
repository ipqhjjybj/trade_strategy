# coding=utf-8

from copy import copy
from datetime import datetime
from tumbler.function import get_vt_key, get_dt_use_timestamp, simplify_tick
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData,
    SubscribeRequest
)
from .ws_api_base import Okex5WsApiBase
from .base import WEBSOCKET_PUBLIC_HOST, okex5_format_symbol, okex5_format_to_system_format


class Okex5WsMarketApi(Okex5WsApiBase):
    """
    OKEX WS MARKET API
    """

    def __init__(self, gateway):
        super(Okex5WsMarketApi, self).__init__(gateway)

        self.set_all_symbols = set([])

        self.url = WEBSOCKET_PUBLIC_HOST
        self.ticks = {}
        self.callbacks = {}

    def on_connected(self):
        self.gateway.write_log("Okex5WsMarketApi connected success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.OKEX5.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def on_disconnected(self):
        self.gateway.write_log("Okex5WsMarketApi disconnected!")

    def subscribe(self, req):
        symbol = req.symbol
        self.set_all_symbols.add(symbol)

        # Create tick data buffer
        tick = TickData()
        tick.symbol = symbol
        tick.name = symbol.replace('_', '/')
        tick.exchange = Exchange.OKEX5.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        self.ticks[req.symbol] = tick

        self.callbacks["books5"] = self.on_depth

        req = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books5",
                    "instId": okex5_format_symbol(req.symbol)
                }
            ]
        }
        self.gateway.write_log("[Okex5WsMarketApi] send req:{}".format(req))
        self.send_packet(req)

    def on_data(self, packet):
        # self.gateway.write_log("[Okex5WsMarketApi] on data:{}".format(packet))
        if "arg" in packet.keys():
            channel = packet["arg"]["channel"]
            data = packet["data"]
            callback = self.callbacks.get(channel, None)
            if callback:
                for d in data:
                    callback(d, packet["arg"])

    def on_ticker(self, data, arg):
        pass

    def on_depth(self, data, arg):
        symbol = okex5_format_to_system_format(arg["instId"])
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        tick.datetime = get_dt_use_timestamp(data["ts"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])

        self.gateway.on_ws_tick(copy(tick))
