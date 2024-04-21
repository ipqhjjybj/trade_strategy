# coding=utf-8

from copy import copy
from datetime import datetime
from tumbler.function import get_vt_key, parse_timestamp, simplify_tick
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData,
    SubscribeRequest
)
from .ws_api_base import OkexWsApiBase
from .base import WEBSOCKET_MARKET_HOST, okex_format_symbol, okex_format_to_system_format


class OkexWsMarketApi(OkexWsApiBase):
    """
    OKEX WS MARKET API
    """

    def __init__(self, gateway):
        super(OkexWsMarketApi, self).__init__(gateway)

        self.set_all_symbols = set([])

        self.url = WEBSOCKET_MARKET_HOST
        self.ticks = {}
        self.callbacks = {}

    def on_connected(self):
        self.gateway.write_log("OkexWsMarketApi connected success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.OKEX.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def on_disconnected(self):
        self.gateway.write_log("OkexWsMarketApi disconnected!")

    def subscribe(self, req):
        symbol = req.symbol
        self.set_all_symbols.add(symbol)

        # Create tick data buffer
        tick = TickData()
        tick.symbol = symbol
        tick.name = symbol.replace('_', '/')
        tick.exchange = Exchange.OKEX.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        self.ticks[req.symbol] = tick

        channel_ticker = "spot/ticker:{}".format(okex_format_symbol(req.symbol))
        channel_depth = "spot/depth5:{}".format(okex_format_symbol(req.symbol))

        self.callbacks["spot/ticker"] = self.on_ticker
        self.callbacks["spot/depth5"] = self.on_depth

        req = {
            "op": "subscribe",
            "args": [channel_ticker, channel_depth]
        }
        self.send_packet(req)

    def on_data(self, packet):
        channel = packet["table"]
        data = packet["data"]
        callback = self.callbacks.get(channel, None)
        if callback:
            for d in data:
                callback(d)

    def on_ticker(self, data):
        pass
        # symbol = okex_format_to_system_format(data["instrument_id"])
        # tick = self.ticks.get(symbol, None)
        # if not tick:
        #     return
        #
        # tick.last_price = float(data["last"])
        # tick.volume = float(data["base_volume_24h"])
        # tick.datetime = parse_timestamp(data["timestamp"])
        #
        # if tick.bid_prices[0]:
        #     self.gateway.on_ws_tick(copy(tick))

    def on_depth(self, data):
        symbol = okex_format_to_system_format(data["instrument_id"])
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        tick.datetime = parse_timestamp(data["timestamp"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["bids"], data["asks"])

        self.gateway.on_ws_tick(copy(tick))
        '''
        bef_time = tick.datetime
        now_time = parse_timestamp(data["timestamp"])
        if (now_time.replace(tzinfo=None) - bef_time.replace(tzinfo=None)).total_seconds() >= 1:
            tick.datetime = now_time
            tick.compute_date_and_time()
            simplify_tick(tick, data["bids"], data["asks"])

            self.gateway.on_ws_tick(copy(tick))
        '''
