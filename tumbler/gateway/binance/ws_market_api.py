# coding=utf-8

from copy import copy
from datetime import datetime
from tumbler.api.websocket import WebsocketClient
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData
)
from tumbler.object import MAX_PRICE_NUM
from tumbler.function import get_vt_key, get_dt_use_timestamp, simplify_tick
from .base import change_binance_format_to_system_format, change_system_format_to_binance_format
from .base import WEBSOCKET_DATA_HOST


class BinanceWsMarketApi(WebsocketClient):

    def __init__(self, gateway):
        super(BinanceWsMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.set_all_symbols = set([])

        self.api_key = ""
        self.secret_key = ""

        self.ticks = {}

    def connect(self, api_key, secret_key, proxy_host="", proxy_port=0):
        self.api_key = api_key
        self.secret_key = secret_key

        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

    def on_connected(self):
        self.gateway.write_log("BinanceWsMarketApi connected success!")

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)
        tick = TickData()
        tick.symbol = req.symbol
        tick.exchange = Exchange.BINANCE.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.name = req.symbol.replace('_','/')
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        self.ticks[req.symbol] = tick

        # Close previous connection
        if self._active:
            self.stop()
            self.join()

        # Create new connection
        channels = []
        for symbol in self.ticks.keys():
            ws_symbol = change_system_format_to_binance_format(symbol).lower()
            channels.append(ws_symbol + "@ticker")
            channels.append(ws_symbol + "@depth10")

        url = WEBSOCKET_DATA_HOST + "/".join(channels)
        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def on_packet(self, packet):
        stream = packet["stream"]
        data = packet["data"]

        ws_symbol, channel = stream.split("@")
        symbol = change_binance_format_to_system_format(ws_symbol)
        tick = self.ticks[symbol]

        if channel == "ticker":
            tick.volume = float(data['v'])
            tick.last_price = float(data['c'])
            tick.datetime = get_dt_use_timestamp(data['E'])
        else:
            simplify_tick(tick, data["bids"], data["asks"])

        if tick.last_price:
            self.gateway.on_ws_tick(copy(tick))
