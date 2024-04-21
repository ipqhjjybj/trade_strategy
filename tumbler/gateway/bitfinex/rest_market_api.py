# coding=utf-8

import time
from copy import copy
from datetime import datetime
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.function import split_url, get_vt_key, simplify_tick
from tumbler.object import MAX_PRICE_NUM

from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData
)

from .base import _bitfinex_format_symbol

REST_MARKET_HOST = "https://api-pub.bitfinex.com"
REST_TRADE_HOST = "https://api.bitfinex.com"

WS_MARKET_HOST = "wss://api-pub.bitfinex.com/ws/2"
WS_TRADE_HOST = "wss://api-pub.bitfinex.com/ws/2"


class BitfinexRestMarketApi(RestClient):

    def __init__(self, gateway):
        super(BitfinexRestMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.ticks = {}

        self.url = REST_MARKET_HOST
        self.host = ""

        self.all_symbols_set = set([])

        self.loop_interval = None
        self.active_loop = False
        self._loop_thread = None

        self.start_timer_thread(1)

    def start_timer_thread(self, interval):
        self.loop_interval = interval
        self.active_loop = True
        self._loop_thread = Thread(target=self._run_loop_thread)
        self._loop_thread.start()

    def _run_loop_thread(self):
        while self.active_loop:
            for symbol in self.all_symbols_set:
                self.query_depth(symbol)

            time.sleep(self.loop_interval)

    def connect(self, url="", proxy_host="", proxy_port=0):
        if url:
            self.url = url

        self.host, _ = split_url(self.url)
        self.init(self.url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BitfinexRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.BITFINEX.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol, limit=20):
        self.add_request(
            method="GET",
            path="/v2/book/{}/P0".format(_bitfinex_format_symbol(symbol), limit),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        symbol = request.extra

        tick = self.ticks[symbol]
        tick.datetime = datetime.now()
        tick.date = tick.datetime.strftime("%Y%m%d")
        tick.time = tick.datetime.strftime("%H:%M:%S")

        bids = []
        asks = []
        for price, tick_num, volume in data:
            if volume > 0:
                bids.append([price, volume])
            elif volume < 0:
                asks.append([price, abs(volume)])

        simplify_tick(tick, bids, asks)
        self.gateway.on_rest_tick(copy(tick))
