# coding=utf-8

import time
from copy import copy
from datetime import datetime

from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.function import split_url, get_vt_key
from tumbler.constant import MAX_PRICE_NUM
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData
)

from .base import REST_MARKET_HOST, change_from_system_to_bitmex, parse_ticker


class BitmexRestMarketApi(RestClient):
    """
    BITMEX REST Market API
    """

    def __init__(self, gateway):
        super(BitmexRestMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.ticks = {}

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
        if not url:
            url = REST_MARKET_HOST

        self.host, _ = split_url(url)
        self.init(url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BitmexRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.BITMEX.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol, depth=20):
        bitmex_symbol = change_from_system_to_bitmex(symbol)
        self.add_request(
            method="GET",
            path="/api/v1/orderBook/L2?symbol={}&depth={}".format(bitmex_symbol, depth),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        symbol = request.extra

        tick = self.ticks[symbol]
        tick = parse_ticker(tick, data)

        self.gateway.on_rest_tick(copy(tick))
