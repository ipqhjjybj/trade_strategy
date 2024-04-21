# coding=utf-8

import time
from copy import copy
from datetime import datetime
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData
)
from tumbler.function import get_vt_key, split_url, simplify_tick
from .base import REST_MARKET_HOST, change_system_format_to_binance_format


class BinanceRestMarketApi(RestClient):
    """
    BINANCE REST API
    """

    def __init__(self, gateway):
        super(BinanceRestMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.host = ""

        self.ticks = {}

        self.all_symbols_set = set([])
        self.all_request_list = []

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
            for req in self.all_request_list:
                self.query_depth(req.symbol)

            time.sleep(self.loop_interval)

    def connect(self, key, secret, proxy_host="", proxy_port=0):
        self.host, _ = split_url(REST_MARKET_HOST)
        self.init(REST_MARKET_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BinanceRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.BINANCE.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)
            self.all_request_list.append(req)

    def query_depth(self, symbol, depth=10):
        self.add_request(
            method="GET",
            path="/api/v1/depth?symbol={}&limit={}".format(change_system_format_to_binance_format(symbol), str(depth)),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        symbol = request.extra

        tick = self.ticks[symbol]
        tick.datetime = datetime.now()
        simplify_tick(tick, data["bids"], data["asks"])

        self.gateway.on_rest_tick(copy(tick))
