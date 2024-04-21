# coding=utf-8

import time

from copy import copy
from datetime import datetime
from threading import Thread

from tumbler.function import split_url, get_vt_key, parse_timestamp, simplify_tick

from tumbler.api.rest import RestClient
from tumbler.constant import (
    Exchange,
)
from tumbler.object import (
    TickData
)


class OkexfRestMarketApi(RestClient):
    """
    OKEXF REST MARKET API
    """

    def __init__(self, gateway):
        super(OkexfRestMarketApi, self).__init__()

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

    def connect(self, key, secret, passphrase, url, proxy_host="", proxy_port=0):
        self.host, _ = split_url(url)
        self.init(url, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("OkexRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '-').upper()
            tick.exchange = Exchange.OKEXF.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol, limit=20):
        self.add_request(
            method="GET",
            path="/api/futures/v3/instruments/{}/book?size={}&depth=0.00000001".format(_okexf_format_symbol(symbol),
                                                                                       limit),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        symbol = request.extra

        tick = self.ticks[symbol]
        tick.datetime = parse_timestamp(data["timestamp"])
        tick.compute_date_and_time()

        simplify_tick(tick, data["bids"], data["asks"])
        self.gateway.on_rest_tick(copy(tick))
