# coding=utf-8


from copy import copy
import time
from threading import Thread
from datetime import datetime

from tumbler.function import split_url, get_dt_use_timestamp
from tumbler.function import simplify_tick
from tumbler.api.rest import RestClient
from tumbler.constant import Exchange
from tumbler.object import TickData
from tumbler.function import get_vt_key

from .base import REST_MARKET_HOST
from .base import nexus_format_symbol
from .base import sign_request


class NexusRestMarketApi(RestClient):

    def __init__(self, gateway):
        super(NexusRestMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.ticks = {}

        self.host = ""

        self.all_symbols_set = set([])

        self.loop_interval = None
        self.active_loop = False
        self._loop_thread = None

        self.start_timer_thread(1)

    def sign(self, request):
        return sign_request(request)

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

        self.gateway.write_log("OkexRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.NEXUS.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol):
        self.add_request(
            method="GET",
            path="/v1/orderbook/{}".format(nexus_format_symbol(symbol)),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        symbol = request.extra

        tick = self.ticks[symbol]
        tick.datetime = get_dt_use_timestamp(data["timestamp"], 1000)
        tick.compute_date_and_time()

        asks = data["asks"]
        bids = data["bids"]

        asks = [(x["price"], x["quantity"]) for x in asks]
        bids = [(x["price"], x["quantity"]) for x in bids]

        simplify_tick(tick, bids, asks)
        self.gateway.on_rest_tick(copy(tick))
