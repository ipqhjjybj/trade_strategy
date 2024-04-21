# coding=utf-8

import time
from datetime import datetime
from copy import copy
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.constant import MAX_PRICE_NUM
from tumbler.function import get_vt_key

from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData
)
from tumbler.function import split_url, simplify_tick
from .base import REST_MARKET_HOST, change_system_format_to_gateio_format


class GateioRestMarketApi(RestClient):
    """
    GATEIO REST MARKET API
    """

    def __init__(self, gateway):
        super(GateioRestMarketApi, self).__init__()

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

    def connect(self, proxy_host="", proxy_port=0):
        self.host, _ = split_url(REST_MARKET_HOST)
        self.init(REST_MARKET_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("GateioRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.GATEIO.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol):
        self.add_request(
            method="GET",
            path="/api2/1/orderBook/{}".format(change_system_format_to_gateio_format(symbol)),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        if self.check_error(data, "query_depth"):
            return

        symbol = request.extra

        tick = self.ticks[symbol]
        tick.datetime = datetime.now()
        tick.compute_date_and_time()

        simplify_tick(tick, data["bids"], data["asks"])
        self.gateway.on_rest_tick(copy(tick))

    def check_error(self, data, func=""):
        if "bids" in data.keys() and "asks" in data.keys():
            return False

        error_code = "g0"
        error_msg = str(data)

        self.gateway.write_log(
            "{}query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True
