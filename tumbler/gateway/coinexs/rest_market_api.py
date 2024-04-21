# coding=utf-8

import time
from datetime import datetime
from copy import copy
from threading import Thread

from tumbler.object import (
    TickData
)

from tumbler.constant import MAX_PRICE_NUM
from tumbler.constant import (
    Exchange
)

from tumbler.function import get_no_under_lower_symbol, get_vt_key
from tumbler.api.rest import RestClient
from tumbler.function import split_url, get_dt_use_timestamp, simplify_tick

from .base import REST_MARKET_HOST


class CoinexsRestMarketApi(RestClient):

    def __init__(self, gateway):
        super(CoinexsRestMarketApi, self).__init__()

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

    def connect(self, key, secret, proxy_host="", proxy_port=0):
        self.host, _ = split_url(REST_MARKET_HOST)
        self.init(REST_MARKET_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("CoinexsRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.COINEXS.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol, limit=10):
        self.add_request(
            method="GET",
            path="/v1/market/depth?market={}&limit={}&merge=0".format(
                get_no_under_lower_symbol(symbol).upper(), limit),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        if self.check_error(data, "query_depth"):
            return

        data = data["data"]
        symbol = request.extra

        dt = get_dt_use_timestamp(data['time'])
        tick = self.ticks[symbol]
        tick.datetime = dt
        tick.date = dt.strftime("%Y%m%d")
        tick.time = dt.strftime("%H:%M:%S")
        simplify_tick(tick, data["bids"], data["asks"])
        self.gateway.on_rest_tick(copy(tick))

    def check_error(self, data, func=""):
        if str(data["code"]) == "0":
            return False

        error_code = data["code"]
        error_msg = data["message"]

        self.gateway.write_log(
            "{} query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True
