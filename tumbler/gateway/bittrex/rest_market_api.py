# coding=utf-8

import time
from copy import copy
from datetime import datetime
from threading import Thread
from tumbler.function import split_url, get_vt_key, simplify_tick
from tumbler.constant import MAX_PRICE_NUM
from tumbler.api.rest import RestClient
from tumbler.object import (
    TickData
)
from tumbler.constant import (
    Exchange
)
from .base import REST_MARKET_HOST, _bittrex_format_symbol


class BittrexRestMarketApi(RestClient):
    """
    REST MARKET API
    """

    def __init__(self, gateway):
        super(BittrexRestMarketApi, self).__init__()

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

        self.gateway.write_log("BittrexRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.BITTREX.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol, limit=20):
        self.add_request(
            method="GET",
            path="/api/v1.1/public/getorderbook?market={}&type=both&limit={}".format(_bittrex_format_symbol(symbol),
                                                                                     limit),
            callback=self.on_query_depth,
            extra=symbol
        )

    def on_query_depth(self, data, request):
        if self.check_error(data, "query_depth"):
            return

        symbol = request.extra

        tick = self.ticks[symbol]
        tick.datetime = datetime.now()
        tick.date = tick.datetime.strftime("%Y%m%d")
        tick.time = tick.datetime.strftime("%H:%M:%S")

        bids = data["result"]["buy"]
        asks = data["result"]["sell"]

        bids = [(float(dic["Rate"]), float(dic["Quantity"])) for dic in bids]
        asks = [(float(dic["Rate"]), float(dic["Quantity"])) for dic in asks]

        simplify_tick(tick, bids, asks)
        self.gateway.on_rest_tick(copy(tick))

    def check_error(self, data, func=""):
        if data["success"] is True:
            return False

        error_code = "b0"
        error_msg = data.get("message", str(data))

        self.gateway.write_log(
            "{}query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True