# coding=utf-8

import time
from copy import copy
from datetime import datetime

from tumbler.api.rest import RestClient
from threading import Thread
from tumbler.function import get_vt_key, get_dt_use_timestamp, split_url, simplify_tick

from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData
)

from .base import REST_MARKET_HOST, okex5_format_symbol


class Okex5RestMarketApi(RestClient):
    """
    OKEX5 REST MARKET API
    """
    def __init__(self, gateway):
        super(Okex5RestMarketApi, self).__init__()

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

        self.gateway.write_log("Okex5RestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_', '/')
            tick.exchange = Exchange.OKEX5.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)

    def query_depth(self, symbol, limit=20):
        self.add_request(
            method="GET",
            path="/api/v5/market/books?instId={}&sz={}".format(okex5_format_symbol(symbol), limit),
            callback=self.on_query_depth,
            extra=symbol
        )

    '''
    {"code":"0","msg":"",
    "data":[{"asks":[["38340.9","0.02","0","1"],["38344.4","0.0789923","0","2"],
    ["38344.7","0.11924878","0","2"],["38345.5","0.15392142","0","3"],
    ["38349.3","0.01328932","0","1"],["38350","0.00093433","0","1"],
    ["38351.8","0.01448773","0","1"],["38353.8","0.00846028","0","1"],
    ["38353.9","0.20320122","0","1"],["38354","0.08110077","0","1"],
    ["38356.4","0.08355845","0","1"],["38356.5","0.013","0","1"],
    ["38360.2","0.05758224","0","1"],["38360.3","0.07111111","0","1"],
    ["38360.7","0.00256974","0","1"],["38361.7","0.001","0","1"],
    ["38362.4","0.4","0","1"],["38362.5","0.000131","0","1"],
    ["38362.6","0.11055986","0","1"],["38366.5","0.001","0","1"]],
    "bids":[["38340.8","0.01864913","0","1"],["38337.6","0.23215957","0","1"],
    ["38336","0.01026032","0","1"],["38334","0.01323403","0","1"],
    ["38328.7","0.001","0","1"],["38325.9","0.001","0","1"],["38325.6","0.00005218","0","1"],
    ["38324.9","0.05758224","0","1"],["38324","0.001","0","1"],["38322.6","0.001","0","1"],
    ["38320.3","0.01","0","1"],["38320","0.01","0","1"],["38319.3","0.04992399","0","1"],
    ["38318.5","0.5207244","0","1"],["38318.2","0.001","0","1"],["38317.9","0.20707022","0","1"],
    ["38316.7","0.001","0","1"],["38314.7","0.31562226","0","1"],
    ["38310.6","0.11362135","0","1"],["38309.2","0.05351191","0","1"]],"ts":"1621480282230"}]}

    '''
    def on_query_depth(self, data, request):
        symbol = request.extra

        for dic in data["data"]:
            tick = self.ticks[symbol]
            tick.datetime = get_dt_use_timestamp(dic["ts"])
            tick.compute_date_and_time()
            simplify_tick(tick, dic["bids"], dic["asks"])

            self.gateway.on_rest_tick(copy(tick))
