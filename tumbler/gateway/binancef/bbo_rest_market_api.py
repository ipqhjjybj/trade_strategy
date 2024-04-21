# coding=utf-8

import time
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.function import split_url
from tumbler.constant import (
    Exchange,
)

from .base import REST_MARKET_HOST, parse_bbo_ticks


class BinancefBBORestMarketApi(RestClient):
    """
    HUOBIU REST API
    """

    def __init__(self, gateway):
        super(BinancefBBORestMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.host = ""

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
            self.query_bbo_depth()

            time.sleep(self.loop_interval)

    def connect(self, key="", secret="", proxy_host="", proxy_port=0):
        self.host, _ = split_url(REST_MARKET_HOST)
        self.init(REST_MARKET_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BinancefBBORestMarketApi start success!")

    def query_bbo_depth(self):
        self.add_request(
            method="GET",
            path="/fapi/v1/ticker/bookTicker",
            callback=self.on_query_bbo_depth
        )

    '''
    [
        {
            "symbol": "BTCUSDT", // 交易对
            "bidPrice": "4.00000000", //最优买单价
            "bidQty": "431.00000000", //挂单量
            "askPrice": "4.00000200", //最优卖单价
            "askQty": "9.00000000", //挂单量
            "time": 1589437530011   // 撮合引擎时间
        }
    ]
    '''

    def on_query_bbo_depth(self, data, request):
        self.gateway.on_rest_bbo_tick(parse_bbo_ticks(data, Exchange.BINANCEF.value))
