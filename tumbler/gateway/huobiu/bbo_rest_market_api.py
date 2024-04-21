# coding=utf-8

from copy import copy
import time
from datetime import datetime
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.function import split_url
from tumbler.constant import (
    Exchange,
)

from .base import REST_MARKET_HOST, parse_bbo_ticks


class HuobiuBBORestMarketApi(RestClient):
    """
    HUOBIU REST API
    """

    def __init__(self, gateway):
        super(HuobiuBBORestMarketApi, self).__init__()

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

        self.gateway.write_log("HuobiuBboRestMarketApi start success!")

    def query_bbo_depth(self):
        self.add_request(
            method="GET",
            path="/linear-swap-ex/market/detail/batch_merged",
            callback=self.on_query_bbo_depth
        )

    '''
        {
            "status":"ok",
            "ticks":[
                {
                    "id":1611109206,
                    "ts":1611109206797,
                    "ask":[
                        3,
                        15
                    ],
                    "bid":[
                        2.5,
                        1
                    ],
                    "contract_code":"EOS-USDT",
                    "open":"2.5",
                    "close":"2.5",
                    "low":"2.5",
                    "high":"2.5",
                    "amount":"0.4",
                    "count":2,
                    "vol":"4",
                    "trade_turnover":"1.1"
                }
            ],
            "ts":1611109206830
        }
        '''

    def on_query_bbo_depth(self, data, request):
        if self.check_error(data, "query_bbo_depth"):
            return

        self.gateway.on_rest_bbo_tick(parse_bbo_ticks(data, Exchange.HUOBIU.value))

    def check_error(self, data, func=""):
        if data["status"] != "error":
            return False

        error_code = data["err-code"]
        error_msg = data["err-msg"]

        self.gateway.write_log(
            "{}query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True
