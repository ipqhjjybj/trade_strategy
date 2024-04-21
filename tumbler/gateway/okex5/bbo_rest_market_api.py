# coding=utf-8

from copy import copy
import time
from datetime import datetime
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.service import log_service_manager
from tumbler.function import split_url
from tumbler.constant import (
    Exchange,
)

from .base import REST_MARKET_HOST, parse_bbo_ticks


class Okex5BBORestMarketApi(RestClient):
    """
    HUOBIU REST API
    """

    def __init__(self, gateway):
        super(Okex5BBORestMarketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.host = ""

        self.loop_interval = None
        self.active_loop = False
        self._loop_thread = None

        self.set_subscribe_instTypes = set([])

        self.start_timer_thread(1)

    def start_timer_thread(self, interval):
        self.loop_interval = interval
        self.active_loop = True
        self._loop_thread = Thread(target=self._run_loop_thread)
        self._loop_thread.start()

    def subscribe_okex5(self, inst_type):
        if inst_type in ["SPOT", "SWAP", "FUTURES", "OPTION"]:
            self.set_subscribe_instTypes.add(inst_type)
        else:
            self.gateway.write_log(f"[subscribe_okex5] instType:{inst_type} not found!")

    def _run_loop_thread(self):
        while self.active_loop:
            self.query_bbo_depth()

            time.sleep(self.loop_interval)

    def connect(self, key="", secret="", proxy_host="", proxy_port=0):
        self.host, _ = split_url(REST_MARKET_HOST)
        self.init(REST_MARKET_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("Okex5BboRestMarketApi start success!")

    def query_bbo_depth(self):
        for instType in self.set_subscribe_instTypes:
            self.add_request(
                method="GET",
                path=f"/api/v5/market/tickers?instType={instType}",
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

        self.gateway.on_rest_bbo_tick(parse_bbo_ticks(data, Exchange.OKEX5.value))

    def check_error(self, data, func=""):
        if str(data["code"]) == "0":
            return False
        else:
            error_code = data["code"]
            error_msg = data["msg"]
            self.gateway.write_log(
                "{}query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
            return True


