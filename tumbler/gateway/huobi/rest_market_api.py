# coding=utf-8

from copy import copy
import time
from datetime import datetime
from threading import Thread

from tumbler.api.rest import RestClient
from tumbler.constant import MAX_PRICE_NUM
from tumbler.function import get_vt_key, get_format_lower_symbol, get_no_under_lower_symbol, split_url
from tumbler.function import get_dt_use_timestamp, simplify_tick
from tumbler.constant import (
    Exchange,
)

from tumbler.object import (
    TickData,
)
from .base import REST_MARKET_HOST


class HuobiRestMarketApi(RestClient):
    """
    HUOBI REST API
    """

    def __init__(self, gateway):
        super(HuobiRestMarketApi, self).__init__()

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

    def connect(self, key="", secret="", proxy_host="", proxy_port=0):
        self.host, _ = split_url(REST_MARKET_HOST)
        self.init(REST_MARKET_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("HuobiRestMarketApi start success!")

    def subscribe(self, req):
        symbol = req.symbol
        if symbol not in self.all_symbols_set:
            tick = TickData()
            tick.symbol = symbol
            tick.name = symbol.replace('_','/')
            tick.exchange = Exchange.HUOBI.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[symbol] = tick

            self.all_symbols_set.add(symbol)
            self.all_request_list.append(req)

    def query_depth(self, symbol, depth=20, u_type="step0"):
        self.add_request(
            method="GET",
            path="/market/depth?symbol={}&type={}&depth={}".format(get_no_under_lower_symbol(symbol), str(u_type),
                                                                   str(depth)),
            callback=self.on_query_depth
        )

    def on_query_depth(self, data, request):
        if self.check_error(data, "query_depth"):
            return

        symbol = data["ch"].split(".")[1]
        symbol = get_format_lower_symbol(symbol)

        tick = self.ticks[symbol]
        tick.datetime = get_dt_use_timestamp(data["ts"])
        tick.compute_date_and_time()
        simplify_tick(tick, data["tick"]["bids"], data["tick"]["asks"])

        self.gateway.on_rest_tick(copy(tick))

    def check_error(self, data, func=""):
        if data["status"] != "error":
            return False

        error_code = data["err-code"]
        error_msg = data["err-msg"]

        self.gateway.write_log(
            "{}query_failed, code:{},information:{}".format(str(func), str(error_code), str(error_msg)))
        return True

