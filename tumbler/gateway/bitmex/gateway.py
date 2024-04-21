# coding=utf-8

from tumbler.constant import (
    Exchange
)
from tumbler.event import (
    EVENT_TIMER,
)
from tumbler.gate import BaseGateway, LocalOrderManager

from .base import WEBSOCKET_MARKET_HOST, WEBSOCKET_TRADE_HOST, REST_MARKET_HOST
from .rest_market_api import BitmexRestMarketApi
from .rest_trade_api import BitmexRestTradeApi
from .ws_market_api import BitmexWsMarketApi
from .ws_trade_api import BitmexWsTradeApi


class BitmexGateway(BaseGateway):
    """
    Trader Gateway for BitMEX connection.
    """

    default_setting = {
        "api_key": "",
        "secret_key": ""
    }

    exchanges = [Exchange.BITMEX.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(BitmexGateway, self).__init__(event_engine, "BITMEX")

        self.order_manager = LocalOrderManager(self)

        self.rest_market_api = None
        self.rest_trade_api = None
        self.ws_market_api = None
        self.ws_trade_api = None

        self.count = 0
        self.query_wait_time = 6

    def connect(self, setting):
        key = setting["api_key"]
        secret = setting["secret_key"]

        proxy_host = setting.get("proxy_host", "")
        proxy_port = int(setting.get("proxy_port", False))

        rest_market = setting.get("rest_market", False)
        ws_market = setting.get("ws_market", True)
        rest_trade = setting.get("rest_trade", True)
        ws_trade = setting.get("ws_trade", True)

        if rest_market:
            self.rest_market_api = BitmexRestMarketApi(self)
            self.rest_market_api.connect(REST_MARKET_HOST, proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = BitmexWsMarketApi(self)
            self.ws_market_api.connect(key, secret, WEBSOCKET_MARKET_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = BitmexRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, proxy_host, proxy_port)

        if ws_trade:
            self.ws_trade_api = BitmexWsTradeApi(self)
            self.ws_trade_api.connect(key, secret, WEBSOCKET_TRADE_HOST, proxy_host, proxy_port)

        self.init_query()

    def subscribe(self, req):
        if self.rest_market_api:
            self.rest_market_api.subscribe(req)
        if self.rest_trade_api:
            self.rest_trade_api.subscribe(req)
        if self.ws_market_api:
            self.ws_market_api.subscribe(req)
        if self.ws_trade_api:
            self.ws_trade_api.subscribe(req)

    def send_order(self, req):
        if self.rest_trade_api:
            return self.rest_trade_api.send_order(req)

    def send_orders(self, reqs):
        self.write_log("[gateway send_orders] :{}".format(reqs))
        if self.rest_trade_api:
            return self.rest_trade_api.send_orders(reqs)
        return []

    def cancel_order(self, req):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_order(req)

    def cancel_orders(self, reqs):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_orders(reqs)

    def query_account(self):
        pass

    def query_position(self):
        pass

    def query_orders(self):
        if self.rest_trade_api:
            #self.rest_trade_api.query_open_orders()
            self.rest_trade_api.query_orders()

    def close(self):
        self.rest_market_api.stop()
        self.ws_market_api.stop()
        self.rest_trade_api.stop()
        self.ws_trade_api.stop()

    def process_timer_event(self, event):
        self.count += 1
        if self.count < self.query_wait_time:
            return

        self.query_orders()
        self.count = 0

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
