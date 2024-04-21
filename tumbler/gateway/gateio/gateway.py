# coding=utf-8

from tumbler.gate import BaseGateway, LocalOrderManager
from tumbler.event import (
    EVENT_TIMER,
)
from tumbler.constant import (
    Exchange
)
from .rest_trade_api import GateioRestTradeApi
from .rest_market_api import GateioRestMarketApi
from .ws_market_api import GateioWsMarketApi
from .ws_trade_api import GateioWsTradeApi
from .base import WEBSOCKET_MARKET_HOST, WEBSOCKET_TRADE_HOST


class GateioGateway(BaseGateway):
    """
    Trader Gateway for GATEIO connection
    """

    default_setting = {
        "api_key": "",
        "secret_key": "",
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchange = [Exchange.GATEIO.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(GateioGateway, self).__init__(event_engine, Exchange.GATEIO.value)

        self.order_manager = LocalOrderManager(self)
        self.query_wait_time = 6            # 表示6秒一次的进行轮训查order那些

        self.rest_market_api = None
        self.ws_market_api = None
        self.rest_trade_api = None
        self.ws_trade_api = None

        self.count = 0

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
            self.rest_market_api = GateioRestMarketApi(self)
            self.rest_market_api.connect(proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = GateioWsMarketApi(self)
            self.ws_market_api.connect(key, secret, WEBSOCKET_MARKET_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = GateioRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, proxy_host, proxy_port)

        if ws_trade:
            self.ws_trade_api = GateioWsTradeApi(self)
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

    def cancel_order(self, req):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_order(req)

    def query_account(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_account()

    def query_position(self):
        pass

    def query_open_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_open_orders()

    def close(self):
        if self.rest_trade_api:
            self.rest_trade_api.stop()
        if self.ws_trade_api:
            self.ws_trade_api.stop()
        if self.ws_market_api:
            self.ws_market_api.stop()

    def process_timer_event(self, event):
        self.count += 1
        if self.count < self.query_wait_time:
            return
        self.count = 0
        self.query_account()
        self.query_open_orders()

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

