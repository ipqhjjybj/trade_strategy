# coding=utf-8

from tumbler.gate import BaseGateway, LocalOrderManager
from tumbler.event import (
    EVENT_TIMER,
)
from tumbler.constant import (
    Exchange
)

from .rest_market_api import CoinexRestMarketApi
from .rest_trade_api import CoinexRestTradeApi
from .ws_market_api import CoinexWsMarketApi
from .ws_trade_api import CoinexWsTradeApi


class CoinexGateway(BaseGateway):
    """
    Trader Gateway for COINEX connection
    """

    default_setting = {
        "api_key": "",
        "secret_key": "",
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchange = [Exchange.COINEX.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(CoinexGateway, self).__init__(event_engine, Exchange.COINEX.value)

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
            self.rest_market_api = CoinexRestMarketApi(self)
            self.rest_market_api.connect(key, secret, proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = CoinexWsMarketApi(self)
            self.ws_market_api.connect(key, secret, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = CoinexRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, proxy_host, proxy_port)

        if ws_trade:
            self.ws_trade_api = CoinexWsTradeApi(self)
            self.ws_trade_api.connect(key, secret, proxy_host, proxy_port)

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
        else:
            return None

    def cancel_order(self, req):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_order(req)
        else:
            return None

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
        # self.query_account()
        # self.query_open_orders()

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
