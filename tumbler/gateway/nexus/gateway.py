# coding=utf-8

from tumbler.gate import BaseGateway, LocalOrderManager
from tumbler.event import EVENT_TIMER
from tumbler.constant import Exchange

from .rest_market_api import NexusRestMarketApi
from .rest_trade_api import NexusRestTradeApi
from .ws_trade_api import NexusWsTradeApi

from .base import REST_MARKET_HOST, REST_TRADE_HOST, WEBSOCKET_TRADE_HOST


class NexusGateway(BaseGateway):

    def __init__(self, event_engine):
        """Constructor"""
        super(NexusGateway, self).__init__(event_engine, Exchange.NEXUS.value)

        self.order_manager = LocalOrderManager(self)
        self.query_wait_time = 20  # 表示6秒一次的进行轮训查order那些

        self.rest_market_api = None
        self.rest_trade_api = None
        self.ws_market_api = None
        self.ws_trade_api = None

        self.count = 0
        self.s_account = None

    def connect(self, setting):
        app_id = setting["app_id"]
        key = setting["api_key"]
        secret = setting["secret_key"]
        symbol = setting["symbol"]

        proxy_host = setting.get("proxy_host", "")
        proxy_port = int(setting.get("proxy_port", False))

        rest_market = setting.get("rest_market", False)
        rest_trade = setting.get("rest_trade", True)
        ws_trade = setting.get("ws_trade", True)

        if rest_market:
            self.rest_market_api = NexusRestMarketApi(self)
            self.rest_market_api.connect(REST_MARKET_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = NexusRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, REST_TRADE_HOST, proxy_host, proxy_port)

        if ws_trade:
            self.ws_trade_api = NexusWsTradeApi(self)
            self.ws_trade_api.connect(app_id, key, secret, symbol, WEBSOCKET_TRADE_HOST, proxy_host, proxy_port)

        self.init_query()

    def subscribe(self, req):
        if self.rest_market_api:
            self.rest_market_api.subscribe(req)
        if self.rest_trade_api:
            self.rest_trade_api.subscribe(req)

    def send_order(self, req):
        if self.rest_trade_api:
            return self.rest_trade_api.send_order(req)

    def cancel_order(self, req):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_order(req)

    def query_account(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_account()

    def query_open_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_open_orders()

    def transfer_amount(self, req):
        if self.rest_trade_api:
            return self.rest_trade_api.transfer_amount(req)

    def query_position(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_positions()

    def close(self):
        if self.rest_market_api:
            self.rest_market_api.stop()
        if self.rest_trade_api:
            self.rest_trade_api.stop()

    def on_position(self, position):
        # 好像用不到这个
        pass

    def process_timer_event(self, event):
        self.count += 1
        if self.count < self.query_wait_time:
            return

        self.count = 0
        self.query_account()
        #self.query_position()
        self.query_open_orders()

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
