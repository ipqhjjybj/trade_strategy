# coding=utf-8


from tumbler.event import EVENT_TIMER
from tumbler.constant import (
    Exchange
)
from tumbler.gate import BaseGateway

from .rest_market_api import OkexsRestMarketApi
from .rest_trade_api import OkexsRestTradeApi
from .ws_market_api import OkexsWsMarketApi
from .ws_trade_api import OkexsWsTradeApi
from .base import REST_MARKET_HOST, REST_TRADE_HOST, WEBSOCKET_MARKET_HOST, WEBSOCKET_TRADE_HOST


class OkexsGateway(BaseGateway):
    """
    Trader Gateway for OKEX connection.
    """

    default_setting = {
        "api_key": "",
        "secret_key": "",
        "passphrase": ""
    }

    exchanges = [Exchange.OKEXS.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(OkexsGateway, self).__init__(event_engine, Exchange.OKEXS.value)

        self.query_wait_time = 6  # 表示6秒一次的进行轮训查order那些
        self.query_position_wait_time = 300  # 5分钟查一次持仓

        self.rest_market_api = None
        self.ws_market_api = None
        self.rest_trade_api = None
        self.ws_trade_api = None

        self.count = 0
        self.count_position = 0
        self.orders = {}

    def connect(self, setting):
        key = setting["api_key"]
        secret = setting["secret_key"]
        passphrase = setting["passphrase"]
        proxy_host = setting.get("proxy_host", "")
        proxy_port = int(setting.get("proxy_port", 0))

        rest_market = setting.get("rest_market", False)
        ws_market = setting.get("ws_market", True)
        rest_trade = setting.get("rest_trade", True)
        ws_trade = setting.get("ws_trade", True)

        if rest_market:
            self.rest_market_api = OkexsRestMarketApi(self)
            self.rest_market_api.connect(key, secret, passphrase, REST_MARKET_HOST, proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = OkexsWsMarketApi(self)
            self.ws_market_api.connect(key, secret, passphrase, WEBSOCKET_MARKET_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = OkexsRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, passphrase, REST_TRADE_HOST, proxy_host, proxy_port)

        if ws_trade:
            self.ws_trade_api = OkexsWsTradeApi(self)
            self.ws_trade_api.connect(key, secret, passphrase, WEBSOCKET_TRADE_HOST, proxy_host, proxy_port)

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
        pass

    def query_position(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_position()

    def query_open_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_open_orders()

    def close(self):
        if self.rest_market_api:
            self.rest_market_api.stop()
        if self.ws_market_api:
            self.ws_market_api.stop()
        if self.rest_trade_api:
            self.rest_trade_api.stop()
        if self.ws_trade_api:
            self.ws_trade_api.stop()

    def process_timer_event(self, event):
        self.count_position += 1
        self.count += 1
        if self.count_position % self.query_position_wait_time == 0:
            self.query_position()

        if self.count < self.query_wait_time:
            return

        self.count = 0
        self.query_account()
        self.query_open_orders()

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def on_order(self, order):
        self.orders[order.order_id] = order
        super(OkexsGateway, self).on_order(order)

    def get_order(self, order_id):
        return self.orders.get(order_id, None)

    def get_active_orders(self):
        return self.orders
