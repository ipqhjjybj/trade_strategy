# coding=utf-8

from tumbler.gate import BaseGateway, LocalOrderManager
from tumbler.event import (
    EVENT_TIMER,
)

from tumbler.constant import (
    Exchange
)

from .base import WEBSOCKET_MARKET_HOST, WEBSOCKET_TRADE_HOST
from .rest_market_api import HuobiuRestMarketApi
from .rest_trade_api import HuobiuRestTradeApi
from .ws_market_api import HuobiuWsMarketApi
from .ws_trade_api import HuobiuWsTradeApi


class HuobiuGateway(BaseGateway):
    """
    Gateway for Huobiu connection.
    """

    default_setting = {
        "api_key": "",
        "secret_key": "",
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchanges = [Exchange.HUOBIU.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(HuobiuGateway, self).__init__(event_engine, Exchange.HUOBIU.value)

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
        proxy_port = setting.get("proxy_port", 0)

        if str(proxy_port).isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

        rest_market = setting.get("rest_market", False)
        ws_market = setting.get("ws_market", True)
        rest_trade = setting.get("rest_trade", True)
        ws_trade = setting.get("ws_trade", True)
        ori_market_msg = setting.get("ori_market_msg", False)
        ws_market_trade = setting.get("trade_detail", False)

        if rest_market:
            self.rest_market_api = HuobiuRestMarketApi(self)
            self.rest_market_api.connect(key, secret, proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = HuobiuWsMarketApi(self, ws_market_trade, ori_market_msg)
            self.ws_market_api.connect(key, secret, WEBSOCKET_MARKET_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = HuobiuRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, proxy_host, proxy_port)

        if ws_trade:
            self.ws_trade_api = HuobiuWsTradeApi(self)
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
        if self.rest_trade_api:
            self.rest_trade_api.query_account()

    def query_position(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_position()

    def query_open_orders(self):
        if self.rest_trade_api:
            #self.rest_trade_api.query_open_orders()
            #self.rest_trade_api.query_complete_orders()
            self.rest_trade_api.query_orders()

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
        self.query_position()
        self.query_open_orders()

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

