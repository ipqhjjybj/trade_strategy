# coding=utf-8

from tumbler.gate import BaseGateway, LocalOrderManager
from tumbler.constant import (
    Exchange,
)
from tumbler.event import EVENT_TIMER
from .rest_market_api import BittrexRestMarketApi
from .rest_trade_api import BittrexRestTradeApi


class BittrexGateway(BaseGateway):
    """
    Trader Gateway for Bittrex conection
    """
    default_setting = {
        "api_key": "",
        "secret_key": ""
    }

    def __init__(self, event_engine):
        """Constructor"""
        super(BittrexGateway, self).__init__(event_engine, Exchange.BITTREX.value)

        self.order_manager = LocalOrderManager(self)
        self.query_wait_time = 6            # 表示6秒一次的进行轮训查order那些

        self.rest_market_api = None
        self.rest_trade_api = None

        self.count = 0

    def connect(self, setting):
        key = setting.get("api_key")
        secret = setting["secret_key"]
        proxy_host = setting.get("proxy_host", "")
        proxy_port = int(setting.get("proxy_port", False))

        rest_market = setting.get("rest_market", False)
        rest_trade = setting.get("rest_trade", True)

        if rest_market:
            self.rest_market_api = BittrexRestMarketApi(self)
            self.rest_market_api.connect(proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = BittrexRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, proxy_host, proxy_port)

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

    def query_position(self):
        pass

    def query_open_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_open_orders()

    def close(self):
        if self.rest_market_api:
            self.rest_market_api.stop()
        if self.rest_trade_api:
            self.rest_trade_api.stop()

    def process_timer_event(self, event):
        self.count += 1
        if self.count < self.query_wait_time:
            return
        self.count = 0
        self.query_account()
        self.query_open_orders()

    def init_query(self):
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)