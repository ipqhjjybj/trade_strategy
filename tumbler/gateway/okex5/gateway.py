# coding=utf-8

from tumbler.gate import BaseGateway
from tumbler.event import (
    EVENT_TIMER,
)
from tumbler.constant import (
    Exchange,
    RunMode,
    OrderType
)

import tumbler.config as config
from tumbler.service import MQSender
from tumbler.apps.data_third_part.base import get_query_account_name
from tumbler.service import log_service_manager

from .rest_market_api import Okex5RestMarketApi
from .rest_trade_api import Okex5RestTradeApi
from .ws_trade_api import Okex5WsTradeApi
from .ws_market_api import Okex5WsMarketApi

from .base import REST_MARKET_HOST, WEBSOCKET_PUBLIC_HOST, REST_TRADE_HOST, WEBSOCKET_PRIVATE_HOST
from .base import OKEX5ModeType


class Okex5Gateway(BaseGateway):
    """
    Trader Gateway for OKEX connection.
    """
    default_setting = {
        "api_key": "",
        "secret_key": "",
        "passphrase": "",
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchanges = [Exchange.OKEX5.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(Okex5Gateway, self).__init__(event_engine, Exchange.OKEX5.value)

        self.query_wait_time = 6  # 表示6秒一次的进行轮训查order那些
        self.rest_market_api = None
        self.ws_market_api = None
        self.rest_trade_api = None
        self.ws_trade_api = None

        self.count = 0
        self.orders = {}

        self.sender = None

        self.account_name = ""

        self.run_mode = RunMode.NORMAL.value

    def connect(self, setting):
        key = setting["api_key"]
        secret = setting["secret_key"]
        passphrase = setting["passphrase"]
        mode_type = setting.get("mode_type", OKEX5ModeType.CROSS.value)
        ord_type = setting.get("ord_type", OrderType.LIMIT.value)

        proxy_host = setting.get("proxy_host", "")
        proxy_port = int(setting.get("proxy_port", False))

        rest_market = setting.get("rest_market", False)
        ws_market = setting.get("ws_market", True)
        rest_trade = setting.get("rest_trade", True)
        ws_trade = setting.get("ws_trade", True)

        self.run_mode = setting.get("mode", RunMode.NORMAL.value)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.account_name = setting["account_name"]
            self.sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                   user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                   exchange=get_query_account_name(self.account_name, Exchange.OKEX5.value))

        if rest_market:
            self.rest_market_api = Okex5RestMarketApi(self)
            self.rest_market_api.connect(REST_MARKET_HOST, proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = Okex5WsMarketApi(self)
            self.ws_market_api.connect(key, secret, passphrase, WEBSOCKET_PUBLIC_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = Okex5RestTradeApi(self)
            self.rest_trade_api.connect(key, secret, passphrase, REST_TRADE_HOST, mode_type, ord_type,
                                        proxy_host, proxy_port, run_mode=self.run_mode)

        if ws_trade:
            self.ws_trade_api = Okex5WsTradeApi(self)
            self.ws_trade_api.connect(key, secret, passphrase, WEBSOCKET_PRIVATE_HOST, proxy_host, proxy_port)

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
            vt_order_id, order = self.rest_trade_api.send_order(req)
            if self.run_mode in [RunMode.PUT_ORDER.value, RunMode.COVER.value]:
                if self.sender:
                    log_service_manager.write_log(
                        "send account_name:{} msg:{}".format(self.account_name, order.get_mq_msg()))
                    self.sender.send("", order.get_mq_msg())
            return vt_order_id

    def send_orders(self, reqs):
        if self.rest_trade_api:
            ret_orders = self.rest_trade_api.send_orders(reqs)
            if self.run_mode in [RunMode.PUT_ORDER.value, RunMode.COVER.value]:
                if self.sender:
                    for order in ret_orders:
                        log_service_manager.write_log(
                            "send account_name:{} msg:{}".format(self.account_name, order.get_mq_msg()))
                        self.sender.send("", order.get_mq_msg())
        return []

    def cancel_order(self, req):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_order(req)

    def query_account(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_account()

    def query_open_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_open_orders()

    def query_send_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_send_orders()

    def query_position(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_position()

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
        # if self.run_mode in [RunMode.NORMAL.value]:
        #     self.query_open_orders()
        self.query_send_orders()

    def process_sender_order(self, event):
        order = event.data
        self.orders[order.order_id] = order

    def init_query(self):
        if self.run_mode in [RunMode.NORMAL.value, RunMode.QUERY.value]:
            self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def on_order(self, order):
        self.orders[order.order_id] = order
        super().on_order(order)

        if not order.is_active():
            self.orders.pop(order.order_id)

    def get_order(self, order_id):
        return self.orders.get(order_id, None)
