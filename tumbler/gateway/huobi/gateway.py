# coding=utf-8

from tumbler.gate import BaseGateway, LocalOrderManager
from tumbler.event import (
    EVENT_TIMER,
)

from tumbler.constant import (
    RunMode,
    Exchange
)
import tumbler.config as config
from tumbler.service import MQSender, log_service_manager
from tumbler.apps.data_third_part.base import get_query_account_name

from .base import WEBSOCKET_MARKET_HOST, WEBSOCKET_TRADE_HOST
from .rest_market_api import HuobiRestMarketApi
from .rest_trade_api import HuobiRestTradeApi
from .ws_market_api import HuobiWsMarketApi
from .ws_trade_api import HuobiWsTradeApi


class HuobiGateway(BaseGateway):
    """
    VN Trader Gateway for Huobi connection.
    """

    default_setting = {
        "api_key": "",
        "secret_key": "",
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchanges = [Exchange.HUOBI.value]

    def __init__(self, event_engine):
        """Constructor"""
        super(HuobiGateway, self).__init__(event_engine, Exchange.HUOBI.value)

        self.order_manager = LocalOrderManager(self)

        self.query_wait_time = 6            # 表示6秒一次的进行轮训查order那些

        self.rest_market_api = None
        self.ws_market_api = None
        self.rest_trade_api = None
        self.ws_trade_api = None

        self.count = 0

        self.sender = None
        self.account_name = ""
        self.run_mode = RunMode.NORMAL.value

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
        ws_market_trade = setting.get("trade_detail", False)

        self.run_mode = setting.get("mode", RunMode.NORMAL.value)
        if self.run_mode in [RunMode.COVER.value, RunMode.PUT_ORDER.value]:
            self.account_name = setting["account_name"]
            self.sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                   user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                   exchange=get_query_account_name(self.account_name, Exchange.OKEX.value))
        if rest_market:
            self.rest_market_api = HuobiRestMarketApi(self)
            self.rest_market_api.connect(key, secret, proxy_host, proxy_port)

        if ws_market:
            self.ws_market_api = HuobiWsMarketApi(self, ws_market_trade)
            self.ws_market_api.connect(key, secret, WEBSOCKET_MARKET_HOST, proxy_host, proxy_port)

        if rest_trade:
            self.rest_trade_api = HuobiRestTradeApi(self)
            self.rest_trade_api.connect(key, secret, proxy_host, proxy_port, run_mode=self.run_mode)

        if ws_trade:
            self.ws_trade_api = HuobiWsTradeApi(self)
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

    def cancel_orders(self, reqs):
        if self.rest_trade_api:
            self.rest_trade_api.cancel_orders(reqs)

    def query_account(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_account_balance()

    def query_open_orders(self):
        if self.rest_trade_api:
            self.rest_trade_api.query_open_orders()
            self.rest_trade_api.query_complete_orders()

    def query_position(self):
        pass

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


