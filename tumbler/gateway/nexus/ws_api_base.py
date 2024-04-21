# coding=utf-8

import json
from copy import copy
from datetime import datetime

from tumbler.api.websocket import WebsocketClient
from tumbler.service.log_service import log_service_manager
from tumbler.function import split_url, get_vt_key
from tumbler.object import TickData
from tumbler.constant import Exchange
from .base import nexus_format_symbol


class NexusApiBase(WebsocketClient):
    def __init__(self, gateway):
        super(NexusApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.app_id = ""
        self.key = ""
        self.symbol = ""
        self.secret = ""
        self.sign_host = ""
        self.path = ""
        self.url = ""
        self.ticker = None

    def connect(self, app_id, key, secret, symbol, url="", proxy_host="", proxy_port=0):
        self.app_id = app_id
        self.key = key
        self.secret = secret
        self.symbol = symbol
        if url:
            self.url = url

        self.url = self.url.format(self.app_id, nexus_format_symbol(self.symbol))

        tick = TickData()
        tick.symbol = symbol
        tick.name = symbol.replace('_', '/')
        tick.exchange = Exchange.NEXUS.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        self.ticker = copy(tick)

        host, path = split_url(self.url)
        self.sign_host = host
        self.path = path

        self.init(self.url, proxy_host, proxy_port)
        self.start()

    def login(self):
        """
        Need to login befores subscribe to websocket topic.
        """
        pass

    def on_login(self, packet):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_packet(self, packet):
        self.on_data(packet)

    def on_data(self, packet):
        log_service_manager.write_log("data : {}".format(packet))
