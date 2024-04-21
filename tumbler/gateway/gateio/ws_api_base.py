# coding=utf-8

import json
import time
import random

from tumbler.function import split_url
from tumbler.api.websocket import WebsocketClient
from tumbler.service.log_service import log_service_manager

from .base import create_signature
from .base import WEBSOCKET_MARKET_HOST


class GateioWsApiBase(WebsocketClient):

    def __init__(self, gateway):
        super(GateioWsApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.url = ""
        self.key = ""
        self.secret = ""
        self.sign_host = ""
        self.path = ""

        self.login_use_id = 0

    def connect(self, key, secret, url=WEBSOCKET_MARKET_HOST, proxy_host="", proxy_port=0):
        self.key = key
        self.secret = secret
        if url:
            self.url = url

        host, path = split_url(self.url)
        self.sign_host = host
        self.path = path

        self.init(self.url, proxy_host, proxy_port)
        self.start()

    def login(self):
        self.login_use_id = random.randint(1, 7)
        params = {"id": self.login_use_id, "method": "server.sign"}
        nonce = int(time.time() * 1000)
        params["params"] = [self.key, create_signature(self.secret, str(nonce)), nonce]

        return self.send_packet(params)

    def on_login(self):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_packet(self, packet):
        if "ping" in packet:
            req = {"pong": packet["ping"]}
            self.send_packet(req)

        elif "error" in packet and packet["error"] is not None:
            return self.on_error_msg(packet)

        else:
            u_id = packet.get("id", -1)
            if u_id == self.login_use_id:
                self.on_login()
            else:
                self.on_data(packet)

    def on_data(self, packet):
        log_service_manager.write_log("data : {}".format(packet))

    def on_error_msg(self, packet):
        msg = packet.get("error", None)

        if msg is None:
            return

        self.gateway.write_log(packet["error"])