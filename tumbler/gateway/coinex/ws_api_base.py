# coding=utf-8

import time
import json
import random

from tumbler.function import split_url
from tumbler.api.websocket import WebsocketClient
from tumbler.service.log_service import log_service_manager
from .base import create_signature


class CoinexWsApiBase(WebsocketClient):

    def __init__(self, gateway):
        super(CoinexWsApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.url = ""
        self.key = ""
        self.secret = ""
        self.sign_host = ""
        self.path = ""

        self.login_use_id = 0

        self.req_id = 0

    def connect(self, key, secret, url="", proxy_host="", proxy_port=0):
        self.key = key
        self.secret = secret
        if url:
            self.url = url

        host, path = split_url(self.url)
        self.sign_host = host
        self.path = path

        self.init(self.url, proxy_host, proxy_port)
        self.start()

    def get_nonce(self):
        return int(time.time() * 1000)

    def login(self):
        self.login_use_id = random.randint(1, 20)
        nonce = self.get_nonce()
        params = {
            "method": "server.sign",
            "params": [
                self.key,
                create_signature(self.secret, {"access_id": self.key, "tonce": nonce}),
                nonce
            ],
            "id": self.login_use_id
        }
        return self.send_packet(params)

    def on_login(self):
        log_service_manager.write_log("on_login")

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_packet(self, packet):
        u_id = packet.get("id", None)
        if u_id is not None and int(u_id) == self.login_use_id:
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
