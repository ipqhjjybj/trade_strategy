# coding=utf-8

import time
import json
import zlib

from tumbler.api.websocket import WebsocketClient
from tumbler.function import split_url
from tumbler.service.log_service import log_service_manager
from .base import generate_signature


class OkexWsApiBase(WebsocketClient):

    def __init__(self, gateway):
        super(OkexWsApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.passphrase = ""
        self.sign_host = ""
        self.path = ""
        self.url = ""

    def connect(self, key, secret, passphrase, url="", proxy_host="", proxy_port=0):
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        if url:
            self.url = url

        host, path = split_url(self.url)
        self.sign_host = host
        self.path = path

        self.init(self.url, proxy_host, proxy_port)
        self.start()

    def login(self):
        """
        Need to login befores subscribe to websocket topic.
        """
        timestamp = str(time.time())

        msg = timestamp + 'GET' + '/users/self/verify'
        signature = generate_signature(msg, self.secret)

        req = {
            "op": "login",
            "args": [
                self.key,
                self.passphrase,
                timestamp,
                signature
            ]
        }
        self.send_packet(req)

    def on_login(self, packet):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(zlib.decompress(data, -zlib.MAX_WBITS))

    def on_packet(self, packet):
        if "event" in packet:
            event = packet["event"]
            if event == "subscribe":
                return
            elif event == "error":
                msg = packet["message"]
                self.gateway.write_log("Websocket API occur error!{}".format(msg))
            elif event == "login":
                self.on_login(packet)
        else:
            self.on_data(packet)

    def on_data(self, packet):
        log_service_manager.write_log("data : {}".format(packet))
