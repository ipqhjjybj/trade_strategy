# coding=utf-8

import time
import json

from tumbler.api.websocket import WebsocketClient
from tumbler.function import split_url
from tumbler.service.log_service import log_service_manager
from tumbler.gateway.okex.base import generate_signature


class Okex5WsApiBase(WebsocketClient):
    def __init__(self, gateway):
        super(Okex5WsApiBase, self).__init__()

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
                {
                    "apiKey": self.key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": signature
                }
            ]
        }
        self.send_packet(req)

    def on_login(self, packet):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_packet(self, packet):
        # self.gateway.write_log("[Okex5WsApiBase] on_packet:{}".format(packet))
        if "event" in packet:
            event = packet["event"]
            if event == "subscribe":
                self.gateway.write_log(packet)
            elif event == "error":
                self.gateway.write_log("Websocket API occur error!{}".format(packet))
            elif event == "login":
                self.on_login(packet)
            else:
                self.gateway.write_log("other event:{}".format(packet))
        else:
            self.on_data(packet)

    def on_data(self, packet):
        log_service_manager.write_log("data : {}".format(packet))
