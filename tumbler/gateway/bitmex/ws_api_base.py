# coding=utf-8

import hashlib
import hmac
import time
import json

from tumbler.api.websocket import WebsocketClient
from tumbler.service import log_service_manager


class BitmexWsApiBase(WebsocketClient):

    def __init__(self, gateway):
        super(BitmexWsApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.url = ""

    def connect(self, key, secret, url="", proxy_host="", proxy_port=0):
        self.key = key
        self.secret = secret
        if url:
            self.url = url

        self.init(self.url, proxy_host, proxy_port)
        self.start()

    def login(self):
        """
        Need to login befores subscribe to websocket topic.
        """
        expires = int(time.time())
        method = "GET"
        path = "/realtime"
        msg = method + path + str(expires)
        signature = hmac.new(self.secret.encode('utf-8'), msg.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()

        req = {"op": "authKey", "args": [self.key, expires, signature]}
        self.send_packet(req)

    def on_login(self, packet):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_packet(self, packet):
        if "error" in packet:
            self.gateway.write_log("Websocket API error：%s" % packet["error"])

            if "not valid" in packet["error"]:
                self.active = False

        elif "request" in packet:
            req = packet["request"]
            success = packet["success"]

            if success:
                if req["op"] == "authKey":
                    self.gateway.write_log("Websocket API auth successily!")
                    self.subscribe_topic()

        elif "table" in packet:
            self.on_data(packet)

    def subscribe_topic(self):
        pass

    def on_data(self, packet):
        log_service_manager.write_log("data : {}".format(packet))
