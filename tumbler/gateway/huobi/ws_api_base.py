# coding=utf-8

import json
import zlib

from tumbler.api.websocket import WebsocketClient
from tumbler.function import split_url
from tumbler.service.log_service import log_service_manager

from .base import create_signature


class HuobiWsApiBase(WebsocketClient):

    def __init__(self, gateway):
        super(HuobiWsApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.url = ""
        self.key = ""
        self.secret = ""
        self.sign_host = ""
        self.path = ""

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

    def login(self):
        params = {"op": "auth"}
        params.update(create_signature(self.key, "GET", self.sign_host, self.path, self.secret))
        return self.send_packet(params)

    def on_login(self):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(zlib.decompress(data, 31))

    def on_packet(self, packet):
        if "ping" in packet:
            req = {"pong": packet["ping"]}
            self.send_packet(req)
        elif "op" in packet and packet["op"] == "ping":
            req = {
                "op": "pong",
                "ts": packet["ts"]
            }
            self.send_packet(req)
        elif "err-msg" in packet:
            return self.on_error_msg(packet)
        elif "op" in packet and packet["op"] == "auth":
            return self.on_login()
        else:
            self.on_data(packet)

    def on_data(self, packet):
        log_service_manager.write_log("data : {}".format(packet))

    def on_error_msg(self, packet):
        msg = packet["err-msg"]
        if msg == "invalid pong":
            return

        self.gateway.write_log(packet["err-msg"])