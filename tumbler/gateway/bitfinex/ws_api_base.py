# coding=utf-8

import time
import json
import hmac
import hashlib

from tumbler.api.websocket import WebsocketClient
from tumbler.function import split_url, get_format_lower_symbol


class BitfinexWsApiBase(WebsocketClient):

    def __init__(self, gateway):
        super(BitfinexWsApiBase, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.url = ""
        self.key = ""
        self.secret = ""
        self.sign_host = ""
        self.path = ""

        self.channels = {}

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
        """
        Need to login befores subscribe to websocket topic.
        """
        nonce = int(time.time() * 1000000)
        auth_payload = "AUTH" + str(nonce)
        signature = hmac.new(self.secret.encode('utf-8'), auth_payload.encode('utf-8'),
                             digestmod=hashlib.sha384).hexdigest()

        req = {
            "apiKey": self.key,
            "event": "auth",
            "authPayload": auth_payload,
            "authNonce": nonce,
            "authSig": signature
        }

        self.send_packet(req)

    def on_login(self, packet):
        pass

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_packet(self, packet):
        if isinstance(packet, dict):
            if "event" not in packet:
                return

            if packet["event"] == "subscribed":
                symbol = get_format_lower_symbol(str(packet["symbol"].replace("t", "")))
                self.channels[packet["chanId"]] = (packet["channel"], symbol)

        else:
            if packet[1] == "hb":
                return

            channel_id = packet[0]

            if not channel_id:
                self.on_trade_update(packet)
            else:
                self.on_data_update(packet)

    def on_trade_update(self, data):
        pass

    def on_data_update(self, data):
        pass
