# coding=utf-8

import hashlib

REST_MARKET_HOST = "https://api.coinex.com"
REST_TRADE_HOST = "https://api.coinex.com"
WEBSOCKET_MARKET_HOST = "wss://socket.coinex.com"
WEBSOCKET_TRADE_HOST = "wss://socket.coinex.com"


def create_signature(secret_key, params):
    sort_params = sorted(params)
    data = []
    for item in sort_params:
        data.append(item + '=' + str(params[item]))
    str_params = "{0}&secret_key={1}".format('&'.join(data), secret_key)
    token = hashlib.md5(str_params.encode("utf-8")).hexdigest().upper()
    return token
