# coding=utf-8

import hashlib

REST_MARKET_HOST = "https://api.coinex.com/perpetual"
REST_TRADE_HOST = "https://api.coinex.com/perpetual"
WEBSOCKET_MARKET_HOST = "wss://perpetual.coinex.com"
WEBSOCKET_TRADE_HOST = "wss://perpetual.coinex.com"


def create_signature(secret_key, params):
    data = ['='.join([str(k), str(v)]) for k, v in params.items()]
    str_params = "{0}&secret_key={1}".format('&'.join(data), secret_key)
    token = hashlib.sha256(str_params.encode("utf-8")).hexdigest()
    return token

def server_sign(apikey, secret_key, timestamp):
    str_params = "access_id={}&timestamp={}&secret_key={}".format(apikey, timestamp, secret_key)
    token = hashlib.sha256(str_params.encode("utf-8")).hexdigest().lower()
    return token