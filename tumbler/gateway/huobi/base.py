# coding=utf-8

import base64
import hashlib
import hmac
from datetime import datetime
from copy import copy
import json
from tumbler.function import urlencode, get_vt_key, get_str_dt_use_timestamp

from tumbler.constant import (
    Direction,
    Status,
    OrderType,
    Exchange
)
from tumbler.object import OrderData, ContractData, AccountData
from tumbler.constant import Product
import tumbler.config as config

REST_MARKET_HOST = config.SETTINGS["huobi_market_host"]
REST_TRADE_HOST = config.SETTINGS["huobi_trade_host"]
WEBSOCKET_MARKET_HOST = config.SETTINGS["huobi_ws_market_host"]  # Market Data
WEBSOCKET_TRADE_HOST = config.SETTINGS["huobi_ws_trade_host"]  # Account and Order

STATUS_HUOBI2VT = {
    "submitted": Status.NOTTRADED.value,
    "partial-filled": Status.PARTTRADED.value,
    "filled": Status.ALLTRADED.value,
    "cancelling": Status.CANCELLED.value,
    "partial-canceled": Status.CANCELLED.value,
    "canceled": Status.CANCELLED.value,
}

ORDER_TYPE_VT2HUOBI = {
    (Direction.LONG.value, OrderType.MARKET.value): "buy-market",
    (Direction.SHORT.value, OrderType.MARKET.value): "sell-market",
    (Direction.LONG.value, OrderType.LIMIT.value): "buy-limit",
    (Direction.SHORT.value, OrderType.LIMIT.value): "sell-limit",
}
ORDER_TYPE_HUOBI2VT = {v: k for k, v in ORDER_TYPE_VT2HUOBI.items()}

HUOBI_WITHDRAL_FEE = {
    "eth": 0.007,
    "usdt": 5,
    "btm": 5,
    "btc": 0.0005,
    "ltc": 0.001,
    "dot": 0.1
}


def sign_request(request, api_key, secret_key, host):
    """
    Generate HUOBI signature.
    """
    request.headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) \
                AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"
    }
    params_with_signature = create_signature(
        api_key,
        request.method,
        host,
        request.path,
        secret_key,
        request.params
    )
    request.params = params_with_signature

    if request.method == "POST":
        request.headers["Content-Type"] = "application/json"

        if request.data:
            request.data = json.dumps(request.data)

    return request


def create_signature(api_key, method, host, path, secret_key, get_params=None):
    """
    创建签名
    :param secret_key:
    :param path:
    :param host:
    :param method:
    :param api_key:
    :param get_params: dict 使用GET方法时附带的额外参数(urlparams)
    :return:
    """
    sorted_params = [
        ("AccessKeyId", api_key),
        ("SignatureMethod", "HmacSHA256"),
        ("SignatureVersion", "2"),
        ("Timestamp", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"))
    ]

    if get_params:
        sorted_params.extend(list(get_params.items()))
        sorted_params = list(sorted(sorted_params))

    encode_params = urlencode(sorted_params)

    payload = [method, host, path, encode_params]
    payload = "\n".join(payload)
    payload = payload.encode(encoding="UTF8")

    secret_key = secret_key.encode(encoding="UTF8")

    digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(digest)

    params = dict(sorted_params)
    params["Signature"] = signature.decode("UTF8")
    return params


def create_signature_v2(api_key, method, host, path, secret_key):
    """
    创建签名, v2
    :param secret_key:
    :param path:
    :param host:
    :param method:
    :param api_key:
    :return:
    """
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    sorted_params = [
        ("accessKey", api_key),
        ("signatureVersion", "2.1"),
        ("signatureMethod", "HmacSHA256"),
        ("timestamp", timestamp)
    ]

    sorted_params = list(sorted(sorted_params))

    encode_params = urlencode(sorted_params)

    payload = [method, host, path, encode_params]
    payload = "\n".join(payload)
    payload = payload.encode(encoding="UTF8")

    secret_key = secret_key.encode(encoding="UTF8")

    digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode()

    params = dict(sorted_params)
    params["signature"] = signature
    params["authType"] = "api"

    tmp_params = {
        "accessKey": api_key,
        "signatureVersion": "2.1",
        "signatureMethod": "HmacSHA256",
        "timestamp": timestamp,
        "signature": signature,
        "authType": "api"
    }

    params["action"] = "req"
    params["ch"] = "auth"
    params["params"] = tmp_params
    return params


def parse_order_info(d, symbol, order_id, gateway_name, _type):
    direction, order_type = ORDER_TYPE_HUOBI2VT[d["type"]]

    order = OrderData()
    order.order_id = order_id
    order.exchange = Exchange.HUOBI.value
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.symbol = symbol
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["price"])
    order.volume = float(d["amount"])
    order.type = order_type
    order.direction = direction
    if _type == "query_open_order":
        order.traded = float(d["filled-amount"])
    elif _type == "query_order":
        order.traded = float(d["field-amount"])
    else:
        if "field-amount" in d.keys():
            order.traded = float(d["field-amount"])
        else:
            order.traded = float(d["filled-amount"])
    order.status = STATUS_HUOBI2VT.get(d["state"], None)
    order.order_time = get_str_dt_use_timestamp(d["created-at"])
    order.gateway_name = gateway_name

    if order.status == Status.CANCELLED.value:
        order.cancel_time = get_str_dt_use_timestamp(d["canceled-at"])

    return copy(order)


def parse_contract_info(d, gateway_name):
    base_currency = d["base-currency"]
    quote_currency = d["quote-currency"]
    name = "{}/{}".format(base_currency.upper(), quote_currency.upper())

    price_tick = 1 / pow(10, d["price-precision"])
    volume_tick = 1 / pow(10, d["amount-precision"])
    min_volume = d["min-order-amt"]

    contract = ContractData()
    contract.symbol = "{}_{}".format(base_currency.lower(), quote_currency.lower())
    contract.exchange = Exchange.HUOBI.value
    contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
    contract.name = name
    contract.price_tick = price_tick
    contract.size = 1
    contract.volume_tick = volume_tick
    contract.min_volume = min_volume
    contract.product = Product.SPOT.value
    contract.gateway_name = gateway_name

    return copy(contract)


def parse_account_info(data, gateway_name):
    ret = []
    buf = {}
    for d in data["data"]["list"]:
        currency = d["currency"]
        currency_data = buf.setdefault(currency, {})
        currency_data[d["type"]] = float(d["balance"])

    for currency, currency_data in buf.items():
        account = AccountData()
        account.account_id = currency
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = currency_data["trade"] + currency_data["frozen"]
        account.frozen = currency_data["frozen"]
        account.available = currency_data["trade"]
        account.gateway_name = gateway_name

        ret.append(account)
    return ret
