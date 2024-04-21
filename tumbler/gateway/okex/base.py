# coding=utf-8

import hashlib
import hmac
import base64
from datetime import datetime
from copy import copy
import json

from tumbler.constant import (
    Direction,
    Status,
    OrderType,
    Exchange,
    Product
)
from tumbler.function import get_vt_key, parse_timestamp_get_str, urlencode
from tumbler.object import AccountData, OrderData, ContractData

REST_MARKET_HOST = "https://www.okex.com"
REST_TRADE_HOST = "https://www.okex.com"
WEBSOCKET_MARKET_HOST = "wss://real.okex.com:8443/ws/v3"  # Market Data
WEBSOCKET_TRADE_HOST = "wss://real.okex.com:8443/ws/v3"  # Account and Order

STATUS_OKEX2VT = {
    "ordering": Status.SUBMITTING.value,
    "open": Status.NOTTRADED.value,
    "part_filled": Status.PARTTRADED.value,
    "filled": Status.ALLTRADED.value,
    "cancelled": Status.CANCELLED.value,
    "cancelling": Status.CANCELLED.value,
    "failure": Status.REJECTED.value,
}

DIRECTION_VT2OKEX = {Direction.LONG.value: "buy", Direction.SHORT.value: "sell"}
DIRECTION_OKEX2VT = {v: k for k, v in DIRECTION_VT2OKEX.items()}

ORDER_TYPE_VT2OKEX = {
    OrderType.LIMIT.value: "limit",
    OrderType.MARKET.value: "market"
}

ORDER_TYPE_OKEX2VT = {v: k for k, v in ORDER_TYPE_VT2OKEX.items()}


def okex_format_to_system_format(symbol):
    return symbol.replace('/', '-').replace('-', '_').lower()


def okex_format_symbol(symbol):
    return symbol.replace('/', '-').replace('_', '-').upper()


def generate_signature(msg, secret_key):
    """OKEX V3 signature"""
    return str(
        base64.b64encode(hmac.new(secret_key.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()).decode(
            'utf-8'))


def sign_request(request, api_key, secret_key, passphrase):
    """
    Generate OKEX signature.
    """
    # Sign
    # timestamp = str(time.time())
    timestamp = get_timestamp()
    request.data = json.dumps(request.data)

    if request.params:
        path = request.path + '?' + urlencode(request.params)
    else:
        path = request.path

    msg = timestamp + request.method + path + request.data
    signature = generate_signature(msg, secret_key)

    # Add headers
    request.headers = {
        'OK-ACCESS-KEY': api_key,
        'OK-ACCESS-SIGN': signature,
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-PASSPHRASE': passphrase,
        'Content-Type': 'application/json'
    }
    return request


def get_timestamp():
    now = datetime.utcnow()
    timestamp = now.isoformat("T", "milliseconds")
    return timestamp + "Z"


def parse_contract_info(data, gateway_name):
    ret = []
    for instrument_data in data:
        symbol = okex_format_to_system_format(instrument_data["instrument_id"])
        contract = ContractData()
        contract.symbol = symbol
        contract.name = symbol.replace('_', '/')
        contract.exchange = Exchange.OKEX.value
        contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
        contract.price_tick = float(instrument_data["tick_size"])
        contract.size = 1
        contract.min_volume = float(instrument_data["min_size"])
        contract.volume_tick = float(instrument_data["size_increment"])
        contract.product = Product.SPOT.value
        contract.gateway_name = gateway_name
        ret.append(contract)
    return ret


def parse_single_account(account_data, gateway_name):
    account = AccountData()
    account.account_id = account_data["currency"].lower()
    account.vt_account_id = get_vt_key(gateway_name, account.account_id)
    account.balance = float(account_data["balance"])
    account.frozen = float(account_data["hold"])
    account.available = float(account_data["available"])
    account.gateway_name = gateway_name
    return copy(account)


def parse_account_data(data, gateway_name):
    account_arr = []
    for account_data in data:
        account = parse_single_account(account_data, gateway_name)
        account_arr.append(account)
    return account_arr


def parse_order_info(order_data, order_id, gateway_name):
    order = OrderData()
    order.symbol = okex_format_to_system_format(order_data["instrument_id"])
    order.exchange = Exchange.OKEX.value
    order.order_id = order_id
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(order_data["price"])
    order.volume = float(order_data["size"])
    order.type = ORDER_TYPE_OKEX2VT[order_data["type"]]
    order.direction = DIRECTION_OKEX2VT[order_data["side"]]
    order.traded = float(order_data["filled_size"])
    order.status = STATUS_OKEX2VT[order_data["status"]]
    order.order_time = parse_timestamp_get_str(order_data["timestamp"])
    order.gateway_name = gateway_name
    return copy(order)

