# coding=utf-8

import base64
import hashlib
import hmac
from datetime import datetime
from copy import copy

from tumbler.function import get_two_currency, get_vt_key, get_volume_tick_from_min_volume
from tumbler.function import get_str_dt_use_timestamp
from tumbler.object import ContractData, OrderData, AccountData
from tumbler.constant import Exchange, Product, Direction, Offset, OrderType, Status

REST_MARKET_HOST = "https://data.gate.io"
REST_TRADE_HOST = "https://api.gateio.la"
WEBSOCKET_MARKET_HOST = "wss://ws.gate.io/v3/"
WEBSOCKET_TRADE_HOST = "wss://ws.gate.io/v3/"

# 映射关系
gateio_exchanges_dict = {
    "bchsv": "bsv"
}

exchanges_gateio_dict = {v: k for k, v in gateio_exchanges_dict.items()}


def asset_from_gateio_to_other_exchanges(asset):
    """
    将 币安的某些asset 与 其他交易所统一, 比如 bchsv 换成 bsv
    """
    global gateio_exchanges_dict
    if asset in gateio_exchanges_dict.keys():
        return gateio_exchanges_dict[asset]
    return asset


def asset_from_other_exchanges_to_gateio(asset):
    """
    将 其他交易所的asset 印射到 gate的实际 asset , 如 bch映射成bchabc
    """
    global exchanges_gateio_dict
    if asset in exchanges_gateio_dict.keys():
        return exchanges_gateio_dict[asset]
    return asset


def change_system_format_to_gateio_format(symbol):
    """
    将 其他交易所的 symbol,如 bsv_usdt 映射到gate bchsv_usdt
    """
    target_currency, base_currency = get_two_currency(symbol)
    target_currency = asset_from_other_exchanges_to_gateio(target_currency)
    return '{}_{}'.format(target_currency, base_currency)


def change_gateio_format_to_system_format(symbol):
    """
    将 gateio的 交易对如 bchsv_usdt 映射成 bsv_usdt
    """
    target_currency, base_currency = symbol.lower().split('_')
    target_currency = asset_from_gateio_to_other_exchanges(target_currency)
    return ('{}_{}'.format(target_currency, base_currency)).lower()


def create_signature(secret_key, message):
    h = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha512)
    return str(base64.b64encode(h.digest()).decode('utf-8'))


def create_rest_signature(params, secret_key):
    sign = ''
    for key in params.keys():
        value = str(params[key])
        sign += key + '=' + value + '&'
    b_sign = sign[:-1]

    my_sign = hmac.new(secret_key.encode("utf-8"), b_sign.encode("utf-8"), hashlib.sha512).hexdigest()
    return str(my_sign)


def parse_contract_info_arr(data, gateway):
    ret = []
    pairs = data.get("pairs", None)
    if pairs is not None:
        for info in pairs:
            for symbol in info.keys():
                dic = info[symbol]
                symbol = change_gateio_format_to_system_format(symbol)
                contract = ContractData()
                contract.symbol = symbol
                contract.name = symbol.replace('_', '/')
                contract.exchange = Exchange.GATEIO.value
                contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
                contract.price_tick = 10 ** (-1 * int(dic["decimal_places"]))
                contract.size = 1
                contract.min_volume = float(dic["min_amount"])
                contract.volume_tick = get_volume_tick_from_min_volume(contract.min_volume)
                contract.product = Product.SPOT.value
                contract.gateway_name = gateway
                ret.append(contract)
    return ret


def parse_account_info_arr(data, gateway_name):
    ret = []
    available = data.get("available", {})
    locked = data.get("locked", {})
    all_keys = list(set(list(available.keys()) + list(locked.keys())))
    for asset in all_keys:
        tmp_ava = available.get(asset, 0)
        tmp_locked = locked.get(asset, 0)
        account = AccountData()
        account.account_id = asset_from_gateio_to_other_exchanges(asset.lower())
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(tmp_ava) + float(tmp_locked)
        account.frozen = float(tmp_locked)
        account.available = float(tmp_ava)
        account.gateway_name = gateway_name
        ret.append(account)
    return ret


def parse_order_info(d, symbol, order_id, gateway_name, _type):
    direction = Direction.LONG.value
    if d["type"] == "sell":
        direction = Direction.SHORT.value
    order_type = OrderType.LIMIT.value

    order = OrderData()
    order.order_id = order_id
    order.exchange = Exchange.GATEIO.value
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.symbol = symbol
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["rate"])
    order.volume = float(d["amount"])
    order.type = order_type
    order.direction = direction
    order.traded = float(d["filledAmount"])
    order.status = Status.NOTTRADED.value
    if order.traded > 0:
        if order.traded + 1e-6 > order.volume:
            order.status = Status.ALLTRADED.value
        else:
            order.status = Status.PARTTRADED.value
    order.order_time = get_str_dt_use_timestamp(d["timestamp"], 1)
    order.gateway_name = gateway_name

    if order.status == Status.CANCELLED.value:
        order.cancel_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return copy(order)

