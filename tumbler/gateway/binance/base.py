# coding=utf-8

from enum import Enum
import time
import hmac
import hashlib
from datetime import datetime

from tumbler.constant import (
    Direction,
    Status,
    OrderType
)

from tumbler.constant import Product, Exchange
from tumbler.function import get_two_currency, get_vt_key, urlencode, get_str_dt_use_timestamp
from tumbler.object import ContractData, AccountData, OrderData, BBOTickData

REST_MARKET_HOST = "https://www.binance.com"
REST_TRADE_HOST = "https://www.binance.com"

WEBSOCKET_TRADE_HOST = "wss://stream.binance.com:9443/ws/"
WEBSOCKET_DATA_HOST = "wss://stream.binance.com:9443/stream?streams="

STATUS_BINANCE2VT = {
    "NEW": Status.NOTTRADED.value,
    "PARTIALLY_FILLED": Status.PARTTRADED.value,
    "FILLED": Status.ALLTRADED.value,
    "CANCELED": Status.CANCELLED.value,
    "REJECTED": Status.REJECTED.value
}

ORDER_TYPE_VT2BINANCE = {
    OrderType.LIMIT.value: "LIMIT",
    OrderType.MARKET.value: "MARKET"
}
ORDER_TYPE_BINANCE2VT = {v: k for k, v in ORDER_TYPE_VT2BINANCE.items()}

DIRECTION_VT2BINANCE = {
    Direction.LONG.value: "BUY",
    Direction.SHORT.value: "SELL"
}
DIRECTION_BINANCE2VT = {v: k for k, v in DIRECTION_VT2BINANCE.items()}


class Security(Enum):
    NONE = 0
    SIGNED = 1
    API_KEY = 2


symbol_name_map = {}

# 印射关系
binance_exchanges_dict = {
}

exchanges_biannce_dict = {v: k for k, v in binance_exchanges_dict.items()}


def parse_bbo_ticks(data, exchange):
    bbo_ticker = BBOTickData()
    bbo_ticker.exchange = exchange

    for tick_dic in data:
        symbol = change_binance_format_to_system_format(tick_dic["symbol"])
        bbo_ticker.symbol_dic[symbol] = {
            "bid": [float(tick_dic["bidPrice"]), float(tick_dic["bidQty"])],
            "ask": [float(tick_dic["askPrice"]), float(tick_dic["askQty"])],
            "vol": 0
        }
    bbo_ticker.datetime = datetime.now()
    return bbo_ticker


def sign_request(request, api_key, secret_key, recv_window=5000, time_offset=0):
    """
    Generate BINANCE signature.
    """
    if request.data:
        security = request.data.get("security", Security.NONE.value)
    else:
        security = Security.NONE.value
    if security == Security.NONE.value:
        request.data = None
        return request

    if request.params:
        path = request.path + "?" + urlencode(request.params)
    else:
        request.params = dict()
        path = request.path

    if security == Security.SIGNED.value:
        timestamp = int(time.time() * 1000) - time_offset

        request.params["recvWindow"] = recv_window
        request.params["timestamp"] = timestamp

        query = urlencode(sorted(request.params.items()))
        signature = hmac.new(secret_key.encode('utf-8'), query.encode("utf-8"), hashlib.sha256).hexdigest()

        query += "&signature={}".format(signature)
        path = request.path + "?" + query

    request.path = path
    request.params = {}
    request.data = {}

    # Add headers
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-MBX-APIKEY": api_key
    }

    if security in [Security.SIGNED.value, Security.API_KEY.value]:
        request.headers = headers

    return request


def parse_contract_info(d, exchange, gateway_name):
    symbol = change_binance_format_to_system_format(d["symbol"])

    base_currency = asset_from_other_exchanges_to_binance(d["baseAsset"])
    quote_currency = (asset_from_binance_to_other_exchanges(d["quoteAsset"]))
    name = "{}/{}".format(base_currency.upper(), quote_currency.upper())

    price_tick = 1
    min_volume = 1
    volume_tick = 1

    for f in d["filters"]:
        if f["filterType"] == "PRICE_FILTER":
            price_tick = float(f["tickSize"])
        elif f["filterType"] == "LOT_SIZE":
            min_volume = float(f["stepSize"])
            volume_tick = min_volume

    contract = ContractData()
    contract.symbol = symbol
    contract.name = name
    contract.exchange = exchange
    contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
    contract.price_tick = price_tick
    contract.size = 1
    contract.min_volume = min_volume
    contract.volume_tick = volume_tick
    contract.product = Product.SPOT.value
    contract.gateway_name = gateway_name
    return contract


def parse_contract_arr(data, exchange, gateway_name):
    ret = []
    for d in data["symbols"]:
        if d.get("status", None) == "TRADING":
            contract = parse_contract_info(d, exchange, gateway_name)
            ret.append(contract)
    return ret


def parse_order_info(d, sys_order_id, gateway_name):
    order = OrderData()
    order.order_id = sys_order_id
    order.exchange = Exchange.BINANCE.value
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.symbol = change_binance_format_to_system_format(d["symbol"])
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["price"])
    order.volume = float(d["origQty"])
    order.type = ORDER_TYPE_BINANCE2VT[d["type"]]
    order.traded = float(d["executedQty"])
    order.status = STATUS_BINANCE2VT.get(d["status"], Status.SUBMITTING.value)

    order.time = get_str_dt_use_timestamp(d["time"])
    order.gateway_name = gateway_name
    return order


def parse_account_info_arr(data, gateway_name):
    ret = []
    for account_data in data["balances"]:
        account = AccountData()
        account.account_id = asset_from_other_exchanges_to_binance(account_data["asset"].lower())
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(account_data["free"]) + float(account_data["locked"])
        account.frozen = float(account_data["locked"])
        account.available = float(account_data["free"])
        account.gateway_name = gateway_name

        ret.append(account)
    return ret


def asset_from_binance_to_other_exchanges(asset):
    """
    将 币安的某些asset 与 其他交易所统一, 比如 bchabc 换成 bch
    """
    global binance_exchanges_dict
    if asset in binance_exchanges_dict.keys():
        return binance_exchanges_dict[asset.lower()]
    return asset


def asset_from_other_exchanges_to_binance(asset):
    """
    将 其他交易所的asset 印射到 币安的实际 asset , 如 bch映射成bchabc
    """
    global exchanges_biannce_dict
    if asset in exchanges_biannce_dict.keys():
        return exchanges_biannce_dict[asset.lower()]
    return asset


def change_system_format_to_binance_format(symbol):
    """
    将 其他交易所的 symbol,如 bch_usdt 映射到币安 bchabcusdt
    """
    target_currency, base_currency = get_two_currency(symbol)
    target_currency = asset_from_other_exchanges_to_binance(target_currency)
    return ('{}{}'.format(target_currency, base_currency)).upper()


def change_binance_format_to_system_format(symbol):
    """
    将 币安的 交易对如 BCHABCUSDT映射成 bch_usdt
    """
    target_currency, base_currency = get_two_currency(symbol)
    target_currency = asset_from_binance_to_other_exchanges(target_currency)
    return ('{}_{}'.format(target_currency, base_currency)).lower()

