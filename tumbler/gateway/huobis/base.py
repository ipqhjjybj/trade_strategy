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
    Offset,
    Product
)
from tumbler.object import ContractData
from tumbler.function import get_str_dt_use_timestamp
from tumbler.function import get_vt_key, parse_timestamp_get_str, urlencode
from tumbler.object import AccountData, OrderData, PositionData
import tumbler.config as config

REST_MARKET_HOST = config.SETTINGS["huobis_market_host"]
REST_TRADE_HOST = config.SETTINGS["huobis_trade_host"]
WEBSOCKET_MARKET_HOST = config.SETTINGS["huobis_ws_market_host"]  # Market Data
WEBSOCKET_TRADE_HOST = config.SETTINGS["huobis_ws_trade_host"]  # Account and Order


DIRECTION_VT2HUOBIS = {
    Direction.LONG.value: "buy",
    Direction.SHORT.value: "sell",
}

OFFSET_VT2HUOBIS = {
    Offset.OPEN.value: "open",
    Offset.CLOSE.value: "close",
}

DIRECTION_HUOBIS2VT = {v: k for k, v in DIRECTION_VT2HUOBIS.items()}

STATUS_HUOBIS2VT = {
    1: Status.SUBMITTING.value,
    2: Status.SUBMITTING.value,
    3: Status.NOTTRADED.value,
    4: Status.PARTTRADED.value,
    5: Status.CANCELLED.value,
    6: Status.ALLTRADED.value,
    7: Status.CANCELLED.value,
    11: Status.CANCELLED.value,
}

ORDERTYPE_VT2HUOBIS = {
    OrderType.MARKET.value: "opponent",
    OrderType.LIMIT.value: "limit",
    OrderType.FOK.value: "fok",
    OrderType.FAK.value: "ioc"
}

ORDERTYPE_HUOBIS2VT = {v: k for k, v in ORDERTYPE_VT2HUOBIS.items()}
ORDERTYPE_HUOBIS2VT[1] = OrderType.LIMIT.value
ORDERTYPE_HUOBIS2VT[3] = OrderType.MARKET.value
ORDERTYPE_HUOBIS2VT[4] = OrderType.MARKET.value
ORDERTYPE_HUOBIS2VT[5] = OrderType.STOP.value
ORDERTYPE_HUOBIS2VT[6] = OrderType.LIMIT.value
ORDERTYPE_HUOBIS2VT["lightning"] = OrderType.MARKET.value
ORDERTYPE_HUOBIS2VT["optimal_5"] = OrderType.MARKET.value
ORDERTYPE_HUOBIS2VT["optimal_10"] = OrderType.MARKET.value
ORDERTYPE_HUOBIS2VT["optimal_20"] = OrderType.MARKET.value


def get_from_huobi_to_system_format(symbol):
    return symbol.replace('-', "_").lower().replace('usd', 'usdt')


def get_huobi_future_system_format_symbol(symbol):
    return symbol.replace('usdt', 'usd').replace('_', '-').upper()


def parse_contract_info(d, gateway_name):
    contract = ContractData()
    contract.symbol = get_from_huobi_to_system_format(d["contract_code"])
    contract.exchange = Exchange.HUOBIS.value
    contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
    contract.name = contract.symbol.replace("_", "/").upper()
    contract.price_tick = d["price_tick"]
    contract.size = int(d["contract_size"])
    contract.volume_tick = 1
    contract.min_volume = 1
    contract.product = Product.FUTURES.value
    contract.gateway_name = gateway_name

    return copy(contract)


def parse_account_info(info, gateway_name):
    account = AccountData()
    account.account_id = info["symbol"].lower()
    account.vt_account_id = get_vt_key(gateway_name, account.account_id)
    account.balance = float(info["margin_balance"])
    account.frozen = float(info["margin_frozen"])
    account.available = account.balance - account.frozen
    account.gateway_name = gateway_name
    return account


def parse_position_holding(d, gateway_name):
    """parse single 'holding' record in replied position data to PositionData. """
    pos = PositionData()
    pos.symbol = get_from_huobi_to_system_format(d["contract_code"])
    pos.exchange = Exchange.HUOBIS.value  # 交易所代码
    pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)  # 合约在vt系统中的唯一代码，合约代码.交易所代码

    # 持仓相关
    pos.direction = DIRECTION_HUOBIS2VT[d["direction"]]  # 持仓方向
    pos.position = d["volume"]  # 持仓量
    pos.frozen = d["frozen"]  # 冻结数量
    pos.price = d["cost_hold"]  # 持仓均价
    pos.vt_position_id = get_vt_key(pos.vt_symbol, pos.direction)  # 持仓在vt系统中的唯一代码，通常是vtSymbol.方向
    return pos


def parse_order_info(d, symbol, order_id, gateway_name):
    order = OrderData()
    order.order_id = order_id
    order.exchange = Exchange.HUOBIS.value
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.symbol = symbol
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["price"])
    order.volume = float(d["volume"])
    order.type = ORDERTYPE_HUOBIS2VT[d["order_price_type"]]
    order.direction = DIRECTION_HUOBIS2VT[d["direction"]]
    order.traded = float(d["trade_volume"])

    order.status = STATUS_HUOBIS2VT.get(d["status"], None)
    order.order_time = get_str_dt_use_timestamp(d["created_at"])
    order.gateway_name = gateway_name

    if order.status == Status.CANCELLED.value:
        order.cancel_time = get_str_dt_use_timestamp(d["created_at"])

    return copy(order)
