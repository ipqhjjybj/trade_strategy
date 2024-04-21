# coding=utf-8

from copy import copy
from datetime import datetime

from tumbler.constant import (
    Status,
    Exchange,
    Product
)
from tumbler.object import ContractData
from tumbler.function import get_str_dt_use_timestamp, get_dt_use_timestamp
from tumbler.function import get_vt_key
from tumbler.object import AccountData, OrderData, PositionData
from tumbler.gateway.huobis.base import STATUS_HUOBIS2VT, ORDERTYPE_HUOBIS2VT, DIRECTION_HUOBIS2VT
import tumbler.config as config


REST_MARKET_HOST = config.SETTINGS["huobif_market_host"]
REST_TRADE_HOST = config.SETTINGS["huobif_trade_host"]
WEBSOCKET_MARKET_HOST = config.SETTINGS["huobif_ws_market_host"]  # Market Data
WEBSOCKET_TRADE_HOST = config.SETTINGS["huobif_ws_trade_host"]  # Account and Order


def get_from_huobi_to_system_format(symbol):
    return (symbol[:-6] + "_usd_" + symbol[-6:]).lower()


def get_huobi_future_system_format_symbol(symbol):
    symbol = symbol.upper()
    end_symbol = symbol.split('_')[-1]
    if end_symbol in ["CW", "NW", "CQ", "NQ"]:
        return symbol
    else:
        arr = symbol.split('_')
        return arr[0] + arr[-1]


def get_huobi_future_ws_system_format_symbol(symbol):
    symbol = symbol.upper()
    return symbol.split('_')[0]


def get_huobi_future_cancel_order_format_symbol(symbol):
    return symbol.lower().split("_")[0]


def parse_contract_info(d, gateway_name):
    contract = ContractData()
    contract.symbol = get_from_huobi_to_system_format(d["contract_code"])
    contract.exchange = Exchange.HUOBIF.value
    contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
    contract.name = contract.symbol.replace("_", "/").upper()
    contract.price_tick = d["price_tick"]
    contract.size = int(d["contract_size"])
    contract.volume_tick = 1
    contract.min_volume = 1
    contract.product = Product.FUTURES.value

    contract.contract_type = d["contract_type"]
    contract.listing_datetime = datetime.strptime(d["create_date"] + " 16", "%Y%m%d %H")
    contract.delivery_datetime = get_dt_use_timestamp(d["delivery_time"])

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
    pos.exchange = Exchange.HUOBIF.value  # 交易所代码
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
    order.exchange = Exchange.HUOBIF.value
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

    if d.get("canceled_at", 0):
        order.cancel_time = get_str_dt_use_timestamp(d["canceled_at"])

    return copy(order)


