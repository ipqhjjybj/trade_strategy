# coding=utf-8

from copy import copy

from tumbler.constant import (
    Status,
    Exchange,
    Product
)
from tumbler.object import ContractData
from tumbler.function import get_str_dt_use_timestamp, get_dt_use_timestamp
from tumbler.function import get_vt_key
from tumbler.object import AccountData, OrderData, PositionData, BBOTickData
from tumbler.gateway.huobis.base import STATUS_HUOBIS2VT, ORDERTYPE_HUOBIS2VT, DIRECTION_HUOBIS2VT
import tumbler.config as config


REST_MARKET_HOST = config.SETTINGS["huobiu_market_host"]
REST_TRADE_HOST = config.SETTINGS["huobiu_trade_host"]
WEBSOCKET_MARKET_HOST = config.SETTINGS["huobiu_ws_market_host"]  # Market Data
WEBSOCKET_TRADE_HOST = config.SETTINGS["huobiu_ws_trade_host"]  # Account and Order


def get_from_huobi_to_system_format(symbol):
    return symbol.replace('-', "_").lower()


def get_huobi_future_system_format_symbol(symbol):
    return symbol.replace('_', '-').upper()


def parse_bbo_ticks(d, exchange):
    bbo_ticker = BBOTickData()
    bbo_ticker.exchange = exchange

    for tick_data in d["ticks"]:
        symbol = get_from_huobi_to_system_format(tick_data["contract_code"])
        bbo_ticker.symbol_dic[symbol] = {"bid": tick_data["bid"], "ask": tick_data["ask"],
                                         "vol": float(tick_data["vol"])}
    bbo_ticker.datetime = get_dt_use_timestamp(d["ts"])
    return bbo_ticker


def parse_contract_info(d, gateway_name):
    contract = ContractData()
    contract.symbol = get_from_huobi_to_system_format(d["contract_code"])
    contract.exchange = Exchange.HUOBIU.value
    contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
    contract.name = contract.symbol.replace("_", "/").upper()
    contract.price_tick = d["price_tick"]
    contract.size = float(d["contract_size"])
    contract.volume_tick = 1
    contract.min_volume = 1
    contract.product = Product.FUTURES.value
    contract.gateway_name = gateway_name
    contract.support_margin_mode = d["support_margin_mode"]

    return copy(contract)


def parse_account_info(info, gateway_name):
    account = AccountData()
    account.account_id = info["margin_account"].lower()
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
    pos.exchange = Exchange.HUOBIU.value  # 交易所代码
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
    order.exchange = Exchange.HUOBIU.value
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


