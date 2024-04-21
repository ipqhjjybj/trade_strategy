# coding=utf-8

from datetime import datetime

from tumbler.function import get_dt_use_timestamp, get_vt_key, get_str_dt_use_timestamp
from tumbler.constant import Status, Direction, Exchange, Product
from tumbler.object import BBOTickData, OrderData, ContractData
from tumbler.gateway.binance.base import ORDER_TYPE_BINANCE2VT, asset_from_binance_to_other_exchanges
from tumbler.gateway.binance.base import change_binance_format_to_system_format, asset_from_other_exchanges_to_binance

REST_MARKET_HOST = "https://fapi.binance.com"
REST_TRADE_HOST = "https://fapi.binance.com"
WEBSOCKET_TRADE_HOST = "wss://fstream.binance.com/ws/"
WEBSOCKET_DATA_HOST = "wss://fstream.binance.com/stream?streams="

STATUS_BINANCEF2VT = {
    "NEW": Status.NOTTRADED.value,
    "PARTIALLY_FILLED": Status.PARTTRADED.value,
    "FILLED": Status.ALLTRADED.value,
    "CANCELED": Status.CANCELLED.value,
    "REJECTED": Status.REJECTED.value
}

DIRECTION_VT2BINANCEF = {
    Direction.LONG.value: "BUY",
    Direction.SHORT.value: "SELL"
}
DIRECTION_BINANCEF2VT = {v: k for k, v in DIRECTION_VT2BINANCEF.items()}


def parse_bbo_ticks(data, gateway_name):
    bbo_ticker = BBOTickData()
    bbo_ticker.exchange = gateway_name

    for tick_dic in data:
        symbol = change_binance_format_to_system_format(tick_dic["symbol"])
        bbo_ticker.symbol_dic[symbol] = {
            "bid": [float(tick_dic["bidPrice"]), float(tick_dic["bidQty"])],
            "ask": [float(tick_dic["askPrice"]), float(tick_dic["askQty"])],
            "vol": 0,
            "datetime": get_dt_use_timestamp(tick_dic["time"])
        }
    bbo_ticker.datetime = datetime.now()
    return bbo_ticker


def parse_order_info(d, sys_order_id, gateway_name):
    order = OrderData()
    order.order_id = sys_order_id
    order.exchange = Exchange.BINANCEF.value
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.symbol = change_binance_format_to_system_format(d["symbol"])
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["price"])
    order.volume = float(d["origQty"])
    order.type = ORDER_TYPE_BINANCE2VT[d["type"]]
    order.direction = DIRECTION_BINANCEF2VT[d["side"]]
    order.traded = float(d["executedQty"])
    order.status = STATUS_BINANCEF2VT.get(d["status"], Status.SUBMITTING.value)
    order.order_time = get_str_dt_use_timestamp(d["time"])
    order.gateway_name = gateway_name
    return order


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
    contract.product = Product.FUTURES.value
    contract.gateway_name = gateway_name
    return contract


def parse_contract_arr(data, exchange, gateway_name):
    ret = []
    for d in data["symbols"]:
        if d.get("status", None) == "TRADING":
            contract = parse_contract_info(d, exchange, gateway_name)
            ret.append(contract)
    return ret
