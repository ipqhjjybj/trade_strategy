# coding=utf-8

from tumbler.constant import (
    Direction,
    OrderType,
    Status
)

REST_MARKET_HOST = "https://api-pub.bitfinex.com"
REST_TRADE_HOST = "https://api.bitfinex.com"

WS_MARKET_HOST = "wss://api-pub.bitfinex.com/ws/2"
WS_TRADE_HOST = "wss://api-pub.bitfinex.com/ws/2"

STATUS_BITFINEX2VT = {
    "ACTIVE": Status.NOTTRADED.value,
    "PARTIALLY FILLED": Status.PARTTRADED.value,
    "EXECUTED": Status.ALLTRADED.value,
    "CANCELED": Status.CANCELLED.value,
}

ORDER_TYPE_VT2BITFINEX = {
    OrderType.LIMIT.value: "EXCHANGE LIMIT",
    OrderType.MARKET.value: "EXCHANGE MARKET",
}

ORDER_TYPE_BITFINEX2VT = {v: k for k, v in ORDER_TYPE_VT2BITFINEX.items()}

DIRECTION_VT2BITFINEX = {
    Direction.LONG.value: "Buy",
    Direction.SHORT.value: "Sell",
}
DIRECTION_BITFINEX2VT = {
    "Buy": Direction.LONG.value,
    "Sell": Direction.SHORT.value,
}


def _bitfinex_format_symbol(symbol):
    return "t{}".format(symbol.replace('_', "").upper())
