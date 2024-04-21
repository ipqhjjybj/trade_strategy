# coding=utf-8

import time
import hashlib
import hmac
from datetime import datetime
from urllib import parse
from copy import copy

from tumbler.constant import MAX_PRICE_NUM, Exchange
from tumbler.function import urlencode, get_vt_key, parse_timestamp
from tumbler.object import OrderData
from tumbler.constant import (
    Direction,
    OrderType,
    Status
)

REST_MARKET_HOST = "https://www.bitmex.com"
REST_TRADE_HOST = "https://www.bitmex.com"
WEBSOCKET_MARKET_HOST = "wss://www.bitmex.com/realtime"
WEBSOCKET_TRADE_HOST = "wss://www.bitmex.com/realtime"

ORDER_TYPE_VT2BITMEX = {
    OrderType.LIMIT.value: "Limit",
    OrderType.MARKET.value: "Market",
    OrderType.STOP.value: "Stop"
}

ORDER_TYPE_BITMEX2VT = {v: k for k, v in ORDER_TYPE_VT2BITMEX.items()}

DIRECTION_VT2BITMEX = {Direction.LONG.value: "Buy", Direction.SHORT.value: "Sell"}
DIRECTION_BITMEX2VT = {v: k for k, v in DIRECTION_VT2BITMEX.items()}

STATUS_BITMEX2VT = {
    "New": Status.NOTTRADED.value,
    "Partially filled": Status.PARTTRADED.value,
    "Filled": Status.ALLTRADED.value,
    "Canceled": Status.CANCELLED.value,
    "Rejected": Status.REJECTED.value,
}

bitmex_dict_symbol = {"XBTUSD": "btc_usdt"}
system_dict_bitmex = {v: k for k, v in bitmex_dict_symbol.items()}


def change_from_bitmex_to_system(symbol):
    return bitmex_dict_symbol.get(symbol, symbol)


def change_from_system_to_bitmex(symbol):
    return system_dict_bitmex.get(symbol, symbol)


def sign_request(request, apikey, secret_key):
    """
    Generate BitMEX signature.
    """
    # Sign
    expires = int(time.time() + 30)

    if request.params:
        query = urlencode(request.params)
        #query = parse.urlencode(request.params)
        path = request.path + "?" + query
    else:
        path = request.path

    if request.data:
        request.data = urlencode(request.data)
        #request.data = parse.urlencode(request.data)
    else:
        request.data = ""

    msg = request.method + path + str(expires) + request.data

    signature = hmac.new(secret_key.encode('utf-8'), msg.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()

    # Add headers
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "api-key": apikey,
        "api-expires": str(expires),
        "api-signature": signature,
    }

    request.headers = headers
    return request


def parse_ticker(tick, data):
    tick.datetime = datetime.now()
    tick.date = tick.datetime.strftime("%Y%m%d")
    tick.time = tick.datetime.strftime("%H:%M:%S")

    bids = [(float(dic["price"]), float(dic["size"])) for dic in data if dic["side"] == 'Buy']
    asks = [(float(dic["price"]), float(dic["size"])) for dic in data if dic["side"] == "Sell"]

    bids.sort(reverse=True)
    asks.sort()

    max_num = min(MAX_PRICE_NUM, len(bids))
    for n in range(max_num):
        price, volume = bids[n]
        tick.bid_prices[n] = float(price)
        tick.bid_volumes[n] = float(volume)

    max_num = min(MAX_PRICE_NUM, len(asks))
    for n in range(max_num):
        price, volume = asks[n]
        tick.ask_prices[n] = float(price)
        tick.ask_volumes[n] = float(volume)

    tick.last_price = (tick.ask_prices[0] + tick.bid_prices[0]) / 2.0
    return tick


def parse_order_info(d, local_order_id, gateway_name):
    '''
    {
       "orderID":"0d1f00a7-0aac-4c05-99ab-e2eabd544c4b",
       "clOrdID":"",
       "clOrdLinkID":"",
       "account":1474991,
       "symbol":"XBTUSD",
       "side":"Buy",
       "simpleOrderQty":"None",
       "orderQty":4000,
       "price":10000,
       "displayQty":"None",
       "stopPx":"None",
       "pegOffsetValue":"None",
       "pegPriceType":"",
       "currency":"USD",
       "settlCurrency":"XBt",
       "ordType":"Limit",
       "timeInForce":"GoodTillCancel",
       "execInst":"",
       "contingencyType":"",
       "exDestination":"XBME",
       "ordStatus":"New",
       "triggered":"",
       "workingIndicator":True,
       "ordRejReason":"",
       "simpleLeavesQty":"None",
       "leavesQty":4000,
       "simpleCumQty":"None",
       "cumQty":0,
       "avgPx":"None",
       "multiLegReportingType":"SingleSecurity",
       "text":"Submission from www.bitmex.com",
       "transactTime":"2020-10-27T05:15:39.937Z",
       "timestamp":"2020-10-27T05:15:39.937Z"
    }
    '''
    order = OrderData()
    order.order_id = local_order_id
    order.exchange = Exchange.BITMEX.value
    order.vt_order_id = get_vt_key(order.order_id, order.exchange)
    order.symbol = bitmex_dict_symbol.get(d["symbol"], d["symbol"])
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["price"])
    order.volume = float(d["orderQty"])
    order.type = ORDER_TYPE_BITMEX2VT[d["ordType"]]
    order.direction = DIRECTION_BITMEX2VT[d["side"]]
    order.traded = float(d["cumQty"])
    order.status = STATUS_BITMEX2VT[d["ordStatus"]]
    order.order_time = parse_timestamp(d["transactTime"])
    order.gateway_name = gateway_name

    if order.status == Status.CANCELLED.value:
        order.cancel_time = parse_timestamp(d["timestamp"])

    return copy(order)

