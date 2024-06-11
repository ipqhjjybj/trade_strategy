# coding=utf-8

from datetime import datetime
from copy import copy
from enum import Enum

from tumbler.object import BBOTickData
from tumbler.object import ContractData, AccountData, PositionData, OrderData, Status
from tumbler.constant import Exchange, Product, Direction, OrderType
from tumbler.function import get_str_dt_use_timestamp
from tumbler.function import get_vt_key, get_dt_use_timestamp

REST_MARKET_HOST = "https://www.okx.com"
REST_TRADE_HOST = "https://www.okx.com"
WEBSOCKET_PUBLIC_HOST = "wss://ws.okx.com:8443/ws/v5/public"  # Market Data
WEBSOCKET_PRIVATE_HOST = "wss://ws.okx.com:8443/ws/v5/private"  # Account and Order


class OKEX5ModeType(Enum):
    CASH = "cash"
    CROSS = "cross"


def parse_bbo_ticks(d, exchange):
    bbo_ticker = BBOTickData()
    bbo_ticker.exchange = exchange

    for tick_data in d["data"]:
        symbol = okex5_format_to_system_format(tick_data["instId"])

        bbo_ticker.symbol_dic[symbol] = {
            "bid": [float(tick_data["bidPx"]), float(tick_data["bidSz"])],
            "ask": [float(tick_data["askPx"]), float(tick_data["askSz"])],
            "vol": float(tick_data["lastSz"]),
            "datetime": get_dt_use_timestamp(tick_data["ts"])
        }
    bbo_ticker.datetime = datetime.now()
    return bbo_ticker


def okex5_format_to_system_format(symbol):
    return symbol.replace('/', '-').replace('-', '_').lower()


def okex5_format_symbol(symbol):
    return symbol.replace('/', '-').replace('_', '-').upper()


STATUS_OKEX5_2VT = {
    "live": Status.NOTTRADED.value,
    "partially_filled": Status.PARTTRADED.value,
    "filled": Status.ALLTRADED.value,
    "canceled": Status.CANCELLED.value
}

DIRECTION_VT2OKEX5 = {Direction.LONG.value: "buy", Direction.SHORT.value: "sell"}
DIRECTION_OKEX5_2VT = {v: k for k, v in DIRECTION_VT2OKEX5.items()}

ORDER_TYPE_VT2OKEX5 = {
    OrderType.LIMIT.value: "limit",
    OrderType.MARKET.value: "market",
    OrderType.POST_ONLY.value: "post_only",
    OrderType.FOK.value: "fok",
    OrderType.IOC.value: "ioc"
}

ORDER_TYPE_OKEX5_2VT = {v: k for k, v in ORDER_TYPE_VT2OKEX5.items()}


def get_inst_type_from_okex_symbol(instrument_id):
    if "-SWAP" in instrument_id:
        return "SWAP"
    return "SPOT"


def get_inst_uly_from_okex_symbol(symbol):
    return "BTC-USD"


def parse_contract_info(data, gateway_name, _type):
    ret = []
    for dic in data:
        symbol = okex5_format_to_system_format(dic["instId"])
        contract = ContractData()
        contract.symbol = symbol
        contract.name = symbol.replace('_', '/')
        contract.exchange = Exchange.OKEX5.value
        contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
        contract.price_tick = float(dic["tickSz"])
        if _type in ["SPOT"]:
            contract.size = 1
        else:
            contract.size = float(dic["ctVal"])
        contract.min_volume = float(dic["minSz"])
        contract.volume_tick = float(dic["lotSz"])
        if _type == "SPOT":
            contract.product = Product.SPOT.value
        elif _type in ["FUTURES"]:
            contract.product = Product.FUTURES.value
            contract.contract_type = dic["alias"]
            contract.delivery_datetime = get_dt_use_timestamp(dic["expTime"])
            contract.listing_datetime = get_dt_use_timestamp(dic["listTime"])
        elif _type in ["SWAP"]:
            contract.product = Product.FUTURES.value
        else:
            contract.product = Product.OPTIONS.value
        contract.gateway_name = gateway_name
        ret.append(contract)
    return ret


def parse_single_account(dic, gateway_name):
    '''
    {
       "code":"0",
       "data":[
          {
             "adjEq":"",
             "details":[
                {
                   "availBal":"28695.03",
                   "availEq":"",
                   "cashBal":"28695.03",
                   "ccy":"BTM",
                   "crossLiab":"",
                   "disEq":"0",
                   "eq":"28695.03",
                   "eqUsd":"2462.033574",
                   "frozenBal":"0",
                   "interest":"",
                   "isoEq":"",
                   "isoLiab":"",
                   "liab":"",
                   "maxLoan":"",
                   "mgnRatio":"",
                   "notionalLever":"",
                   "ordFrozen":"0",
                   "twap":"0",
                   "uTime":"1629191612710",
                   "upl":"",
                   "uplLiab":""
                },
                {
                   "availBal":"42.0262128",
                   "availEq":"",
                   "cashBal":"42.0262128",
                   "ccy":"USDT",
                   "crossLiab":"",
                   "disEq":"42.035878828944",
                   "eq":"42.0262128",
                   "eqUsd":"42.035878828944",
                   "frozenBal":"0",
                   "interest":"",
                   "isoEq":"",
                   "isoLiab":"",
                   "liab":"",
                   "maxLoan":"",
                   "mgnRatio":"",
                   "notionalLever":"",
                   "ordFrozen":"0",
                   "twap":"0",
                   "uTime":"1629178447120",
                   "upl":"",
                   "uplLiab":""
                },
                {
                   "availBal":"0.000539919",
                   "availEq":"",
                   "cashBal":"0.000539919",
                   "ccy":"BTC",
                   "crossLiab":"",
                   "disEq":"25.25305907286",
                   "eq":"0.000539919",
                   "eqUsd":"25.25305907286",
                   "frozenBal":"0",
                   "interest":"",
                   "isoEq":"",
                   "isoLiab":"",
                   "liab":"",
                   "maxLoan":"",
                   "mgnRatio":"",
                   "notionalLever":"",
                   "ordFrozen":"0",
                   "twap":"0",
                   "uTime":"1629178331690",
                   "upl":"",
                   "uplLiab":""
                }
             ],
             "imr":"",
             "isoEq":"",
             "mgnRatio":"",
             "mmr":"",
             "notionalUsd":"",
             "ordFroz":"",
             "totalEq":"2529.322511901804",
             "uTime":"1629195578789"
          }
       ],
       "msg":""
    }
    '''
    imr_val = dic.get("imr", "")
    if not imr_val:
        # 说明是简单模式
        ret = []
        for d in dic["details"]:
            account = AccountData()
            account.account_id = d["ccy"].lower()
            account.vt_account_id = get_vt_key(gateway_name, account.account_id)
            account.balance = float(d["cashBal"])
            account.frozen = float(d["cashBal"]) - float(d["availBal"])
            account.available = float(d["availBal"])
            account.gateway_name = gateway_name
            ret.append(copy(account))
    else:
        # 说明是跨币种保证金模式
        ret = []
        account = AccountData()
        account.account_id = "usdt_all"
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(dic["totalEq"])
        account.frozen = float(dic["imr"])
        account.available = float(dic["adjEq"])
        if abs(float(dic["totalEq"])) > 1e-8:
            account.level_rate = float(dic["notionalUsd"]) / float(dic["totalEq"])
        account.gateway_name = gateway_name
        ret.append(copy(account))

        for d in dic["details"]:
            account = AccountData()
            account.account_id = d["ccy"].lower()
            account.vt_account_id = get_vt_key(gateway_name, account.account_id)
            account.balance = float(d["eq"])
            account.frozen = float(d["eq"]) - float(d["availEq"])
            account.available = float(d["availEq"])
            account.gateway_name = gateway_name
            ret.append(copy(account))
    return ret


def parse_account_data(data, gateway_name):
    ret = []
    for dic in data:
        arr = parse_single_account(dic, gateway_name)
        ret.extend(arr)
    return ret


def parse_asset_data(data, gateway_name):
    ret = []
    for dic in data:
        account = AccountData()
        account.account_id = dic["ccy"].lower()
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(dic["bal"])
        account.frozen = float(dic["frozenBal"])
        account.available = float(dic["availBal"])
        ret.append(account)
    return ret
    

def parse_single_position(dic, gateway_name):
    pos = PositionData()
    pos.symbol = okex5_format_to_system_format(dic["instId"])
    pos.exchange = Exchange.OKEX5.value
    pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
    if dic["posSide"] == "net":
        if float(dic["pos"]) > 0:
            pos.direction = Direction.LONG.value
        else:
            pos.direction = Direction.SHORT.value
    elif dic["posSide"] == "long":
        pos.direction = Direction.LONG.value
    else:
        pos.direction = Direction.SHORT.value

    if dic["availPos"]:
        pos.frozen = float(dic["pos"]) - float(dic["availPos"])
    else:
        pos.frozen = 0
    pos.position = float(dic["pos"])

    pos.price = float(dic.get("avgPx", 0))
    pos.vt_position_id = get_vt_key(pos.vt_symbol, pos.direction)
    pos.gateway_name = gateway_name
    return [pos]


def parse_position_data(data, gateway_name):
    ret = []
    for dic in data:
        arr = parse_single_position(dic, gateway_name)
        ret.extend(arr)
    return ret


def parse_order_info(order_data, order_id, gateway_name):
    order = OrderData()
    order.symbol = okex5_format_to_system_format(order_data["instId"])
    order.exchange = Exchange.OKEX5.value
    order.order_id = order_id
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(order_data["px"])
    order.volume = float(order_data["sz"])
    order.type = ORDER_TYPE_OKEX5_2VT[order_data["ordType"]]
    order.direction = DIRECTION_OKEX5_2VT[order_data["side"]]
    order.traded = float(order_data["accFillSz"])
    order.status = STATUS_OKEX5_2VT[order_data["state"]]
    order.order_time = get_str_dt_use_timestamp(order_data["cTime"])
    if order.status == Status.CANCELLED.value:
        order.cancel_time = get_str_dt_use_timestamp(order_data["uTime"])
    order.gateway_name = gateway_name
    return copy(order)
