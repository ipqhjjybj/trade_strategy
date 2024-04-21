# coding=utf-8

import hashlib
import hmac
import time
from copy import copy
from tumbler.constant import Exchange, Direction
from tumbler.function import get_vt_key, urlencode, get_dt_use_timestamp
from tumbler.object import ContractData, AccountData, PositionData, OrderData
from tumbler.constant import Product, Status, OrderType, Offset
import tumbler.config as config

#REST_MARKET_HOST = "https://staging.nexus.kronostoken.com"
#REST_TRADE_HOST = "https://staging.nexus.kronostoken.com"
#WEBSOCKET_MARKET_HOST = "wss://staging.nexus.kronostoken.com/ws/{}/{}"  # Market Data
#WEBSOCKET_TRADE_HOST = "wss://staging.nexus.kronostoken.com/ws/{}/{}"  #

REST_MARKET_HOST = config.SETTINGS["nexus_market_host"]
REST_TRADE_HOST = config.SETTINGS["nexus_trade_host"]
WEBSOCKET_MARKET_HOST = config.SETTINGS["nexus_ws_market_host"]  # Market Data
WEBSOCKET_TRADE_HOST = config.SETTINGS["nexus_ws_trade_host"]  # Trade Data

api_key = config.SETTINGS["nexus_api_key"]
secret_key = config.SETTINGS["nexus_secret_key"]
account_id = config.SETTINGS["nexus_account_id"]

ORDER_TYPE_VT2NEXUS = {
    Direction.LONG.value: "BUY",
    Direction.SHORT.value: "SELL"
}
ORDER_TYPE_NEXUS2VT = {v: k for k, v in ORDER_TYPE_VT2NEXUS.items()}

ORDER_PRICE_VT2NEXUS = {
    OrderType.LIMIT.value: "LIMIT",
    OrderType.MARKET.value: "MARKET"
}
ORDER_PRICE_NEXUS2VT = {v: k for k, v in ORDER_PRICE_VT2NEXUS.items()}

STATUS_NEXUS2VT = {
    "NEW": Status.NOTTRADED.value,
    "PARTIAL_FILLED": Status.PARTTRADED.value,
    "FILLED": Status.ALLTRADED.value,
    "CANCELLED": Status.CANCELLED.value,
}


def nexus_format_symbol(symbol):
    return "SPOT_" + symbol.upper()


def system_symbol_from_nexus(symbol):
    return symbol.replace("SPOT_", "").lower()


def parse_contract_info(data, gateway_name):
    contract = ContractData()
    contract.symbol = system_symbol_from_nexus(data["symbol"])  # 代码
    contract.exchange = Exchange.NEXUS.value  # 交易所代码
    contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码
    contract.name = contract.symbol.replace("_", "/").upper()  # 合约中文名

    contract.size = 1  # 合约大小
    contract.price_tick = data["quote_tick"]  # 合约最小价格TICK
    contract.volume_tick = data["base_tick"]  # 合约最小数量tick
    contract.min_volume = data["base_min"]  # 最小交易数量
    contract.stop_supported = False  # 是否支持stop order
    contract.product = Product.FUTURES.value  # 品种种类
    return contract


def parse_account_info(data, gateway_name):
    account = AccountData()
    risk_rate = data["risk_rate"]
    if risk_rate is None:
        risk_rate = 0
    total = float(data["application"]["collateral"]) * (1 - risk_rate / 100.0)
    frozen = float(data["application"]["collateral_frozen"])
    account.account_id = data["application"]["application_id"]
    account.vt_account_id = get_vt_key(gateway_name, account.account_id)
    account.balance = total
    account.frozen = frozen
    account.gateway_name = gateway_name
    return account


def parse_position_list(data, gateway_name):
    position_list = []
    for d in data["holding"]:
        position = PositionData()
        position.symbol = d["token"].lower()
        position.exchange = Exchange.NEXUS.value
        position.position = d["holding"]
        position.frozen = 0
        position.vt_position_id = get_vt_key(position.symbol, Direction.NET.value)
        position_list.append(position)
    return position_list


def parse_order_info(d, order_id, gateway_name):
    order = OrderData()
    order.symbol = system_format_from_nexus(d["symbol"])
    order.vt_symbol = get_vt_key(order.symbol, Exchange.NEXUS.value)
    order.exchange = Exchange.NEXUS.value
    order.order_id = order_id
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.direction = ORDER_TYPE_NEXUS2VT[d["side"]]     # 报单方向
    order.type = ORDER_PRICE_VT2NEXUS[d["type"]]  # LIMIT单还是MARKET单
    order.offset = Offset.OPEN.value  # 报单开平仓
    order.price = d["price"]  # 报单价格
    order.volume = d["quantity"]  # 报单总数量
    order.traded = d["executed"]  # 报单成交数量
    order.status = Status.SUBMITTING.value  # 报单状态
    order.order_time = get_dt_use_timestamp(d["created_time"], mill=1)  # 发单时间
    order.gateway_name = gateway_name
    return copy(order)


def system_format_from_nexus(symbol):
    return symbol.replace('SPOT_', "").lower()


def generate_signature(msg, secret_key):
    """ nexus """
    return str(hmac.new(secret_key.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest())


def get_timestamp():
    return str(int(time.time() * 1e3))


def sign_request(request):
    """
    Generate NEXUS signature.
    """
    timestamp = get_timestamp()
    if not request.data:
        request.data = {}
    if not request.params:
        request.params = {}

    s = list(request.data.items()) + list(request.params.items())
    s.sort()

    msg = urlencode(s) + "|" + timestamp
    signature = generate_signature(msg, secret_key)

    # Add headers
    request.headers = {
        'x-api-key': api_key,
        'x-api-signature': signature,
        'x-api-timestamp': timestamp,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    return request
