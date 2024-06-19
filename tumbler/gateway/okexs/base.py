# coding=utf-8

from tumbler.function import get_vt_key, parse_timestamp_get_str
from tumbler.constant import (
    Direction,
    Exchange,
    OrderType,
    Status,
    Offset,
    Product
)

from tumbler.object import (
    OrderData,
    AccountData,
    PositionData,
    ContractData
)

REST_MARKET_HOST = "https://www.okx.com"
REST_TRADE_HOST = "https://www.okx.com"
WEBSOCKET_MARKET_HOST = "wss://real.okx.com:8443/ws/v3"  # Market Data
WEBSOCKET_TRADE_HOST = "wss://real.okx.com:8443/ws/v3"  # Account and Order

OKEXS_REST_HOST = "https://www.okx.com"
OKEXS_WEBSOCKET_HOST = "wss://real.okx.com:8443/ws/v3"

STATUS_OKEXS2VT = {
    "0": Status.NOTTRADED.value,
    "1": Status.PARTTRADED.value,
    "2": Status.ALLTRADED.value,
    "3": Status.NOTTRADED.value,
    "-1": Status.CANCELLED.value,
}

ORDERTYPE_OKEXS2VT = {
    "0": OrderType.LIMIT.value,
    "1": OrderType.MARKET.value,
}

TYPE_OKEXS2VT = {
    "1": (Offset.OPEN.value, Direction.LONG.value),
    "2": (Offset.OPEN.value, Direction.SHORT.value),
    "3": (Offset.CLOSE.value, Direction.SHORT.value),
    "4": (Offset.CLOSE.value, Direction.LONG.value),
}
TYPE_VT2OKEXS = {v: k for k, v in TYPE_OKEXS2VT.items()}

DIRECTION_OKEXS2VT = {
    "long": Direction.LONG.value,
    "short": Direction.SHORT.value,
}


def okexs_format_to_system_format(symbol):
    return symbol.replace('-', '_').lower()


def okexs_format_symbol(symbol):
    return symbol.replace('_', '-').upper()


okexs_currencies = set(["btc", "ltc", "eth", "etc", "xrp", "eos", "bch", "bsv", "trx"])
okexs_instruments = set([])

for symbol in okexs_currencies:
    okexs_instruments.add('{}_usd_swap'.format(symbol))


def parse_contract_info(data, gateway_name):
    ret = []
    for instrument_data in data:
        symbol = okexs_format_to_system_format(instrument_data["instrument_id"])

        contract = ContractData()
        contract.symbol = symbol
        contract.name = symbol.replace('_', '-').upper()
        contract.exchange = Exchange.OKEXS.value
        contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
        contract.price_tick = float(instrument_data["tick_size"])
        contract.volume_tick = int(instrument_data["size_increment"])
        contract.size = float(instrument_data["contract_val"])

        contract.product = Product.FUTURES.value
        contract.gateway_name = gateway_name
        ret.append(contract)
    return ret


def parse_position_holding(holding, symbol, gateway_name):
    """parse single 'holding' record in replied position data to PositionData. """
    pos = PositionData()
    pos.symbol = okexs_format_to_system_format(symbol)
    pos.exchange = Exchange.OKEXS.value
    pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
    pos.direction = DIRECTION_OKEXS2VT[holding['side']]
    pos.position = int(holding["position"])
    pos.frozen = float(int(holding["position"]) - float(holding["avail_position"]))
    pos.price = float(holding["avg_cost"])
    pos.vt_position_id = get_vt_key(pos.vt_symbol, pos.direction)
    pos.gateway_name = gateway_name

    return pos


def parse_account_info(info, gateway_name):
    account = AccountData()
    account.account_id = info["instrument_id"].upper()
    account.vt_account_id = get_vt_key(gateway_name, account.account_id)
    account.balance = float(info["equity"])
    account.frozen = float(info["margin_frozen"])
    account.available = account.balance - account.frozen
    account.gateway_name = gateway_name
    return account


def parse_order_info(d, gateway_name):
    offset, direction = TYPE_OKEXS2VT[d["type"]]

    order = OrderData()
    order.order_id = d["client_oid"]
    order.exchange = Exchange.OKEXS.value
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.symbol = okexs_format_to_system_format(d["instrument_id"])
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(d["price"])
    order.volume = float(d["size"])
    order.offset = offset
    order.type = ORDERTYPE_OKEXS2VT[d["order_type"]]
    order.direction = direction
    order.traded = float(d["filled_qty"])
    order.status = STATUS_OKEXS2VT[d["state"]]
    order.gateway_name = gateway_name
    order.order_time = parse_timestamp_get_str(d["timestamp"])
    return order
