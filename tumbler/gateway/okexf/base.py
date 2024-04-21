# coding=utf-8
from datetime import datetime

from tumbler.constant import (
    Direction,
    OrderType,
    Status,
    Offset,
    Exchange,
    Product
)
from tumbler.object import ContractData, AccountData, PositionData, OrderData
from tumbler.function import get_vt_key, parse_timestamp_get_str
from tumbler.constant import DiffTypeFuture


REST_MARKET_HOST = "https://www.okex.com"
REST_TRADE_HOST = "https://www.okex.com"
WEBSOCKET_MARKET_HOST = "wss://real.okex.com:8443/ws/v3"  # Market Data
WEBSOCKET_TRADE_HOST = "wss://real.okex.com:8443/ws/v3"  # Account and Order

OKEXF_REST_HOST = "https://www.okex.com"
OKEXF_WEBSOCKET_HOST = "wss://real.okex.com:8443/ws/v3"

STATUS_OKEXF2VT = {
    "0": Status.NOTTRADED.value,
    "1": Status.PARTTRADED.value,
    "2": Status.ALLTRADED.value,
    "3": Status.NOTTRADED.value,
    "-1": Status.CANCELLED.value,
}

ORDERTYPE_OKEXF2VT = {
    "0": OrderType.LIMIT.value,
    "1": OrderType.MARKET.value,
}

TYPE_OKEXF2VT = {
    "1": (Offset.OPEN.value, Direction.LONG.value),
    "2": (Offset.OPEN.value, Direction.SHORT.value),
    "3": (Offset.CLOSE.value, Direction.SHORT.value),
    "4": (Offset.CLOSE.value, Direction.LONG.value),
}
TYPE_VT2OKEXF = {v: k for k, v in TYPE_OKEXF2VT.items()}


def okexf_format_to_system_format(symbol):
    return symbol.replace('-', '_').lower()


def okexf_format_symbol(symbol):
    return symbol.replace('_', '-').upper()


def get_underlying_symbol(symbol):
    return '-'.join(symbol.split('_')[:-1])


okexf_currencies = set(["btc", "ltc", "eth", "etc", "xrp", "eos", "bch", "bsv", "trx"])
okexf_contract_pairs = set([])
for currency in okexf_currencies:
    okexf_contract_pairs.add(currency + "_usd")
    okexf_contract_pairs.add(currency + "_usdt")


def get_pre_symbol(symbol):
    return '_'.join(symbol.split('_')[:-1])


def parse_get_diff_type_contract(contract_arr, pre_symbol):
    ret_dic = {}
    for contract in contract_arr:
        if get_pre_symbol(contract.symbol) == pre_symbol:
            ret_dic[contract.contract_type] = contract
    return ret_dic


def parse_contract_info(data, gateway_name):
    ret = []
    for instrument_data in data:
        symbol = okexf_format_to_system_format(instrument_data["instrument_id"])
        contract = ContractData()
        contract.symbol = symbol
        contract.name = okexf_format_symbol(symbol)
        contract.exchange = Exchange.OKEXF.value
        contract.vt_symbol = get_vt_key(contract.symbol, contract.exchange)
        contract.price_tick = float(instrument_data["tick_size"])
        contract.volume_tick = float(instrument_data["trade_increment"])
        contract.size = float(instrument_data["contract_val"])

        contract.contract_type = instrument_data["alias"]
        contract.delivery_datetime = datetime.strptime(instrument_data["delivery"] + " 16", "%Y-%m-%d %H")
        contract.listing_datetime = datetime.strptime(instrument_data["listing"] + " 16", "%Y-%m-%d %H")

        contract.product = Product.FUTURES.value
        contract.gateway_name = gateway_name
        ret.append(contract)
    return ret


def parse_single_account(currency, d, gateway_name):
    if "underlying" in d.keys():
        # 全仓模式 或者是 bsv-usd 那种反向合约
        account = AccountData()
        account.account_id = okexf_format_to_system_format(d["underlying"])
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(d["equity"])
        account.frozen = float(d.get("margin_for_unfilled", 0))
        account.available = account.balance - account.frozen
        account.gateway_name = gateway_name
        return account
    else:
        # 逐仓模式 , 或者是 bsv-usdt 那种正向合约
        '''
        {
           "total_avail_balance":"0.11129351",
           "contracts":[
              {
                 "available_qty":"0.9486",
                 "fixed_balance":"0",
                 "instrument_id":"BSV-USDT-200626",
                 "margin_for_unfilled":"0",
                 "margin_frozen":"0",
                 "realized_pnl":"0.837308",
                 "unrealized_pnl":"0"
              }
           ],
           "equity":"0.94860151",
           "margin_mode":"fixed",
           "auto_margin":"0",
           "liqui_mode":"tier",
           "can_withdraw":"0.11129351",
           "currency":"USDT"
        }
        '''
        account = AccountData()
        account.account_id = okexf_format_to_system_format(currency)
        account.vt_account_id = get_vt_key(gateway_name, account.account_id)
        account.balance = float(d["equity"])
        account.frozen = float(d["equity"]) - float(d["total_avail_balance"])
        account.available = account.balance - account.frozen
        account.gateway_name = gateway_name
        return account


def parse_account_info(data, gateway_name):
    account_list = []
    for currency, d in data["info"].items():
        account = parse_single_account(currency, d, gateway_name)
        account_list.append(account)
    return account_list


def parse_single_position(d):
    pos1 = PositionData()
    pos1.symbol = okexf_format_to_system_format(d["instrument_id"])
    pos1.exchange = Exchange.OKEXF.value
    pos1.vt_symbol = get_vt_key(pos1.symbol, pos1.exchange)
    pos1.direction = Direction.LONG.value
    pos1.position = float(d["long_qty"])
    pos1.frozen = float(d["long_qty"]) - float(d["long_avail_qty"])
    pos1.price = float(d["long_avg_cost"])
    pos1.vt_position_id = get_vt_key(pos1.vt_symbol, pos1.direction)

    pos2 = PositionData()
    pos2.symbol = okexf_format_to_system_format(d["instrument_id"])
    pos2.exchange = Exchange.OKEXF.value
    pos2.vt_symbol = get_vt_key(pos2.symbol, pos2.exchange)
    pos2.direction = Direction.SHORT.value
    pos2.position = float(d["short_qty"])
    pos2.frozen = float(d["short_qty"]) - float(d["short_avail_qty"])
    pos2.price = float(d["short_avg_cost"])
    pos2.vt_position_id = get_vt_key(pos2.vt_symbol, pos2.direction)

    return pos1, pos2


def parse_position_info(data, set_all_symbols):
    ret_positions = []
    all_position_sets = set([])
    for symbol in set_all_symbols:
        for direction in [Direction.LONG.value, Direction.SHORT.value]:
            all_position_sets.add((symbol, direction))

    if data["holding"]:
        for arr in data["holding"]:
            for d in arr:
                pos1, pos2 = parse_single_position(d)
                if (pos1.symbol, pos1.direction) in all_position_sets:
                    all_position_sets.remove((pos1.symbol, pos1.direction))
                ret_positions.append(pos1)

                if (pos2.symbol, pos2.direction) in all_position_sets:
                    all_position_sets.remove((pos2.symbol, pos2.direction))
                ret_positions.append(pos2)

    for symbol, direction in all_position_sets:
        new_pos = PositionData()
        new_pos.symbol = symbol
        new_pos.exchange = Exchange.OKEXF.value
        new_pos.vt_symbol = get_vt_key(new_pos.symbol, new_pos.exchange)
        new_pos.direction = direction
        new_pos.position = 0
        new_pos.vt_position_id = get_vt_key(new_pos.vt_symbol, new_pos.direction)
        ret_positions.append(new_pos)

    return ret_positions


def parse_order_info(order_data, gateway_name):
    symbol = okexf_format_to_system_format(order_data["instrument_id"])
    offset, direction = TYPE_OKEXF2VT[order_data["type"]]
    order_id = order_data["client_oid"]
    order = OrderData()
    order.symbol = symbol
    order.exchange = Exchange.OKEXF.value
    order.order_id = order_id
    order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
    order.vt_symbol = get_vt_key(order.symbol, order.exchange)
    order.price = float(order_data["price"])
    order.volume = float(order_data["size"])
    order.type = ORDERTYPE_OKEXF2VT[order_data["order_type"]]
    order.direction = direction
    order.offset = offset
    order.traded = float(order_data["filled_qty"])
    order.status = STATUS_OKEXF2VT[order_data["status"]]
    order.order_time = parse_timestamp_get_str(order_data["timestamp"])
    order.gateway_name = gateway_name
    return order
