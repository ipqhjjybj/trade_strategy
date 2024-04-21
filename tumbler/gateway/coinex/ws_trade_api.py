# coding=utf-8

import time
from tumbler.constant import Status

from tumbler.constant import (
    Exchange
)
from tumbler.object import TradeData
from tumbler.function import get_no_under_lower_symbol, get_vt_key, get_str_dt_use_timestamp
from .ws_api_base import CoinexWsApiBase
from .base import WEBSOCKET_TRADE_HOST


class CoinexWsTradeApi(CoinexWsApiBase):

    def __init__(self, gateway):
        super(CoinexWsTradeApi, self).__init__(gateway)

        self.order_manager = gateway.order_manager
        self.order_manager.push_data_callback = self.on_data

        self.url = WEBSOCKET_TRADE_HOST
        self.req_id = 2
        self.set_all_symbols = set([])

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

        self.req_id += 1
        req = {
            "id": self.req_id,
            "method": "order.subscribe",
            "params": [get_no_under_lower_symbol(x).upper() for x in list(self.set_all_symbols)]
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log("CoinexWsTradeApi connect success!")
        self.login()

    def on_login(self):
        self.gateway.write_log("CoinexWsTradeApi login success!")

        self.req_id += 1
        req = {
            "id": self.req_id,
            "method": "order.subscribe",
            "params": [get_no_under_lower_symbol(x).upper() for x in list(self.set_all_symbols)]
        }
        self.send_packet(req)

    def on_data(self, packet):
        method = packet.get("method", None)
        if "order.update" == method:
            self.on_order(packet)

    """
    on_order:{'method': 'order.update', 'params': [1, {'id': 10685244443, 'type': 1, 'side': 1, 'user': 174845, 'account': 0, 'option': 0, 'ctime': 1574988117.124922, 'mtime': 1574988117.124922, 'market': 'BCHUSDT', 'source': 'web', 'client_id': '', 'price': '250.00', 'amount': '0.01000000', 'taker_fee': '0.0010', 'maker_fee': '0.0010', 'left': '0.01000000', 'deal_stock': '0', 'deal_money': '0', 'deal_fee': '0', 'asset_fee': '0', 'fee_discount': '0.50', 'last_deal_amount': '0', 'last_deal_price': '0', 'fee_asset': 'CET'}], 'id': None}
    on_order:{'method': 'order.update', 'params': [3, {'id': 10685244443, 'type': 1, 'side': 1, 'user': 174845, 'account': 0, 'option': 0, 'ctime': 1574988117.124922, 'mtime': 1574988117.124922, 'market': 'BCHUSDT', 'source': 'web', 'client_id': '', 'price': '250.00', 'amount': '0.01000000', 'taker_fee': '0.0010', 'maker_fee': '0.0010', 'left': '0.01000000', 'deal_stock': '0', 'deal_money': '0', 'deal_fee': '0', 'asset_fee': '0', 'fee_discount': '0.50', 'last_deal_amount': '0', 'last_deal_price': '0', 'fee_asset': 'CET'}], 'id': None}
    """

    def on_order(self, data):
        int_event, order_info = data["params"]
        sys_order_id = str(order_info["id"])

        order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
        if not order:
            self.order_manager.add_push_data(sys_order_id, data)
            return

        traded_volume = float(order_info["amount"]) - float(order_info["left"])
        new_traded_volume = traded_volume - order.traded

        # Push order event
        order.traded = traded_volume

        # Push order event
        order.traded = traded_volume
        if int_event == 1:
            order.status = Status.NOTTRADED.value
        elif int_event == 2:
            order.status = Status.PARTTRADED.value
        elif int_event == 3:
            if float(order_info["left"]) > 0:
                order.status = Status.CANCELLED.value
            else:
                order.status = Status.ALLTRADED.value
        else:
            order.status = Status.REJECTED.value

        self.order_manager.on_order(order)

        # Push trade event
        if not new_traded_volume:
            return

        trade = TradeData()
        trade.symbol = order.symbol
        trade.exchange = Exchange.COINEX.value
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.order_id = order.order_id
        trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
        trade.trade_id = sys_order_id + "_" + str(time.time())
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = order.direction
        trade.type = order.type
        trade.offset = order.offset
        trade.price = float(order_info["price"])
        trade.volume = new_traded_volume
        trade.trade_time = get_str_dt_use_timestamp(order_info["ftime"], mill=1)
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)
