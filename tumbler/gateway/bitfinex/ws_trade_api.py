# coding=utf-8

import time
from copy import copy
from tumbler.function import get_vt_key, get_format_lower_symbol
from tumbler.function import get_str_dt_use_timestamp

from tumbler.constant import (
    Direction,
    Exchange,
    OrderType,
    Status,
    Offset
)
from tumbler.object import (
    OrderData,
    TradeData,
    AccountData
)
from .ws_api_base import BitfinexWsApiBase
from .base import WS_TRADE_HOST, ORDER_TYPE_VT2BITFINEX, _bitfinex_format_symbol, STATUS_BITFINEX2VT


class BitfinexWsTradeApi(BitfinexWsApiBase):

    def __init__(self, gateway):
        super(BitfinexWsTradeApi, self).__init__(gateway)

        self.order_manager = gateway.order_manager

        self.trade_id = 0
        self.order_id = 0
        self.url = WS_TRADE_HOST

        self.set_all_symbols = set([])
        self.set_all_currencies = set([])

        self.connect_time = 0

    def on_connected(self):
        self.gateway.write_log("BitfinexWsTradeApi API connect success!")
        self.login()

    def on_disconnected(self):
        self.gateway.write_log("BitfinexWsTradeApi API disconnected!")

    def subscribe(self, req):
        if req.symbol not in self.set_all_symbols:
            self.set_all_symbols.add(req.symbol)

    def _gen_unqiue_cid(self):
        self.order_id += 1
        local_oid = int(time.strftime("%y%m%d%H%M%S")) + self.order_id
        return int(local_oid)

    def send_order(self, req):
        order_id = self._gen_unqiue_cid()

        if req.direction == Direction.LONG.value:
            amount = req.volume
        else:
            amount = -req.volume

        self.order_manager.add_order_id(str(order_id))
        o = {
            "cid": order_id,
            "type": ORDER_TYPE_VT2BITFINEX[req.type],
            "symbol": _bitfinex_format_symbol(req.symbol),
            "amount": str(amount),
            "price": str(req.price),
        }

        request = [0, "on", None, o]

        order = req.create_order_data(order_id, self.gateway_name)
        self.send_packet(request)

        self.gateway.on_order(order)
        return order.vt_order_id

    def cancel_order(self, req):
        order_id = req.order_id
        date_str = "20" + str(order_id)[0:6]
        date = date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:8]

        request = [
            0,
            "oc",
            None,
            {
                "cid": int(order_id),
                "cid_date": date
            }
        ]

        self.send_packet(request)

    def on_trade_update(self, data):
        name = data[1]
        info = data[2]

        if name == "ws":
            for l in info:
                self.on_wallet(l)
        elif name == "wu":
            self.on_wallet(info)
        elif name == "os":
            for l in info:
                self.on_order(l)
        elif name in ["on", "ou", "oc"]:
            self.on_order(info)
        elif name == "te":
            self.on_trade(info)
        elif name == "n":
            self.on_order_error(info)

    def on_wallet(self, data):
        if str(data[0]) == "exchange":
            # available 不为 null时才返回
            if data[4]:
                account = AccountData()
                account.account_id = str(data[1]).lower()
                account.vt_account_id = get_vt_key(self.gateway_name, account.account_id)
                account.balance = float(data[2])
                account.available = float(data[4])
                account.frozen = account.balance - account.available

                account.gateway_name = self.gateway_name
                self.gateway.on_account(copy(account))

    def on_order(self, data):
        real_order_id = str(data[0])
        order_id = str(data[2])

        if data[7] > 0:
            direction = Direction.LONG.value
            offset = Offset.OPEN.value
        else:
            direction = Direction.SHORT.value
            offset = Offset.CLOSE.value

        order_status = str(data[13].split("@")[0]).replace(" ", "")
        if order_status == "CANCELED":
            order_time = get_str_dt_use_timestamp(data[5])
        else:
            order_time = get_str_dt_use_timestamp(data[4])

        order = OrderData()
        order.symbol = get_format_lower_symbol(data[3].replace("t", ""))
        order.exchange = Exchange.BITFINEX.value
        order.order_id = order_id
        order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
        order.vt_symbol = get_vt_key(order.symbol, order.exchange)
        order.price = float(data[16])
        order.volume = abs(data[7])
        order.type = OrderType.LIMIT.value
        order.direction = direction
        order.offset = offset
        order.traded = abs(data[7]) - abs(data[6])
        order.status = STATUS_BITFINEX2VT[order_status]
        order.order_time = order_time
        order.gateway_name = self.gateway_name

        if not self.order_manager.has_order_id(order_id):
            if self.gateway.rest_trade_api and order.is_active():
                self.gateway.rest_trade_api.cancel_system_order(real_order_id)
            return

        self.gateway.on_order(order)

        if not order.is_active():
            self.order_manager.remove_order_id(order_id)

    def on_trade(self, data):
        self.trade_id += 1

        if data[4] > 0:
            direction = Direction.LONG.value
            offset = Offset.OPEN.value
        else:
            direction = Direction.SHORT.value
            offset = Offset.CLOSE.value

        trade = TradeData()
        trade.symbol = get_format_lower_symbol((data[1].replace("t", "")))
        trade.exchange = Exchange.BITFINEX.value
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.order_id = data[-1]
        trade.vt_order_id = get_vt_key(trade.order_id, Exchange.BITFINEX.value)
        trade.trade_id = str(self.trade_id)
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = direction
        trade.type = OrderType.LIMIT.value
        trade.offset = offset
        trade.price = data[5]
        trade.volume = abs(data[4])
        trade.trade_time = get_str_dt_use_timestamp(data[2])
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)

    def on_order_error(self, d):
        if d[-2] != "ERROR":
            return

        data = d[4]
        error_info = d[-1]

        # Filter cancel of non-existing order
        order_id = data[2]
    
        self.order_manager.remove_order_id(str(order_id))
        if not order_id:
            self.gateway.write_log("cancel order failed , info:{}!".format(error_info))
            return

        if data[6] > 0:
            direction = Direction.LONG.value
            offset = Offset.OPEN.value
        else:
            direction = Direction.SHORT.value
            offset = Offset.CLOSE.value

        order = OrderData()
        order.symbol = get_format_lower_symbol(data[3].replace("t", ""))
        order.exchange = Exchange.BITFINEX.value
        order.order_id = order_id
        order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
        order.vt_symbol = get_vt_key(order.symbol, order.exchange)
        order.price = float(data[16])
        order.volume = abs(data[6])
        order.type = OrderType.LIMIT.value
        order.direction = direction
        order.offset = offset
        order.traded = 0
        order.status = Status.REJECTED.value
        order.order_time = get_str_dt_use_timestamp(d[0])
        order.gateway_name = self.gateway_name

        self.gateway.on_order(copy(order))

        self.gateway.write_log("on_order_error , info:{}!".format(error_info))
