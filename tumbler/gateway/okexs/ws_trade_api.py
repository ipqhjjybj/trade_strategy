# coding=utf-8

from copy import copy
from datetime import datetime

from tumbler.function import get_vt_key
from tumbler.constant import (
    Direction,
    Exchange
)
from tumbler.object import (
    TradeData,
    PositionData,
)
from .ws_api_base import OkexsWsApiBase
from .base import parse_order_info, parse_account_info, parse_position_holding
from .base import WEBSOCKET_TRADE_HOST, okexs_instruments, okexs_format_symbol


class OkexsWsTradeApi(OkexsWsApiBase):

    def __init__(self, gateway):
        super(OkexsWsTradeApi, self).__init__(gateway)

        self.url = WEBSOCKET_TRADE_HOST

        self.set_all_symbols = set([])

        self.trade_count = 0

        self.callbacks = {}
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)
        self.subscribe_topic()

    def subscribe_topic(self):
        """
        Subscribe to all private topics.
        """
        self.callbacks["swap/account"] = self.on_account
        self.callbacks["swap/order"] = self.on_order
        self.callbacks["swap/position"] = self.on_position

        # Subscribe to order update
        channels = []
        for symbol in self.set_all_symbols:
            instrument_id = okexs_format_symbol(symbol)
            channel = "swap/order:{}".format(instrument_id)
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

        # Subscribe to account update
        channels = []
        for instrument_id in okexs_instruments:
            instrument_id = okexs_format_symbol(instrument_id).upper()
            channel = "swap/account:{}".format(instrument_id)
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

        channels = []
        for instrument_id in okexs_instruments:
            instrument_id = okexs_format_symbol(instrument_id).upper()
            channel = "swap/position:{}".format(instrument_id)
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log("OkexsWsTradeApi API connect success!")
        self.login()

    def on_disconnected(self):
        self.gateway.write_log("OkexsWsTradeApi API disconnected")

    def on_login(self, data):
        success = data.get("success", False)
        if success:
            self.gateway.write_log("OkexsWsTradeApi API login success!")
            self.subscribe_topic()
        else:
            self.gateway.write_log("OkexsWsTradeApi API login failed!")

    def on_data(self, packet):
        channel = packet["table"]
        data = packet["data"]
        callback = self.callbacks.get(channel, None)

        if callback:
            for d in data:
                callback(d)

    """
    .', {'status': 'NOTTRADED', 'cancel_time': '', 'direction': 'LONG', 'gateway_name': 'OKEXS', 'order_
    time': '2019-12-01 10:17:05', 'exchange': 'OKEXS', 'order_id': u'', 'traded': 0.0, 'symbol': u'bsv_u
    sd_swap', 'vt_symbol': 'bsv_usd_swap.OKEXS', 'vt_order_id': '.OKEXS', 'volume': 1.0, 'offset': 'OPEN
    ', 'type': 'LIMIT', 'price': 20.0})
    """

    def on_order(self, d):
        order = parse_order_info(d, gateway_name=self.gateway_name)

        # 不是本地发出去的单，不处理
        if not order.order_id:
            real_order_id = d["order_id"]
            return

        self.gateway.on_order(copy(order))

        trade_volume = float(d.get("last_fill_qty", 0))
        if not trade_volume:
            return

        self.trade_count += 1
        trade_id = "{}{}".format(self.connect_time, self.trade_count)

        trade = TradeData()
        trade.symbol = order.symbol
        trade.exchange = order.exchange
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.order_id = order.order_id
        trade.vt_order_id = get_vt_key(trade.order_id, order.exchange)
        trade.trade_id = trade_id
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = order.direction
        trade.type = order.type
        trade.offset = order.offset
        trade.price = float(d["last_fill_px"])
        trade.volume = float(trade_volume)
        trade.trade_time = order.order_time
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)

    def on_account(self, d):
        account = parse_account_info(d, self.gateway_name)
        self.gateway.on_account(account)

    def on_position(self, d):
        self.gateway.write_log("OkexsWsTradeApi on_position:{}".format(d))

        holdings = d["holding"]
        symbol = d["instrument_id"].lower()
        for holding in holdings:
            pos = parse_position_holding(holding=holding, symbol=symbol, gateway_name=self.gateway_name)
            self.gateway.on_position(pos)
