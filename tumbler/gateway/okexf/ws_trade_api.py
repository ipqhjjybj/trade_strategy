# coding=utf-8

from copy import copy
from datetime import datetime
from tumbler.function import get_vt_key, get_two_currency

from tumbler.constant import (
    Direction,
    Exchange
)

from tumbler.object import (
    TradeData,
    PositionData
)
from .ws_api_base import OkexfWsApiBase
from .base import WEBSOCKET_TRADE_HOST
from .base import parse_single_account, parse_order_info
from .base import okexf_format_symbol, okexf_currencies, TYPE_OKEXF2VT, okexf_format_to_system_format


class OkexfWsTradeApi(OkexfWsApiBase):

    def __init__(self, gateway):
        super(OkexfWsTradeApi, self).__init__(gateway)

        self.url = WEBSOCKET_TRADE_HOST

        self.trade_count = 0

        self.set_all_symbols = set([])
        self.set_all_currencies = set([])

        self.callbacks = {}
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)
        target_symbol, base_symbol = get_two_currency(req.symbol)
        self.set_all_currencies.add(target_symbol)
        self.set_all_currencies.add(base_symbol)

        self.subscribe_topic()

    def subscribe_topic(self):
        """
        Subscribe to all private topics.
        """
        self.callbacks["futures/account"] = self.on_account
        self.callbacks["futures/order"] = self.on_order
        self.callbacks["futures/position"] = self.on_position

        # Subscribe to order update
        channels = []
        for symbol in self.set_all_symbols:
            instrument_id = okexf_format_symbol(symbol)
            channel = "futures/order:{}".format(instrument_id)
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

        # log_service_manager.write_log("OkexfWsTradeApi req:{}".format(req))

        # Subscribe to account update
        channels = []
        for currency in okexf_currencies:
            currency = currency.upper()
            channel = "futures/account:{}".format(currency.upper())
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

        # log_service_manager.write_log("OkexfWsTradeApi req:{}".format(req))

    def on_connected(self):
        self.gateway.write_log("OkexfWsTradeApi API connect success!")
        self.login()

    def on_disconnected(self):
        self.gateway.write_log("OkexfWsTradeApi API disconnected")

    def on_login(self, data):
        success = data.get("success", False)
        if success:
            self.gateway.write_log("OkexfWsTradeApi API login success!")
            self.subscribe_topic()
        else:
            self.gateway.write_log("OkexfWsTradeApi API login failed!")

    def on_data(self, packet):
        # log_service_manager.write_log("on_data:{}".format(packet))
        channel = packet["table"]
        data = packet["data"]
        callback = self.callbacks.get(channel, None)

        if callback:
            for d in data:
                callback(d)

    def on_order(self, d):
        order_id = d["client_oid"]
        # 不是系统发出去的单，不处理
        if not order_id:
            return

        order = parse_order_info(d, self.gateway_name)
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
        for key, account_data in d.items():
            account = parse_single_account(key, account_data, self.gateway_name)
            self.gateway.on_account(account)

    def on_position(self, d):
        pos = PositionData()
        pos.symbol = okexf_format_to_system_format(d["instrument_id"])
        pos.exchange = Exchange.OKEXF.value
        pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
        pos.direction = Direction.LONG.value
        pos.position = float(d["long_qty"])
        pos.frozen = float(d["long_qty"]) - float(d["long_avail_qty"])
        pos.price = float(d["long_avg_cost"])
        pos.vt_position_id = get_vt_key(pos.vt_symbol, pos.direction)

        self.gateway.on_position(pos)

        pos = PositionData()
        pos.symbol = okexf_format_to_system_format(d["instrument_id"])
        pos.exchange = Exchange.OKEXF.value
        pos.vt_symbol = get_vt_key(pos.symbol, pos.exchange)
        pos.direction = Direction.SHORT.value
        pos.position = float(d["short_qty"])
        pos.frozen = float(d["short_qty"]) - float(d["short_avail_qty"])
        pos.price = float(d["short_avail_qty"])
        pos.vt_position_id = get_vt_key(pos.vt_symbol, pos.direction)

        self.gateway.on_position(pos)
