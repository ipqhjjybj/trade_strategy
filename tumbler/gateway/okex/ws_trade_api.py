# coding=utf-8

from datetime import datetime
from tumbler.function import get_vt_key
from tumbler.function import get_two_currency
from tumbler.object import (
    TradeData
)
from .ws_api_base import OkexWsApiBase
from .base import WEBSOCKET_TRADE_HOST, okex_format_symbol, parse_order_info, parse_single_account


class OkexWsTradeApi(OkexWsApiBase):

    def __init__(self, gateway):
        super(OkexWsTradeApi, self).__init__(gateway)

        self.trade_count = 0
        self.url = WEBSOCKET_TRADE_HOST

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
        self.callbacks["spot/account"] = self.on_account
        self.callbacks["spot/order"] = self.on_order

        # Subscribe to order update
        channels = []
        for symbol in self.set_all_symbols:
            instrument_id = okex_format_symbol(symbol)
            channel = "spot/order:{}".format(instrument_id)
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

        # Subscribe to account update
        channels = []
        for currency in self.set_all_currencies:
            currency = currency.upper()
            channel = "spot/account:{}".format(currency)
            channels.append(channel)

        req = {
            "op": "subscribe",
            "args": channels
        }
        self.send_packet(req)

        # Subscribe to BTC/USDT trade for keep connection alive
        req = {
            "op": "subscribe",
            "args": ["spot/trade:BTC-USDT"]
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log("OkexWsTradeApi API connect success!")
        self.login()

    def on_disconnected(self):
        self.gateway.write_log("OkexWsTradeApi API disconnected")

    def on_login(self, data):
        success = data.get("success", False)
        if success:
            self.gateway.write_log("OkexWsTradeApi API login success!")
            self.subscribe_topic()
        else:
            self.gateway.write_log("OkexWsTradeApi API login failed!")

    def on_data(self, packet):
        channel = packet["table"]
        data = packet["data"]
        callback = self.callbacks.get(channel, None)

        if callback:
            for d in data:
                callback(d)

    def on_order(self, d):
        order_id = d["client_oid"]
        # 非本系统发的单，不处理
        if not order_id:
            return
        order = parse_order_info(d, order_id, self.gateway_name)
        self.gateway.on_order(order)

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
        account = parse_single_account(d, self.gateway_name)
        self.gateway.on_account(account)
