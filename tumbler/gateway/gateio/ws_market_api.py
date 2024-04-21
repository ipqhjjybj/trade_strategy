# coding=utf-8

from datetime import datetime
from copy import copy

from tumbler.constant import MAX_PRICE_NUM
from tumbler.function import get_vt_key, simplify_tick
from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData,
    SubscribeRequest
)

from .ws_api_base import GateioWsApiBase
from .base import WEBSOCKET_MARKET_HOST, change_system_format_to_gateio_format, change_gateio_format_to_system_format


class GateioWsMarketApi(GateioWsApiBase):
    """
    GATEIO WS MARKET API
    """

    def __init__(self, gateway):
        super(GateioWsMarketApi, self).__init__(gateway)

        self.set_all_symbols = set([])

        self.url = WEBSOCKET_MARKET_HOST

        self.req_id = 1
        self.ticks = {}

        self.depth_bids_info = {}
        self.depth_asks_info = {}

    def on_connected(self):
        self.gateway.write_log("GateioWsMarketApi connected success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.GATEIO.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def subscribe(self, req):
        symbol = req.symbol

        self.set_all_symbols.add(symbol)

        # Create tick data buffer
        tick = TickData()
        tick.symbol = symbol
        tick.name = symbol.replace('_','/')
        tick.exchange = Exchange.GATEIO.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        if symbol not in self.ticks.keys():
            self.ticks[symbol] = tick

            self.depth_bids_info[symbol] = {}
            self.depth_asks_info[symbol] = {}

        arr = []
        for symbol in self.set_all_symbols:
            arr.append([change_system_format_to_gateio_format(symbol).upper(), 30, "0.00000001"])

        # Subscribe to market depth update
        self.req_id += 1
        req = {'id': self.req_id, 'method': "depth.subscribe", 'params': arr}

        self.send_packet(req)

    def on_data(self, packet):
        method = packet.get("method", None)
        if method:
            if "depth.update" in method:
                params = packet.get("params", None)
                if params:
                    flag, data, symbol = params
                    symbol = change_gateio_format_to_system_format(symbol)
                    if flag:
                        self.depth_bids_info[symbol] = {}
                        self.depth_asks_info[symbol] = {}
                    self.on_market_data(data, symbol)

    def on_market_data(self, data, symbol):
        dic_bids = self.depth_bids_info.get(symbol, None)
        dic_asks = self.depth_asks_info.get(symbol, None)

        if dic_bids is not None:
            asks = data.get("asks", [])
            bids = data.get("bids", [])

            for price_str, volume_str in bids:
                dic_bids[price_str] = volume_str
                if float(volume_str) < 1e-12:
                    dic_bids.pop(price_str)

            for price_str, volume_str in asks:
                dic_asks[price_str] = volume_str
                if float(volume_str) < 1e-12:
                    dic_asks.pop(price_str)

            tick = self.ticks[symbol]
            tick.datetime = datetime.now()
            tick.compute_date_and_time()

            simplify_tick(tick, dic_bids.items(), dic_asks.items())
            self.gateway.on_ws_tick(copy(tick))

