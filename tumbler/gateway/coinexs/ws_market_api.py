# coding=utf-8

from datetime import datetime
from copy import copy
from tumbler.object import (
    TickData
)

from tumbler.object import SubscribeRequest
from tumbler.constant import MAX_PRICE_NUM

from tumbler.constant import (
    Exchange
)
from tumbler.function import get_no_under_lower_symbol, get_vt_key, get_format_lower_symbol, simplify_tick
from .ws_api_base import CoinexsWsApiBase

from .base import WEBSOCKET_MARKET_HOST


class CoinexsWsMarketApi(CoinexsWsApiBase):

    def __init__(self, gateway):
        super(CoinexsWsMarketApi, self).__init__(gateway)

        self.set_all_symbols = set([])
        self.url = WEBSOCKET_MARKET_HOST

        self.req_id = 0
        self.ticks = {}

        self.depth_bids_info = {}
        self.depth_asks_info = {}

    def on_connected(self):
        self.gateway.write_log("COINEXSWsMarketApi connected success!")

        # coinex 只需要一次订阅
        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.COINEXS.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)
            break

    def subscribe(self, req):
        symbol = req.symbol
        self.set_all_symbols.add(symbol)

        # Create tick data buffer
        tick = TickData()
        tick.symbol = symbol
        tick.name = symbol.replace('_', '/')
        tick.exchange = Exchange.COINEXS.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        if symbol not in self.ticks.keys():
            self.ticks[symbol] = tick
            self.depth_bids_info[symbol] = {}
            self.depth_asks_info[symbol] = {}

        for symbol in self.ticks.keys():
            req = {"id": self.req_id, "method": "depth.subscribe", "params": [get_no_under_lower_symbol(symbol).upper(), 10, "0"]}
            self.send_packet(req)

    # def subscribe(self, req):
    #     symbol = req.symbol
    #     self.set_all_symbols.add(symbol)

    #     # Create tick data buffer
    #     tick = TickData()
    #     tick.symbol = symbol
    #     tick.name = symbol.replace('_', '/')
    #     tick.exchange = Exchange.COINEXS.value
    #     tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
    #     tick.datetime = datetime.now()
    #     tick.gateway_name = self.gateway_name

    #     if symbol not in self.ticks.keys():
    #         self.ticks[symbol] = tick
    #         self.depth_bids_info[symbol] = {}
    #         self.depth_asks_info[symbol] = {}

    #     arr = []
    #     for symbol in self.ticks.keys():
    #         arr.append([get_no_under_lower_symbol(symbol).upper(), 10, "0"])

    #     self.req_id += 1
    #     req = {"id": self.req_id, "method": "depth.subscribe_multi", "params": arr}
    #     self.send_packet(req)

    def on_data(self, packet):
        method = packet.get("method", None)
        if method:
            flag, data, symbol = packet["params"]
            symbol = get_format_lower_symbol(symbol)
            if flag:
                self.depth_bids_info[symbol] = {}
                self.depth_asks_info[symbol] = {}

            dic_bids = self.depth_bids_info.get(symbol, None)
            dic_asks = self.depth_asks_info.get(symbol, None)

            if dic_bids is not None:
                last_price = data.get("last", None)

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

                if last_price is not None:
                    tick.last_price = last_price

                self.gateway.on_ws_tick(copy(tick))
