# coding=utf-8

from copy import copy
from datetime import datetime
from tumbler.function import get_vt_key, get_format_lower_symbol, get_no_under_lower_symbol
from tumbler.function import get_dt_use_timestamp, simplify_tick
from tumbler.constant import Direction

from tumbler.constant import (
    Exchange,
)

from tumbler.object import (
    TickData,
    SubscribeRequest,
    MarketTradeData
)
from .base import WEBSOCKET_MARKET_HOST
from .ws_api_base import HuobiWsApiBase


class HuobiWsMarketApi(HuobiWsApiBase):

    def __init__(self, gateway, ws_market_trade=False, ori_market_msg=False):
        super(HuobiWsMarketApi, self).__init__(gateway)

        self.ws_market_trade = ws_market_trade
        self.ws_ori_market_msg = ori_market_msg

        self.set_all_symbols = set([])
        self.url = WEBSOCKET_MARKET_HOST

        self.req_id = 0
        self.ticks = {}

    def on_connected(self):
        self.gateway.write_log("HuobiWsMarketApi connected success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.HUOBI.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def subscribe(self, req):
        symbol = req.symbol

        self.set_all_symbols.add(symbol)

        # Create tick data buffer
        tick = TickData()
        tick.symbol = symbol
        tick.name = symbol.replace('_', '/')
        tick.exchange = Exchange.HUOBI.value
        tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
        tick.datetime = datetime.now()
        tick.gateway_name = self.gateway_name

        if symbol not in self.ticks.keys():
            self.ticks[symbol] = tick

        # Subscribe to market depth update
        self.req_id += 1
        req = {
            "sub": "market.{}.depth.step0".format(get_no_under_lower_symbol(str(symbol))),
            "id": str(self.req_id)
        }

        self.send_packet(req)

        # Subscribe to market detail update
        # self.req_id += 1
        # req = {
        #     "sub": "market.{}.detail".format(get_no_under_lower_symbol(str(symbol))),
        #     "id": str(self.req_id)
        # }
        # self.send_packet(req)

        if self.ws_market_trade:
            self.req_id += 1
            req = {
                "sub": "market.{}.trade.detail".format(get_no_under_lower_symbol(str(symbol)))
            }
            self.send_packet(req)

    def on_data(self, packet):
        channel = packet.get("ch", None)
        if channel:
            if "depth.step" in channel:
                self.on_market_depth(packet)
                if self.ws_ori_market_msg:
                    self.gateway.on_ori_msg(packet)
            elif "trade.detail" in channel:
                self.on_trade_detail(packet)
                if self.ws_ori_market_msg:
                    self.gateway.on_ori_msg(packet)
            elif "detail" in channel:
                self.on_market_detail(packet)
                if self.ws_ori_market_msg:
                    self.gateway.on_ori_msg(packet)
        elif "err-code" in packet:
            code = packet["err-code"]
            msg = packet["err-msg"]
            self.gateway.write_log("on_data,error_code:{}, error_information:{}".format(code, msg))

    def on_market_depth(self, data):
        """行情深度推送 """
        symbol = data["ch"].split(".")[1]
        symbol = get_format_lower_symbol(symbol)

        tick = self.ticks[symbol]
        tick.datetime = get_dt_use_timestamp(data["ts"])
        simplify_tick(tick, data["tick"]["bids"], data["tick"]["asks"])

        self.gateway.on_ws_tick(copy(tick))

    def on_trade_detail(self, data):
        """成交深度推送"""
        symbol = data["ch"].split(".")[1]
        symbol = get_format_lower_symbol(symbol)

        trade = MarketTradeData()
        trade.symbol = symbol
        trade.exchange = Exchange.HUOBI.value
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)

        ret_arr = []
        arr = data["tick"]["data"]
        for dic in arr:
            side = dic["direction"] == "buy"
            if side == "buy":
                side = Direction.LONG.value
            else:
                side = Direction.SHORT.value
            # [price, volume, direction, ts]
            ret_arr.append((dic["price"], dic["amount"], side, str(dic["ts"])))

        trade.info_arr = ret_arr
        self.gateway.on_market_trade(trade)

    def on_market_detail(self, data):
        """市场细节推送"""
        pass
        # symbol = data["ch"].split(".")[1]
        # symbol = get_format_lower_symbol(symbol)
        # tick = self.ticks[symbol]
        # tick.datetime = get_dt_use_timestamp(data["ts"])
        #
        # tick_data = data["tick"]
        # tick.open_price = float(tick_data["open"])
        # tick.high_price = float(tick_data["high"])
        # tick.low_price = float(tick_data["low"])
        # tick.last_price = float(tick_data["close"])
        # tick.volume = float(tick_data["vol"])
        #
        # if tick.bid_prices[0]:
        #     self.gateway.on_ws_tick(copy(tick))
