# coding=utf-8

from copy import copy
from datetime import datetime

from tumbler.function import get_vt_key, simplify_tick
from tumbler.object import MAX_PRICE_NUM

from tumbler.constant import (
    Exchange
)
from tumbler.object import (
    TickData,
    SubscribeRequest
)
from tumbler.service.log_service import log_service_manager

from .ws_api_base import BitfinexWsApiBase
from .base import WS_MARKET_HOST, _bitfinex_format_symbol


class BitfinexWsMarketApi(BitfinexWsApiBase):
    
    def __init__(self, gateway):
        super(BitfinexWsMarketApi, self).__init__(gateway)

        self.set_all_symbols = set([])

        self.url = WS_MARKET_HOST
        self.ticks = {}
        self.bids = {}
        self.asks = {}

    def on_connected(self):
        self.gateway.write_log("BitfinexWsMarketApi connected success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.BITFINEX.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def subscribe(self, req):
        """
        Subscribe to tick data upate.
        """
        if req.symbol not in self.set_all_symbols:
            self.set_all_symbols.add(req.symbol)

            tick = TickData()
            tick.symbol = req.symbol
            tick.name = req.symbol.replace('_','-').upper()
            tick.exchange = Exchange.BITFINEX.value
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)
            tick.datetime = datetime.now()
            tick.gateway_name = self.gateway_name
            self.ticks[req.symbol] = tick

        d = {
            "event": "subscribe",
            "channel": "book",
            "symbol": _bitfinex_format_symbol(req.symbol),
        }
        self.send_packet(d)

    def on_data_update(self, data):
        channel_id = data[0]
        channel, symbol = self.channels[channel_id]

        # Get the Tick object
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        data = data[1]
        # Update deep quote
        if channel == "book":
            bid = self.bids.setdefault(symbol, {})
            ask = self.asks.setdefault(symbol, {})

            if len(data) > 3:
                for price, count, amount in data:
                    price = float(price)
                    count = int(count)
                    amount = float(amount)

                    if amount > 0:
                        bid[price] = amount
                    else:
                        ask[price] = -amount
            else:
                price, count, amount = data
                price = float(price)
                count = int(count)
                amount = float(amount)

                if not count:
                    if price in bid:
                        del bid[price]
                    elif price in ask:
                        del ask[price]
                else:
                    if amount > 0:
                        bid[price] = amount
                    else:
                        ask[price] = -amount

            try:
                bids = []
                asks = []

                # BID
                bid_keys = bid.keys()
                bid_price_list = sorted(bid_keys, reverse=True)

                ask_keys = ask.keys()
                ask_price_list = sorted(ask_keys)

                max_num = min(MAX_PRICE_NUM, len(bid))
                for n in range(max_num):
                    price = bid_price_list[n]
                    volume = bid[price]
                    bids.append([price, volume])

                max_num = min(MAX_PRICE_NUM, len(ask))
                for n in range(max_num):
                    price = ask_price_list[n]
                    volume = ask[price]
                    asks.append([price, abs(volume)])

                simplify_tick(tick, bids, asks)
            except Exception as ex:
                log_service_manager.write_log(ex)
                return

        dt = datetime.now()
        tick.date = dt.strftime("%Y%m%d")
        tick.time = dt.strftime("%H:%M:%S.%f")
        tick.datetime = dt

        if tick.bid_prices[0] and tick.ask_prices[0]:
            self.gateway.on_ws_tick(copy(tick))
