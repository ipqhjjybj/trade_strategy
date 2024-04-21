# coding=utf-8

from datetime import datetime
from tumbler.function import get_vt_key, get_str_dt_use_timestamp
from tumbler.function import get_two_currency
from tumbler.object import (
    TradeData
)
from .ws_api_base import Okex5WsApiBase
from .base import WEBSOCKET_PRIVATE_HOST, okex5_format_symbol, parse_order_info
from .base import get_inst_type_from_okex_symbol, get_inst_uly_from_okex_symbol
from .base import parse_single_position, parse_single_account


class Okex5WsTradeApi(Okex5WsApiBase):
    def __init__(self, gateway):
        super(Okex5WsTradeApi, self).__init__(gateway)

        self.trade_count = 0
        self.url = WEBSOCKET_PRIVATE_HOST

        self.set_all_symbols = set([])
        self.set_all_currencies = set(["btc", "usdt", "eth", "okb"])

        self.callbacks = {}
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)
        print("subscribe symbol:", req.symbol)

        self.subscribe_topic()

    def subscribe_topic(self):
        """
        Subscribe to all private topics.
        """
        self.callbacks["account"] = self.on_account
        self.callbacks["positions"] = self.on_position
        self.callbacks["orders"] = self.on_order

        # Subscribe to order update
        args = [{
             "channel": "positions",
             "instType": "ANY"
        }]
        for currency in self.set_all_currencies:
            args.append(
                {
                    "channel": "account",
                    "ccy": currency.upper()
                }
            )

        for symbol in self.set_all_symbols:
            instrument_id = okex5_format_symbol(symbol)

            inst_type = get_inst_type_from_okex_symbol(instrument_id)
            if inst_type == "OPTION":
                arg = {
                    "channel": "orders",
                    "instType": inst_type,
                    "uly": get_inst_uly_from_okex_symbol(instrument_id),
                    "instId": instrument_id
                }
            else:
                arg = {
                    "channel": "orders",
                    "instType": inst_type,
                    "instId": instrument_id
                }
            args.append(arg)

        req = {
            "op": "subscribe",
            "args": args
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log("Okex5WsTradeApi API connect success!")
        self.login()

    def on_disconnected(self):
        self.gateway.write_log("Okex5WsTradeApi API disconnected")

    '''
    {'event': 'login', 'msg': '', 'code': '0'}
    '''
    def on_login(self, data):
        code = data.get("code", -1)
        if int(code) == 0:
            self.gateway.write_log("Okex5WsTradeApi API login success!")
            self.subscribe_topic()
        else:
            self.gateway.write_log("Okex5WsTradeApi login failed! data:{}".format(data))

    def on_data(self, packet):
        if "arg" in packet.keys():
            channel = packet["arg"]["channel"]
            data = packet.get("data", [])
            if data:
                callback = self.callbacks.get(channel, None)

                if callback:
                    for d in data:
                        callback(d)
        else:
            self.gateway.write_log("Okex5WsTradeApi other on_data:{}".format(packet))

    '''
    {
        "arg": {
            "channel": "orders",
            "instType": "FUTURES",
            "uly": "BTC-USD"
        },
        "data": [{
            "instType": "FUTURES",
            "instId": "BTC-USD-200329",
            "ordId": "312269865356374016",
            "clOrdId": "b1",
            "tag": "",
            "px": "999",
            "sz": "3",
            "ordType": "limit",
            "side": "buy",
            "posSide": "long",
            "tdMode": "cross",
            "fillSz": "0",
            "fillPx": "long",
            "tradeId": "0",
            "accFillSz": "323",
            "fillTime": "0",
            "fillFee": "0.0001",
            "fillFeeCcy": "BTC",
            "execType": "T",
            "state": "canceled",
            "avgPx": "0",
            "lever": "20",
            "tpTriggerPx": "0",
            "tpOrdPx": "20",
            "slTriggerPx": "0",
            "slOrdPx": "20",
            "feeCcy": "",
            "fee": "",
            "rebateCcy": "",
            "rebate": "",
            "pnl": "",
            "category": "",
            "uTime": "1597026383085",
            "cTime": "1597026383085",
            "reqId": "",
            "amendResult": "",
            "code": "0",
            "msg": ""
        }, {
            "instType": "FUTURES",
            "instId": "BTC-USD-200829",
            "ordId": "312269865356374016",
            "clOrdId": "b1",
            "tag": "",
            "px": "999",
            "sz": "3",
            "ordType": "limit",
            "side": "buy",
            "posSide": "long",
            "tdMode": "cross",
            "fillSz": "0",
            "fillPx": "long",
            "tradeId": "0",
            "accFillSz": "323",
            "fillTime": "0",
            "fillFee": "0.0001",
            "fillFeeCcy": "BTC",
            "execType": "T",
            "state": "canceled",
            "avgPx": "0",
            "lever": "20",
            "tpTriggerPx": "0",
            "tpOrdPx": "20",
            "slTriggerPx": "0",
            "slOrdPx": "20",
            "feeCcy": "",
            "fee": "",
            "rebateCcy": "",
            "rebate": "",
            "pnl": "",
            "category": "",
            "uTime": "1597026383085",
            "cTime": "1597026383085",
            "reqId": "",
            "amendResult": "",
            "code": "0",
            "msg": ""
        }]
    }
    {'arg': {'channel': 'orders', 'instType': 'SPOT', 'instId': 'BTC-USDT'}, 'data': [{'accFillSz': '0', 'amendResult': '', 'avgPx': '', 'cTime': '1621579743001', 'category': 'normal', 'ccy': '', 'clOrdId': '', 'code': '0', 'execType': '', 'fee': '0', 'feeCcy': 'BTC', 'fillFee': '0', 'fillFeeCcy': '', 'fillPx': '', 'fillSz': '0', 'fillTime': '', 'instId': 'BTC-USDT', 'instType': 'SPOT', 'lever': '2', 'msg': '', 'ordId': '315863567702601728', 'ordType': 'limit', 'pnl': '0', 'posSide': '', 'px': '39000', 'rebate': '0', 'rebateCcy': 'USDT', 'reqId': '', 'side': 'buy', 'slOrdPx': '', 'slTriggerPx': '', 'state': 'live', 'sz': '0.001', 'tag': '', 'tdMode': 'cross', 'tpOrdPx': '', 'tpTriggerPx': '', 'tradeId': '', 'uTime': '1621579743001'}]}
    {'arg': {'channel': 'orders', 'instType': 'SPOT', 'instId': 'BTC-USDT'}, 'data': [{'accFillSz': '0', 'amendResult': '', 'avgPx': '', 'cTime': '1621579743001', 'category': 'normal', 'ccy': '', 'clOrdId': '', 'code': '0', 'execType': '', 'fee': '0', 'feeCcy': 'BTC', 'fillFee': '0', 'fillFeeCcy': '', 'fillPx': '', 'fillSz': '0', 'fillTime': '', 'instId': 'BTC-USDT', 'instType': 'SPOT', 'lever': '2', 'msg': '', 'ordId': '315863567702601728', 'ordType': 'limit', 'pnl': '0', 'posSide': '', 'px': '39000', 'rebate': '0', 'rebateCcy': 'USDT', 'reqId': '', 'side': 'buy', 'slOrdPx': '', 'slTriggerPx': '', 'state': 'canceled', 'sz': '0.001', 'tag': '', 'tdMode': 'cross', 'tpOrdPx': '', 'tpTriggerPx': '', 'tradeId': '', 'uTime': '1621579745485'}]}
    '''
    def on_order(self, d):
        order_id = d["clOrdId"]
        # 非本系统发的单，不处理
        if not order_id:
            return

        order = parse_order_info(d, order_id, self.gateway_name)
        self.gateway.on_order(order)

        fillSz = d.get("fillSz", 0)
        if fillSz == "":
            fillSz = 0
        trade_volume = float(fillSz)
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
        trade.price = float(d["fillPx"])
        trade.volume = float(trade_volume)
        trade.trade_time = get_str_dt_use_timestamp(d["fillTime"])
        trade.gateway_name = self.gateway_name

        self.gateway.on_trade(trade)

    '''
    {
      "arg": {
        "channel": "account",
        "ccy": "BTC"
      },
      "data": [
        {
          "uTime": "1597026383085",
          "totalEq": "41624.32",
          "isoEq": "3624.32",
          "adjEq": "41624.32",
          "ordFroz": "0",
          "imr": "4162.33",
          "mmr": "4",
          "notionalUsd": "",
          "mgnRatio": "41624.32",
          "details": [
            {
              "availBal": "",
              "availEq": "1",
              "ccy": "BTC",
              "cashBal": "1",
              "uTime": "1617279471503",
              "disEq": "50559.01",
              "eq": "1",
              "eqUsd": "45078.3790756226851775",
              "frozenBal": "0",
              "interest": "0",
              "isoEq": "0",
              "liab": "0",
              "maxLoan": "",
              "mgnRatio": "",
              "notionalLever": "0.0022195262185864",
              "ordFrozen": "0",
              "upl": "0",
              "uplLiab": "0",
              "crossLiab": "0",
              "isoLiab": "0"
            }
          ]
        }
      ]
    }
    '''
    def on_account(self, d):
        account_arr = parse_single_account(d, self.gateway_name)
        for account in account_arr:
            self.gateway.on_account(account)
        # account = parse_single_account(d, self.gateway_name)
        # self.gateway.on_account(account)

    '''
    {
        "arg":{
            "channel":"positions",
            "instType":"FUTURES"
        },
        "data":[
            {
                "adl":"1",
                "availPos":"1",
                "avgPx":"2566.31",
                "cTime":"1619507758793",
                "ccy":"ETH",
                "deltaBS":"",
                "deltaPA":"",
                "gammaBS":"",
                "gammaPA":"",
                "imr":"",
                "instId":"ETH-USD-210430",
                "instType":"FUTURES",
                "interest":"0",
                "last":"2566.22",
                "lever":"10",
                "liab":"",
                "liabCcy":"",
                "liqPx":"2352.8496681818233",
                "margin":"0.0003896645377994",
                "mgnMode":"isolated",
                "mgnRatio":"11.731726509588816",
                "mmr":"0.0000311811092368",
                "optVal":"",
                "pTime":"1619507761462",
                "pos":"1",
                "posCcy":"",
                "posId":"307173036051017730",
                "posSide":"long",
                "thetaBS":"",
                "thetaPA":"",
                "tradeId":"109844",
                "uTime":"1619507761462",
                "upl":"-0.0000009932766034",
                "uplRatio":"-0.0025490556801078",
                "vegaBS":"",
                "vegaPA":""
            }
        ]
    }
    '''
    def on_position(self, d):
        position_arr = parse_single_position(d, self.gateway_name)
        for position in position_arr:
            self.gateway.on_position(position)

