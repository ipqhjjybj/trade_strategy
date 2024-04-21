# coding=utf-8

import json

from tumbler.function import get_vt_key, get_no_under_lower_symbol, get_str_dt_use_timestamp
from tumbler.service import log_service_manager
from tumbler.constant import (
    Exchange
)

from tumbler.object import (
    TradeData,
    SubscribeRequest
)
from .base import WEBSOCKET_TRADE_HOST, create_signature_v2
from .base import STATUS_HUOBI2VT
from .ws_api_base import HuobiWsApiBase


class HuobiWsTradeApi(HuobiWsApiBase):
    def __init__(self, gateway):
        super(HuobiWsTradeApi, self).__init__(gateway)

        self.url = WEBSOCKET_TRADE_HOST
        self.order_manager = gateway.order_manager
        self.order_manager.push_data_callback = self.on_order

        self.req_id = 0

        self.set_all_symbols = set([])

    def subscribe(self, req):
        self.set_all_symbols.add(req.symbol)

        self.req_id += 1
        send_req = {
            "action": "sub",
            "ch": "orders#{}".format(get_no_under_lower_symbol(req.symbol))
        }
        self.send_packet(send_req)

        self.gateway.write_log("HuobiWsTradeApi subscribe {}".format(send_req))
        # 下面的没必要了，因为order里已经有成交的信息了
        # self.req_id += 1
        # send_req = {
        #     "action": "sub",
        #     "ch": "trade.clearing#{}#0".format(get_no_under_lower_symbol(req.symbol))
        # }
        # self.send_packet(send_req)

    @staticmethod
    def unpack_data(data):
        return json.loads(data)

    def on_connected(self):
        self.gateway.write_log("HuobiWsTradeApi connect success!")
        self.login()

    def on_login(self):
        self.gateway.write_log("HuobiWsTradeApi login success!")

        for symbol in self.set_all_symbols:
            s = SubscribeRequest()
            s.symbol = symbol
            s.exchange = Exchange.HUOBI.value
            s.vt_symbol = get_vt_key(s.symbol, s.exchange)
            self.subscribe(s)

    def login(self):
        params = {"op": "auth"}
        params.update(create_signature_v2(self.key, "GET", self.sign_host, self.path, self.secret))
        return self.send_packet(params)

    def on_packet(self, packet):
        # log_service_manager.write_log("[on_packet] packet:{}".format(packet))
        action = packet.get("action", "")
        code = packet.get("code", -1)
        ch = packet.get("ch", "")
        if "ping" == action:
            req = {
                "action": "pong",
                "data": {
                      "ts": packet["data"]["ts"]
                }
            }
            self.send_packet(req)

        elif "req" == action:
            if int(code) == 200:
                self.on_login()

        elif action == "sub":
            log_service_manager.write_log("data : {}".format(packet))

        elif '#' in ch:
            arr = ch.split('#')
            ch_id = arr[0]
            if ch_id in ["orders", "trade.clearing"]:
                data = packet.get("data", {})
                if "orders" in ch_id:
                    self.on_order(data)
                # 下面的没必要了，因为order足够了。
                # elif "trade.clearing" in ch_id:
                #     self.on_trade(data)

    def on_order(self, data):
        # log_service_manager.write_log("[on_order] data:{}".format(data))
        '''
        2021-01-26 15:51:16,582  INFO: [on_packet] packet:
        {
            'action': 'push',
            'ch': 'orders#btmusdt',
            'data':
            {
                'accountId': 15945550,
                'orderPrice': '0.063',
                'orderSize': '200',
                'orderCreateTime': 1611647476514,
                'orderSource': 'spot-web',
                'clientOrderId': '',
                'orderId': 198939797361189,
                'orderStatus': 'submitted',
                'eventType': 'creation',
                'symbol': 'btmusdt',
                'type': 'sell-limit'
            }
        }
        2021-01-26 15:51:30,519  INFO: [on_packet] packet:{'action': 'ping', 'data': {'ts': 1611647490462}}
        2021-01-26 15:51:42,031  INFO: [on_packet] packet:
        {
            'action': 'push',
            'ch': 'orders#btmusdt',
            'data': {
                'execAmt': '0',
                'lastActTime': 1611647501971,
                'orderPrice': '0.063',
                'orderSize': '200',
                'remainAmt': '200',
                'orderSource': 'spot-web',
                'clientOrderId': '',
                'orderId': 198939797361189,
                'orderStatus': 'canceled',
                'eventType': 'cancellation',
                'symbol': 'btmusdt',
                'type': 'sell-limit'
            }
        }

        2021-01-26 15:54:05,062  INFO: [on_packet] packet:
        {
            'action': 'push',
            'ch': 'orders#btmusdt',
            'data':
            {
                'accountId': 15945550,
                'orderPrice': '0.0625',
                'orderSize': '200',
                'orderCreateTime': 1611647645003,
                'orderSource': 'spot-web',
                'clientOrderId': '',
                'orderId': 198935543983372,
                'orderStatus': 'submitted',
                'eventType': 'creation',
                'symbol': 'btmusdt',
                'type': 'sell-limit'
            }
        }
        2021-01-26 15:54:05,063  INFO: [on_packet] packet:
            {
                'action': 'push',
                'ch': 'orders#btmusdt',
                'data': {
                    'tradePrice': '0.0626',
                    'tradeVolume': '200',
                    'tradeTime': 1611647645003,
                    'aggressor': True,
                    'execAmt': '200',
                    'tradeId': 100058572685,
                    'orderPrice': '0.0625',
                    'orderSize': '200',
                    'remainAmt': '0',
                    'orderSource': 'spot-web',
                    'clientOrderId': '',
                    'orderId': 198935543983372,
                    'orderStatus': 'filled',
                    'eventType': 'trade',
                    'symbol': 'btmusdt',
                    'type': 'sell-limit'
                }
            }
        2021-01-26 15:54:05,075  INFO: [on_packet] packet:
        {
            'action': 'push',
            'ch': 'trade.clearing#btmusdt#0',
            'data': {
                'eventType': 'trade',
                'symbol': 'btmusdt',
                'orderId': 198935543983372,
                'orderSide': 'sell',
                'orderType': 'sell-limit',
                'accountId': 15945550,
                'source': 'spot-web',
                'orderPrice': '0.0625',
                'orderSize': '200',
                'orderCreateTime': 1611647644999,
                'orderStatus': 'filled',
                'feeCurrency': 'usdt',
                'tradePrice': '0.0626',
                'tradeVolume': '200',
                'aggressor': True,
                'tradeId': 100058572685,
                'tradeTime': 1611647645003,
                'transactFee': '0.002504',
                'feeDeduct': '0',
                'feeDeductType': ''
            }
        }
        :param data:
        :return:
        '''
        sys_order_id = str(data["orderId"])

        order = self.order_manager.get_order_with_sys_order_id(sys_order_id)
        if not order:
            self.order_manager.add_push_data(sys_order_id, data)
            return
        #
        traded_volume = float(data.get("tradeVolume", 0))
        order.traded += traded_volume
        order.status = STATUS_HUOBI2VT.get(data["orderStatus"], None)
        # log_service_manager.write_log("[order detail]:{}".format(order.__dict__))
        self.order_manager.on_order(order)

        # Push trade event
        if not traded_volume:
            return
        #
        trade = TradeData()
        trade.symbol = order.symbol
        trade.exchange = Exchange.HUOBI.value
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.order_id = order.order_id
        trade.vt_order_id = get_vt_key(trade.order_id, trade.exchange)
        trade.trade_id = str(data["tradeId"])
        trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
        trade.direction = order.direction
        trade.type = order.type
        trade.offset = order.offset
        trade.price = float(data["tradePrice"])
        trade.volume = float(data["tradeVolume"])
        trade.trade_time = get_str_dt_use_timestamp(data["tradeTime"])
        trade.gateway_name = self.gateway_name

        # log_service_manager.write_log("[trade detail]:{}".format(trade.__dict__))

        self.gateway.on_trade(trade)
