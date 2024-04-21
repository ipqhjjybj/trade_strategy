# coding=utf-8

from copy import copy
import json

from tumbler.event import Event
from tumbler.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_MERGE_TICK,
    EVENT_TRANSFER,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_SENDER_ORDER,
    EVENT_TRADE,
    EVENT_COVER_ORDER_REQ,
    EVENT_DICT_ACCOUNT,
    EVENT_REJECT_COVER_ORDER_REQ,
    EVENT_BBO_TICK
)
from tumbler.apps.data_third_part.base import (
    APP_NAME
)
import tumbler.config as config
from tumbler.service import MQReceiver
from tumbler.constant import Exchange, MQDataType
from tumbler.engine import BaseEngine
from tumbler.object import TickData, MergeTickData, TransferRequest, MQSubscribeRequest, OrderData
from tumbler.object import CoverOrderRequest, RejectCoverOrderRequest, DictAccountData
from tumbler.object import PositionData, AccountData, TradeData, BBOTickData
from tumbler.service.log_service import log_service_manager

from .base import get_diff_type_exchange_name, get_receive_unique_queue

MQ_PARSE_DICT = {
    MQDataType.BBO_TICKER.value: [BBOTickData, EVENT_BBO_TICK],
    MQDataType.TICKER.value: [TickData, EVENT_TICK],
    MQDataType.MERGE_TICKER.value: [MergeTickData, EVENT_MERGE_TICK],
    MQDataType.ORDER.value: [OrderData, EVENT_ORDER],
    MQDataType.POSITION.value: [PositionData, EVENT_POSITION],
    MQDataType.ACCOUNT.value: [AccountData, EVENT_ACCOUNT],
    MQDataType.DICT_ACCOUNT.value: [DictAccountData, EVENT_DICT_ACCOUNT],
    MQDataType.SEND_ORDER.value: [OrderData, EVENT_SENDER_ORDER],
    MQDataType.TRANSFER.value: [TransferRequest, EVENT_TRANSFER],
    MQDataType.TRADE_DATA.value: [TradeData, EVENT_TRADE],
    MQDataType.COVER_ORDER_REQUEST.value: [CoverOrderRequest, EVENT_COVER_ORDER_REQ],
    MQDataType.REJECT_COVER_ORDER_REQUEST.value: [RejectCoverOrderRequest, EVENT_REJECT_COVER_ORDER_REQ]
}


class MQGet(object):
    """ Rabbitmq 行情接收引擎 """

    def __init__(self, _event_engine):
        self.event_engine = _event_engine

        self.mq_receive_dict = {}
        self.all_vt_keys = set([])

    def is_not_subscribed(self, req: MQSubscribeRequest):
        key = req.get_key()
        if key not in self.all_vt_keys:
            self.all_vt_keys.add(key)
            return True
        else:
            return False

    @staticmethod
    def get_exchange_key(vt_symbol):
        return vt_symbol

    def _receive(self, data):
        try:
            # log_service_manager.write_log("_receive data:{}".format(data))
            data = json.loads(data)

            for key, data in data.items():
                u_class, type_event = MQ_PARSE_DICT[key]
                obj = u_class()
                obj.get_from_json(data)
                e = Event(type_event, copy(obj))
                self.event_engine.put(e)

        except Exception as ex:
            log_service_manager.write_log("[error]_receive_account_callback:{} ex:{}".format(data, ex))

    def subscribe(self, req: MQSubscribeRequest):
        if self.is_not_subscribed(req):
            exchange_queue_name = get_diff_type_exchange_name(req.subscribe_type, req.vt_symbol, req.account_name)
            log_service_manager.write_log("[subscribe] exchange_queue_name:{}".format(exchange_queue_name))
            receive = MQReceiver(cb=self._receive, host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                 user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                 exchange=exchange_queue_name,
                                 prefetch_count=0)

            self.mq_receive_dict[exchange_queue_name] = receive
            receive.start(get_receive_unique_queue(exchange_queue_name, req.unique))


class DataThirdPartEngine(BaseEngine):
    """ 行情接收引擎 """
    default_setting = {
        "ticks": [["btc_usdt", Exchange.HUOBI.value], ["btc_usdt", Exchange.GATEIO.value]],
        "merge_ticks": [["btc_usdt", Exchange.AGGREGATION.value]]
    }

    def __init__(self, main_engine, event_engine):
        super(DataThirdPartEngine, self).__init__(main_engine, event_engine, APP_NAME)

        self.mq_manager = None

    def init_engine(self):
        self.main_engine.write_log("DataThirdPartEngine is inited!")

    def subscribe(self, req):
        if self.mq_manager:
            self.mq_manager.subscribe(req)
        else:
            self.main_engine.write_log("DataThirdPartEngine subscribe {} not existed!".format(req))

    def load_from_setting(self, setting):
        self.mq_manager = MQGet(self.event_engine)
        for sub in setting:
            log_service_manager.write_log("[load sub]:{}".format(sub.__dict__))
            self.subscribe(sub)

# if key == MQDataType.TICKER.value:
#     tick = TickData()
#     tick.get_from_json(data)
#     e = Event(EVENT_TICK, copy(tick))
#     self.event_engine.put(e)
# elif key == MQDataType.MERGE_TICKER.value:
#     merge_tick = MergeTickData()
#     merge_tick.get_from_json(data)
#     e = Event(EVENT_MERGE_TICK, copy(merge_tick))
#     self.event_engine.put(e)
# elif key == MQDataType.ORDER.value:
#     log_service_manager.write_log("receive data:{}".format(data))
#     order = OrderData()
#     order.get_from_json(data)
#     e = Event(EVENT_ORDER, copy(order))
#     self.event_engine.put(e)
# elif key == MQDataType.POSITION.value:
#     position = PositionData()
#     position.get_from_json(data)
#     e = Event(EVENT_POSITION, copy(position))
#     self.event_engine.put(e)
# elif key == MQDataType.ACCOUNT.value:
#     account = AccountData()
#     account.get_from_json(data)
#     e = Event(EVENT_ACCOUNT, copy(account))
#     self.event_engine.put(e)
# elif key == MQDataType.DICT_ACCOUNT.value:
#     log_service_manager.write_log("receive data dict_account:{}".format(data))
#     account = DictAccountData()
#     account.get_from_json(data)
#     e = Event(EVENT_DICT_ACCOUNT, copy(account))
#     self.event_engine.put(e)
# elif key == MQDataType.SEND_ORDER.value:
#     log_service_manager.write_log("receive data SEND_ORDER:{}".format(data))
#     order = OrderData()
#     order.get_from_json(data)
#     e = Event(EVENT_SENDER_ORDER, copy(order))
#     self.event_engine.put(e)
# elif key == MQDataType.TRANSFER.value:
#     log_service_manager.write_log("receive data TRANSFER:{}".format(data))
#     transfer_req = TransferRequest()
#     transfer_req.get_from_json_msg(data)
#     e = Event(EVENT_TRANSFER, copy(transfer_req))
#     self.event_engine.put(e)
# elif key == MQDataType.TRADE_DATA.value:
#     log_service_manager.write_log("receive data TRADE:{}".format(data))
#     trade_data = TradeData()
#     trade_data.get_from_json_msg(data)
#     e = Event(EVENT_TRADE, copy(trade_data))
#     self.event_engine.put(e)
# elif key == MQDataType.COVER_ORDER_REQUEST.value:
#     log_service_manager.write_log("receive data COVER_ORDER:{}".format(data))
#     cover_order_data = CoverOrderRequest()
#     cover_order_data.get_from_json_msg(data)
#     e = Event(EVENT_COVER_ORDER_REQ, copy(cover_order_data))
#     self.event_engine.put(e)
# elif key == MQDataType.REJECT_COVER_ORDER_REQUEST.value:
#     log_service_manager.write_log("receive data REJECT_COVER_ORDER_REQ:{}".format(data))
#     reject_cover_order_data = RejectCoverOrderRequest()
#     reject_cover_order_data.get_from_json_msg(data)
#     e = Event(EVENT_REJECT_COVER_ORDER_REQ, copy(reject_cover_order_data))
#     self.event_engine.put(e)