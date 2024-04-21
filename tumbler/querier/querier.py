# coding=utf-8

from threading import Lock

from tumbler.event import EVENT_SENDER_ORDER, Event, EVENT_ACCOUNT
from tumbler.event import EVENT_POSITION, EVENT_ORDER, EVENT_DICT_ACCOUNT
from tumbler.service import MQReceiver, MQSender
import tumbler.config as config
from tumbler.function import get_from_vt_key
from tumbler.object import SubscribeRequest
from tumbler.constant import MQSubscribeType
from tumbler.apps.data_third_part.base import get_query_account_name, get_diff_type_exchange_name
from tumbler.object import OrderData
from tumbler.service import log_service_manager

from tumbler.event import EventEngine
from tumbler.engine import MainEngine
from tumbler.gateway import gateway_dict


class Querier(object):
    """
    查询账户的 资产、订单、仓位等信息，并且推送到 rabbitmq 队列
    """

    def __init__(self, _account_name, _setting, _manager):
        self.account_name = _account_name
        self.exchange = _setting["exchange"]
        self.unique = _setting["unique"]
        self.setting = _setting
        self.manager = _manager

        self.mutex = Lock()

        self.sender_md = {}

        queue_exchange_name = get_diff_type_exchange_name(MQSubscribeType.DICT_ACCOUNT.value,
                                                          account_name=self.account_name)
        self.dict_account_sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                                            user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                                            exchange=queue_exchange_name)

        self.init_receiver()
        self.init_senders()

        self.event_engine = EventEngine()
        self.event_engine.start()
        main_engine = MainEngine(self.event_engine)

        self.bind_process_event()

        self.gateway = main_engine.add_gateway(gateway_dict[self.exchange])
        self.gateway.connect(_setting["connect_setting"])
        self.subscribe_symbols()

    def subscribe_symbols(self):
        for vt_symbol in self.setting.get("vt_symbols", []):
            sub = SubscribeRequest()
            sub.symbol, sub.exchange = get_from_vt_key(vt_symbol)
            sub.vt_symbol = vt_symbol
            self.gateway.subscribe(sub)

    def init_receiver(self):
        queue_exchange_name = get_query_account_name(self.account_name, self.exchange)
        queue_unique_name = queue_exchange_name + "_" + self.unique
        receive = MQReceiver(cb=self.receive_send_order_cb, host=config.SETTINGS["MQ_SERVER"],
                             port=config.SETTINGS["MQ_PORT"],
                             user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                             exchange=queue_exchange_name, prefetch_count=0)

        receive.start(queue_unique_name)

    def init_senders(self):
        for vt_symbol in self.setting.get("vt_symbols", []):
            queue_exchange_name = get_diff_type_exchange_name(MQSubscribeType.ACCOUNT.value, vt_symbol,
                                                              self.account_name)
            sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                              user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                              exchange=queue_exchange_name)
            self.sender_md[vt_symbol] = sender

    def receive_send_order_cb(self, data):
        order = OrderData()
        order.get_from_mq_msg(data)
        log_service_manager.write_log("[receive_send_order_cb] self.account_name:{}, self.exchange:{} order:{}"
                                      .format(self.account_name, self.exchange, order.__dict__))

        if order.is_active():
            e = Event(EVENT_SENDER_ORDER, order)
            self.event_engine.put(e)

    def bind_process_event(self):
        self.event_engine.register(EVENT_POSITION, self.process_position)
        self.event_engine.register(EVENT_ACCOUNT, self.process_account)
        self.event_engine.register(EVENT_DICT_ACCOUNT, self.process_dict_account)
        self.event_engine.register(EVENT_ORDER, self.process_order)

    def process_dict_account(self, event):
        dict_account = event.data
        dict_account.account_name = self.account_name
        self.dict_account_sender.send("", dict_account.get_mq_msg())

    def process_position(self, event):
        position = event.data
        for vt_symbol, sender in self.sender_md.items():
            if position.symbol in vt_symbol:
                # log_service_manager.write_log("send:{},msg:{}".format(vt_symbol, position.get_mq_msg()))
                sender.send("", position.get_mq_msg())

    def process_account(self, event):
        account = event.data
        for vt_symbol, sender in self.sender_md.items():
            if account.account_id in vt_symbol:
                # log_service_manager.write_log("send:{},msg:{}".format(vt_symbol, account.get_mq_msg()))
                sender.send("", account.get_mq_msg())

    def process_order(self, event):
        order = event.data
        for vt_symbol, sender in self.sender_md.items():
            if order.symbol in vt_symbol:
                log_service_manager.write_log("send:{},msg:{}".format(vt_symbol, order.get_mq_msg()))
                sender.send("", order.get_mq_msg())


class QuerierManager(object):
    def __init__(self, _settings):
        self.settings = _settings

        self.acct_json = {}
        for account_name, setting in self.settings.items():
            querirer = Querier(account_name, setting, self)
            self.acct_json[account_name] = querirer
