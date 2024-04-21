# coding=utf-8

from threading import Lock

from tumbler.gateway import bbo_exchange_map
from tumbler.gate import BaseGateway
from tumbler.service import log_service_manager
from tumbler.constant import Exchange


class BBOApiTickerProducer(object):
    def __init__(self, event_engine):
        self.event_engine = event_engine

        self.bbo_exchange_dic = {}
        self.add_exchange_lock = Lock()

    def add_bbo_exchange(self, exchange, inst_types=[]):
        with self.add_exchange_lock:
            if exchange not in self.bbo_exchange_dic.keys() and exchange in bbo_exchange_map.keys():
                g = BaseGateway(self.event_engine, exchange)
                rest = bbo_exchange_map[exchange]
                self.bbo_exchange_dic[exchange] = rest(g)

                if exchange == Exchange.OKEX5.value:
                    for inst_type in inst_types:
                        self.bbo_exchange_dic[exchange].subscribe_okex5(inst_type)
                self.bbo_exchange_dic[exchange].connect()
            else:
                log_service_manager.write_log(f"[BBOApiTickerProducer] exchange:{exchange} not in exchanges!")
