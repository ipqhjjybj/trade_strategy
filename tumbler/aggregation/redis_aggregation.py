# coding=utf-8

from datetime import timedelta
from copy import deepcopy
from threading import Thread
from time import sleep
import os

from threading import Lock

from tumbler.aggregation import Aggregation
from tumbler.object import TickData, BBOTickData

from tumbler.service.redis_service import redis_service_manager
from tumbler.service import log_service_manager

from tumbler.event import EVENT_BBO_TICK, Event
from tumbler.function import simple_load_json, save_json
from tumbler.record.client_quick_query import get_future_symbols
from tumbler.constant import Exchange

key_redis_aggreagation_setting = "redis_aggreagation_setting"
template_json = {
    "type": "aggregation",
    "symbols": {
    },
    "exchanges": {
    },
    "in_server_stop_exchanges": []
}


def get_now_aggregation_files(folder="./"):
    ret = []
    files = os.listdir(folder)
    for file in files:
        if key_redis_aggreagation_setting in file:
            file_path = os.path.join(folder, file)
            ret.append(file_path)
    return ret


def get_now_has_symbols(folder="./"):
    symbols_ret = []
    ret = get_now_aggregation_files(folder)
    for file_path in ret:
        json_data = simple_load_json(file_path)
        symbols_dic = json_data.get("symbols", {})
        if symbols_dic:
            for symbol in symbols_dic.keys():
                symbols_ret.append(symbol)
    return symbols_ret


def make_market_data_setting(per_num=5, exchange=Exchange.HUOBIU.value, use_rest=True, use_ws=False):
    global template_json, key_redis_aggreagation_setting
    now_template_json = deepcopy(template_json)
    now_template_json["exchanges"][exchange] = {
        "key": "",
        "secret_key": "",
        "use_rest": use_rest,
        "use_ws": use_ws
    }

    def generate_func(symbols):
        tmp_json = deepcopy(now_template_json)
        for symbol in symbols:
            tmp_json["symbols"][symbol] = [exchange]
        return tmp_json

    save_setting_ret = []

    all_symbols = get_future_symbols()
    now_symbols = []
    for i in range(len(all_symbols)):
        now_symbols.append(all_symbols[i])
        if (i + 1) % per_num == 0:
            save_setting_ret.append(generate_func(now_symbols))
            now_symbols = []

    if now_symbols:
        save_setting_ret.append(generate_func(now_symbols))

    for j in range(len(save_setting_ret)):
        save_json(f".tumbler/{key_redis_aggreagation_setting}{j}.json", save_setting_ret[j])


class RedisAggregation(Aggregation):
    def __init__(self, event_engine, filename="aggregation.json"):
        super(RedisAggregation, self).__init__(event_engine, filename)
        self.redis_ticker_dict = {}

    def process_ticks(self, tick: TickData):
        super(RedisAggregation, self).process_ticks(tick)

        c_ticker = self.redis_ticker_dict.get(tick.vt_symbol, None)
        if c_ticker:
            if tick.datetime >= c_ticker.datetime + timedelta(seconds=1) and tick.bid_prices[0] > 0:
                redis_service_manager.set_ticker_to_redis(tick)
                self.redis_ticker_dict[tick.vt_symbol] = deepcopy(tick)
        else:
            self.redis_ticker_dict[tick.vt_symbol] = deepcopy(tick)


class RedisTickerProducer(object):
    def __init__(self, event_engine, interval=1, flag_produce_bbo=True):
        self.event_engine = event_engine
        self._thread = Thread(target=self._run_timer)
        self._active = True

        self.interval = interval

        self.bbo_exchanges = set([])

        self.add_exchange_lock = Lock()

        self.flag_produce_bbo = flag_produce_bbo

    def stop(self):
        self._active = False

    def add_bbo_exchange(self, exchange):
        with self.add_exchange_lock:
            self.bbo_exchanges.add(exchange)

    def _run_timer(self):
        while self._active:
            sleep(self.interval)
            try:
                if self.flag_produce_bbo:
                    with self.add_exchange_lock:
                        bbo_exchanges = list(self.bbo_exchanges)

                    for exchange in bbo_exchanges:
                        ticker_dict = redis_service_manager.get_tickers_from_redis(exchange)
                        bbo_ticker = BBOTickData.get_from_ticks_dic(ticker_dict)

                        event = Event(EVENT_BBO_TICK)
                        event.data = bbo_ticker
                        self.event_engine.put(event)

            except Exception as ex:
                log_service_manager.write_log("[RedisTickerProducer] ex:{}".format(ex))
