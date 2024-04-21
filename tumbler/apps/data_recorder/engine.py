# coding=utf-8

from threading import Thread
from queue import Queue, Empty
from copy import copy

from tumbler.engine import BaseEngine
from tumbler.function import load_json, save_json
from tumbler.object import SubscribeRequest
from tumbler.event import Event, EVENT_TICK, EVENT_CONTRACT
from tumbler.function.bar import BarGenerator
from tumbler.service import mongo_service_manager
from tumbler.service.log_service import log_service_manager

from .base import APP_NAME

EVENT_RECORDER_LOG = "eRecorderLog"
EVENT_RECORDER_UPDATE = "eRecorderUpdate"


class RecorderEngine(BaseEngine):
    setting_filename = "data_recorder_setting.json"

    def __init__(self, main_engine, event_engine):
        super(RecorderEngine, self).__init__(main_engine, event_engine, APP_NAME)

        self.queue = Queue()
        self.thread = Thread(target=self.run)
        self.active = False

        self.tick_recordings = {}
        self.bar_recordings = {}
        self.bar_generators = {}

        self.load_setting()
        self.register_event()
        self.start()
        self.put_event()

    def load_setting(self):
        setting = load_json(self.setting_filename)
        self.tick_recordings = setting.get("tick", {})
        self.bar_recordings = setting.get("bar", {})

    def save_setting(self):
        setting = {
            "tick": self.tick_recordings,
            "bar": self.bar_recordings
        }
        save_json(self.setting_filename, setting)

    def run(self):
        while self.active:
            try:
                task = self.queue.get(timeout=1)
                task_type, data = task

                if task_type == "tick":
                    mongo_service_manager.save_tick_data([data])
                elif task_type == "bar":
                    mongo_service_manager.save_bar_data([data])

            except Empty:
                continue

    def close(self):
        self.active = False

        if self.thread.isAlive():
            self.thread.join()

    def start(self):
        self.active = True
        self.thread.start()

    def add_bar_recording(self, vt_symbol):
        if vt_symbol in self.bar_recordings:
            self.write_log("Already in k_line lists:{}".format(vt_symbol))
            return

        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log("Not found contract:{}".format(vt_symbol))
            return

        self.bar_recordings[vt_symbol] = {
            "symbol": contract.symbol,
            "exchange": contract.exchange.value,
            "gateway_name": contract.gateway_name
        }

        self.subscribe(contract)
        self.save_setting()
        self.put_event()

        self.write_log("add k-line successily!{}".format(vt_symbol))

    def add_tick_recording(self, vt_symbol):
        if vt_symbol in self.tick_recordings:
            self.write_log("Already in tick lists: {}".format(vt_symbol))
            return

        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log("not found contract:{}".format(vt_symbol))
            return

        self.tick_recordings[vt_symbol] = {
            "symbol": contract.symbol,
            "exchange": contract.exchange.value,
            "gateway_name": contract.gateway_name
        }

        self.subscribe(contract)
        self.save_setting()
        self.put_event()

        self.write_log("add tick record successily! {}".format(vt_symbol))

    def remove_bar_recording(self, vt_symbol):
        if vt_symbol not in self.bar_recordings:
            self.write_log("not in k-line list:{}".format(vt_symbol))
            return

        self.bar_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log("remove k-line success: {}".format(vt_symbol))

    def remove_tick_recording(self, vt_symbol):
        if vt_symbol not in self.tick_recordings:
            self.write_log("not in tick list:{}".format(vt_symbol))
            return

        self.tick_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log("remove tick record success:{}".format(vt_symbol))

    def register_event(self):
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def process_tick_event(self, event):
        tick = event.data

        if tick.vt_symbol in self.tick_recordings:
            self.record_tick(tick)

        if tick.vt_symbol in self.bar_recordings:
            bg = self.get_bar_generator(tick.vt_symbol)
            bg.update_tick(tick)

    def process_contract_event(self, event):
        contract = event.data
        vt_symbol = contract.vt_symbol

        if vt_symbol in self.tick_recordings or vt_symbol in self.bar_recordings:
            self.subscribe(contract)

    def write_log(self, msg):
        event = Event(EVENT_RECORDER_LOG, msg)
        self.event_engine.put(event)

    def put_event(self):
        tick_symbols = list(self.tick_recordings.keys())
        tick_symbols.sort()

        bar_symbols = list(self.bar_recordings.keys())
        bar_symbols.sort()

        data = {
            "tick": tick_symbols,
            "bar": bar_symbols
        }

        event = Event(
            EVENT_RECORDER_UPDATE,
            data
        )
        self.event_engine.put(event)

    def record_tick(self, tick):
        task = ("tick", copy(tick))
        self.queue.put(task)

    def record_bar(self, bar):
        task = ("bar", copy(bar))
        log_service_manager.write_log(
            "{},{},{}".format(bar.datetime.strftime("%Y-%m-%d %H:%M:%S"), bar.high_price, bar.low_price))
        self.queue.put(task)

    def get_bar_generator(self, vt_symbol):
        bg = self.bar_generators.get(vt_symbol, None)

        if not bg:
            bg = BarGenerator(self.record_bar)
            self.bar_generators[vt_symbol] = bg

        return bg

    def subscribe(self, contract):
        req = SubscribeRequest()
        req.symbol = contract.symbol
        req.exchange = contract.exchange
        req.vt_symbol = contract.vt_symbol

        self.main_engine.subscribe(req, contract.gateway_name)
