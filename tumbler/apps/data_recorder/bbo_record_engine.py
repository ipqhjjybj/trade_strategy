# coding=utf-8

from threading import Thread
from queue import Queue, Empty
from datetime import datetime

from tumbler.function.bar import BarGenerator
from tumbler.service.mysql_service import MysqlService
from tumbler.event import EventEngine, EVENT_BBO_TICK, EVENT_TIMER
from tumbler.service.log_service import log_service_manager
from tumbler.aggregation.bbo_aggregation import BBOApiTickerProducer
from tumbler.constant import Interval
from tumbler.function.future_manager import UsdtContractManager
import tumbler.function.risk as risk

EVENT_RECORDER_LOG = "eRecorderLog"
EVENT_RECORDER_UPDATE = "eRecorderUpdate"


class RecordSymbolExchange(object):
    def __init__(self, super_engine, contract, hour_window=1, day_window=1):
        self.super_engine = super_engine
        self.contract = contract

        self.tick_decorder = risk.TickDecorder(contract.vt_symbol, self)

        self.hour_bg = BarGenerator(self.on_bar, window=hour_window, on_window_bar=self.on_hour_bar,
                                    interval=Interval.HOUR.value, quick_minute=1)

        self.day_bg = BarGenerator(None, window=day_window, on_window_bar=self.on_day_bar,
                                   interval=Interval.DAY.value, quick_minute=1)

    def on_tick(self, tick):
        self.tick_decorder.update_tick(tick)
        if self.tick_decorder.is_tick_ok():
            self.hour_bg.update_tick(tick)
        else:
            self.super_engine.write_log(f"[on_tick] {self.contract.vt_symbol} tick not ok:"
                                        f" tick.datetime:{tick.datetime}, now:{datetime.now()}")

    def on_bar(self, bar):
        self.write_log("[on_bar] bar:{}".format(bar.get_line()))
        self.hour_bg.update_bar(bar)
        self.day_bg.update_bar(bar)

        self.super_engine.on_bar(bar)

    def on_hour_bar(self, bar):
        self.super_engine.on_hour_bar(bar)

    def on_day_bar(self, bar):
        self.super_engine.on_day_bar(bar)

    def write_log(self, msg):
        self.super_engine.write_log(msg)

    def write_important_log(self, msg):
        self.super_engine.write_important_log(msg)


class BBORecordEngine(object):
    def __init__(self):
        self.event_engine = EventEngine()
        self.event_engine.start()

        self.queue = Queue()
        self.thread = Thread(target=self.run)
        self.active = False

        self.bbo_ticker_producer = BBOApiTickerProducer(event_engine=self.event_engine)

        self.symbol_exchanges_dic = {}

        self.mysql_service_manager = MysqlService()

        self.register_event()

        self.update_timer = risk.TimeWork(180)

        self.future_manager_dic = {}

        self.start()

    def subscribe_bbo_exchanges(self, exchange):
        log_service_manager.write_log("[BBORecordEngine] [subscribe_bbo_exchanges] subscribe exchange:{}"
                                      .format(exchange))
        self.bbo_ticker_producer.add_bbo_exchange(exchange)

        self.future_manager_dic[exchange] = UsdtContractManager(exchange, self)

        self.update_futures()

    def register_event(self):
        self.event_engine.register(EVENT_BBO_TICK, self.process_bbo_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def run(self):
        record_save_worker = risk.TimeWork(60)
        period_arr_dic = {}
        while self.active:
            try:
                task = self.queue.get(timeout=1)
                period, data = task

                if period not in period_arr_dic:
                    period_arr_dic[period] = []
                arr = period_arr_dic[period]
                arr.append(data)

                log_service_manager.write_log(f"[BBORecordEngine] [run] cache_bars:{period} {data.get_line()}")

                if record_save_worker.can_work():
                    for period, arr in period_arr_dic.items():
                        if arr:
                            log_service_manager.write_log(f"[BBORecordEngine] [run] insert_bars:{period} ")
                            self.mysql_service_manager.insert_bars(arr, period)
                            arr.clear()
            except Empty:
                continue

    def close(self):
        self.active = False

        if self.thread.isAlive():
            self.thread.join()

    def start(self):
        self.active = True
        self.thread.start()

    def update_futures(self):
        for exchange, future_manager in self.future_manager_dic.items():
            future_manager.run_update()

            for vt_symbol in future_manager.get_all_vt_symbols():
                if vt_symbol not in self.symbol_exchanges_dic.keys():
                    contract = future_manager.get_contract(vt_symbol)
                    se = RecordSymbolExchange(self, contract, hour_window=1, day_window=1)
                    self.symbol_exchanges_dic[vt_symbol] = se

    def process_timer_event(self, event):
        if self.update_timer.can_work():
            self.update_futures()

    def process_bbo_event(self, event):
        bbo_ticker = event.data

        tickers = bbo_ticker.get_tickers()
        for tick in tickers:
            if tick.vt_symbol in self.symbol_exchanges_dic.keys():
                self.symbol_exchanges_dic[tick.vt_symbol].on_tick(tick)
            # else:
            #     log_service_manager.write_log(f"[process_bbo_event] has new tick:{tick.vt_symbol}!")

    def on_bar(self, bar):
        self.queue.put((Interval.MINUTE.value, bar))

    def on_hour_bar(self, bar):
        self.queue.put((Interval.HOUR.value, bar))

    def on_day_bar(self, bar):
        self.queue.put((Interval.DAY.value, bar))

    def write_log(self, msg):
        log_service_manager.write_log(msg)

    def write_important_log(self, msg):
        log_service_manager.write_log("important:{}".format(msg))
