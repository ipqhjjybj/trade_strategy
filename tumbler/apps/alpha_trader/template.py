# coding=utf-8

from datetime import datetime
from copy import copy
import os

from tumbler.constant import Direction, Offset, Interval
from tumbler.function import FilePrint, get_from_vt_key
from tumbler.function import get_folder_path, simple_load_json, save_json
from tumbler.data.binance_data import BinanceClient
from tumbler.service import ding_talk_service
from tumbler.service.mysql_service import MysqlService
import tumbler.function.risk as risk


class DataFetchTemplate(object):
    def __init__(self, strategy_name):
        self.strategy_name = strategy_name
        self.mysql_service_manager = MysqlService()
        self.binance_client = BinanceClient()

        self.fetch_print = FilePrint(self.strategy_name + ".log", "data_fetch_strategy_run_log", mode="w")

    def write_log(self, msg):
        self.fetch_print.write('{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                   self.strategy_name, msg))


class AlphaTemplate(object):
    author = ""
    class_name = "AlphaTemplate"

    symbol_pair = "btc_usd_swap"
    exchange = "HUOBIU"
    vt_symbol = "btc_usdt.HUOBIU"
    pos = 0

    exchange_info = {}

    inited = False
    trading = False

    # 订阅行情
    vt_symbols_subscribe = []

    # 参数列表
    parameters = []

    # 运行时  重点变量列表
    variables = []

    def __init__(self, cta_engine, strategy_name, settings):
        # 设置策略的参数
        if settings:
            d = self.__dict__
            for key in self.parameters:
                if key in settings:
                    d[key] = settings[key]

        self.cta_engine = cta_engine
        self.strategy_name = strategy_name

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")

        self.mysql_service_manager = MysqlService()

        self.file_print = FilePrint(self.strategy_name + ".log", "strategy_run_log", mode="w")
        self.important_print = FilePrint(self.strategy_name + "_important.log", "important_strategy_log", mode="w")

        if not os.path.exists(get_folder_path("positions")):
            os.mkdir(get_folder_path("positions"))
        self.positions_save_file_path = os.path.join(get_folder_path("positions"), self.strategy_name + ".json")

        self.time_send_ding_msg = risk.TimeWork(60)

    def load_current_positions(self):
        return simple_load_json(self.positions_save_file_path)

    def flush_current_positions(self, position_dic):
        for key, val in list(position_dic.items()):
            if abs(val) < 1e-12:
                del position_dic[key]
        save_json(self.positions_save_file_path, position_dic)

    def load_bars_from_mysql(self, vt_symbol, period, start_datetime, end_datetime):
        symbol, exchange = get_from_vt_key(vt_symbol)
        ori_symbol = symbol
        if "_swap" in symbol:
            symbol = symbol.replace('usdt_swap', 'usdt').replace('usd_swap', 'usdt')
        ret_bars = self.mysql_service_manager.get_bars(
            symbols=[symbol], period=period, start_datetime=start_datetime, end_datetime=end_datetime)
        if ori_symbol != symbol:
            for bar in ret_bars:
                bar.symbol = ori_symbol
                bar.vt_symbol = vt_symbol
        return ret_bars

    def subscribe_bbo_exchanges(self, exchange, inst_types=[]):
        self.write_log("[AlphaTemplate] [subscribe_bbo_exchanges] go!")
        self.cta_engine.subscribe_bbo_exchanges(self, exchange, inst_types)

    def update_setting(self, setting):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def write_important_log(self, msg):
        """
        Write important log
        """
        self.important_print.write(msg)

    def write_log(self, msg):
        """
        Write a log message.
        """
        self.file_print.write(
            '{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.strategy_name, msg))

    def load_server_bars(self, vt_symbols: list, days, interval=Interval.DAY.value, callback=None):
        if not callback:
            callback = self.on_bar

        for vt_symbol in vt_symbols:
            self.write_log("[load_bar] vt_symbol:{} interval:{} days:{}".format(vt_symbol, interval, days))
            self.cta_engine.load_bar(vt_symbol, days, interval, callback)

    def load_bar(self, days, interval=Interval.MINUTE.value, callback=None):
        if not callback:
            callback = self.on_bar

        self.write_log("[load_bar] vt_symbol:{} interval:{} days:{}".format(self.vt_symbol, interval, days))
        self.cta_engine.load_bar(self.vt_symbol, days, interval, callback)

    def get_contract(self, vt_symbol):
        """
        获得合约信息 , 从 engine中
        """
        return self.cta_engine.get_contract(vt_symbol)

    def get_account(self, vt_account_id):
        """
        获得账户的信息 , 从 engine中
        """
        return self.cta_engine.get_account(vt_account_id)

    def batch_orders(self, reqs):
        """
        send batch orders to system
        """
        return []

    def cancel_orders(self, vt_order_ids):
        """
        cancel batch orders
        """
        pass

    def buy(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send buy order to open a long position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.OPEN.value, price, volume, stop, lock)

    def sell(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send sell order to close a long position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.CLOSE.value, price, volume, stop, lock)

    def short(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send short order to open as short position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume, stop, lock)

    def cover(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send cover order to close a short position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.CLOSE.value, price, volume, stop, lock)

    def send_order(self, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        """
        Send a new order.
        """
        if self.trading:
            if stop:
                vt_order_ids = self.cta_engine.send_order(self, symbol, exchange, direction,
                                                          offset, price, volume, stop=True)
                msg = "[send_stop_order] vt_order_ids:{} info:{},{},{},{},{},{}" \
                    .format(vt_order_ids, symbol, exchange, direction, offset, price, volume)
                self.write_log(msg)
                return vt_order_ids
            else:
                vt_order_ids = self.cta_engine.send_order(self, symbol, exchange, direction, offset, price, volume)
                msg = "[send_order] vt_order_ids:{} info:{},{},{},{},{},{}" \
                    .format(vt_order_ids, symbol, exchange, direction,
                            offset, price, volume)
                self.write_log(msg)
                return vt_order_ids
        else:
            msg = "[send_order] trading condition is false!"
            self.write_log(msg)
            return []

    def cancel_order(self, vt_order_id):
        """
        Cancel an existing order.
        """
        if self.trading:
            msg = "[cancel order] vt_order_id:{}".format(vt_order_id)
            self.write_log(msg)
            self.cta_engine.cancel_order(self, vt_order_id)

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "strategy_name": self.strategy_name,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def put_event(self):
        """
        Put an strategy data event for ui update.
        """
        if self.inited:
            self.cta_engine.put_strategy_event(self)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    def on_merge_tick(self, merge_tick):
        """
        Callback of new merge tick data update.
        """
        pass

    def on_tick(self, tick):
        """
        Callback of new tick data update.
        """
        pass

    def on_bbo_tick(self, bbo_tick):
        """
        Callback of new bbo data update.
        """
        pass

    def on_bar(self, bar):
        """
        Callback on_bar
        """
        pass

    def on_trade(self, trade):
        """
        Callback of new trade data update.
        """
        pass

    def on_order(self, order):
        """
        Callback of new order data update.
        """
        pass

    def send_ding_msg(self, msg):
        if self.time_send_ding_msg.can_work():
            msg = '{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), self.strategy_name, msg)
            ding_talk_service.send_msg(msg)



