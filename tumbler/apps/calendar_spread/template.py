# coding=utf-8

from datetime import datetime
from copy import copy

from tumbler.constant import Direction, Offset
from tumbler.function import FilePrint
from tumbler.object import OrderData


class CalendarSpreadTemplate(object):
    author = ""
    class_name = "CalendarSpreadTemplate"

    inited = False
    trading = False

    # 订阅行情 
    vt_symbols_subscribe = []
    
    # 参数列表
    parameters = []

    # 运行时  重点变量列表
    variables = []

    def __init__(self, cs_engine, strategy_name, settings):
        # 设置策略的参数
        if settings:
            d = self.__dict__
            for key in self.parameters:
                if key in settings:
                    d[key] = settings[key]

        self.cs_engine = cs_engine
        self.strategy_name = strategy_name

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")

        self.file_print = FilePrint(self.strategy_name + ".log", "strategy_run_log", mode="w")

    def update_setting(self, setting):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def write_log(self, msg):
        """
        Write a log message.
        """
        self.file_print.write('{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.strategy_name, msg))

    def get_contract(self, vt_symbol):
        """
        获得合约信息 , 从 engine中
        """
        return self.cs_engine.get_contract(vt_symbol)

    def get_account(self, vt_account_id):
        """
        获得账户的信息 , 从 engine中
        """
        return self.cs_engine.get_account(vt_account_id)

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
                vt_order_ids = self.cs_engine.send_order(self, symbol, exchange, direction, offset, price, volume, stop=True,lock=lock)
                msg = "[send_stop_order] vt_order_ids:{} info:{},{},{},{},{},{}".format(vt_order_ids, symbol,
                     exchange, direction, offset, price, volume)
                self.write_log(msg)
                return vt_order_ids 
            else:
                vt_order_ids = self.cs_engine.send_order(self, symbol, exchange, direction, offset, price, volume)
                msg = "[send_order] vt_order_ids:{} info:{},{},{},{},{},{}".format(vt_order_ids, symbol,
                     exchange, direction, offset, price, volume)
                self.write_log(msg)
                return vt_order_ids
        else:
            msg = "[send_order] trading condition is false!"
            self.write_log(msg)
            return [None, None]

    def cancel_order(self, vt_order_id):
        """
        Cancel an existing order.
        """
        if self.trading:
            self.cs_engine.cancel_order(self, vt_order_id)

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
            self.cs_engine.put_strategy_event(self)

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

    def on_tick(self, tick ):
        """
        Callback of new tick data update.
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

