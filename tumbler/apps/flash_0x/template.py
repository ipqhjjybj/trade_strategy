# coding=utf-8

from datetime import datetime
from copy import copy

from tumbler.constant import Direction, Offset, Interval
from tumbler.function import FilePrint

from tumbler.service import ding_talk_service


class Flash0xTemplate(object):
    author = ""
    class_name = "CtaTemplate"

    inited = False
    trading = False

    # 订阅行情
    vt_symbols_subscribe = []

    # 参数列表
    parameters = []

    # 运行时  重点变量列表
    variables = []

    def __init__(self, flash_engine, strategy_name, settings):
        # 设置策略的参数
        if settings:
            d = self.__dict__
            for key in self.parameters:
                if key in settings:
                    d[key] = settings[key]

        self.flash_engine = flash_engine
        self.strategy_name = strategy_name

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")

        self.file_print = FilePrint(self.strategy_name + ".log", "strategy_run_log", mode="w")
        self.important_print = FilePrint(self.strategy_name + "_important.log", "important_strategy_log", mode="a")

    def send_ding_msg(self, msg):
        msg = '{}:[{}]:{}'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), self.strategy_name, msg)
        ding_talk_service.send_msg(msg)

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
        self.write_log(msg)
        self.important_print.write('[{}]:{}'.format(self.strategy_name, msg))

    def write_log(self, msg):
        """
        Write a log message.
        """
        self.file_print.write('[{}]:{}'.format(self.strategy_name, msg))

    def load_bar(self, days, interval=Interval.MINUTE.value, callback=None):
        if not callback:
            callback = self.on_bar

        self.flash_engine.load_bar(self.vt_symbol, days, interval, callback)

    def get_contract(self, vt_symbol):
        """
        获得合约信息 , 从 engine中
        """
        return self.flash_engine.get_contract(vt_symbol)

    def get_account(self, vt_account_id, api_key=""):
        """
        获得账户的信息 , 从 engine中
        """
        return self.flash_engine.get_account(vt_account_id, api_key)

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

    def add_auction_query(self, auction_id):
        return self.flash_engine.add_auction_query(self, auction_id)

    def check_data(self, data):
        return data and str(data["code"]) == "200"

    def send_0x_order(self, symbol, direction, price, volume):
        """
        :param symbol:
        :param direction:
        :param price:
        :param volume:
        :return:
        """
        return self.flash_engine.new_flush_0x_order(symbol, direction, price, volume)

    def flush_0x_order(self, symbol, direction, price, volume):
        """
        refresh 0x order
        :param symbol:
        :param direction:
        :param price:
        :param volume:
        :return true or false:
        """
        return self.flash_engine.flush_0x_order(symbol, direction, price, volume)

    def get_orders(self, symbol, side):
        """
        get_orders
        :param symbol:
        :param side:
        :return:
        """
        return self.flash_engine.get_orders(symbol, side)

    def cancel_0x_order_by_order_id(self, order_id):
        return self.flash_engine.cancel_0x_order_by_order_id(order_id)

    def cancel_0x_order(self, symbol, direction):
        """
        cancel flash order
        :param symbol:
        :param direction:
        :return true or false:
        """
        if self.trading:
            msg = "[cancel 0x order] symbol:{}, direction:{}".format(symbol, direction)
            self.write_log(msg)
            return self.flash_engine.cancel_0x_order(symbol, direction)
        return False

    def buy(self, symbol, exchange, price, volume):
        """
        Send buy order to open a long position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.OPEN.value, price, volume)

    def sell(self, symbol, exchange, price, volume):
        """
        Send sell order to close a long position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.CLOSE.value, price, volume)

    def short(self, symbol, exchange, price, volume):
        """
        Send short order to open as short position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume)

    def cover(self, symbol, exchange, price, volume):
        """
        Send cover order to close a short position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.CLOSE.value, price, volume)

    def send_order(self, symbol, exchange, direction, offset, price, volume):
        """
        Send a new order.
        """
        if self.trading:
            vt_order_ids = self.flash_engine.send_order(self, symbol, exchange, direction, offset, price, volume)
            msg = "[send_order] vt_order_ids:{} info:{},{},{},{},{},{}".format(vt_order_ids, symbol,
                                                                               exchange, direction, offset, price,
                                                                               volume)
            self.write_log(msg)
            return vt_order_ids
        else:
            msg = "[send_order] trading condition is false!"
            self.write_log(msg)
            return [[None, None]]

    def cancel_order(self, vt_order_id):
        """
        Cancel an existing order.
        """
        if self.trading:
            msg = "[cancel order] vt_order_id:{}".format(vt_order_id)
            self.write_log(msg)
            return self.flash_engine.cancel_order(self, vt_order_id)

    def transfer_amount(self, transfer_request):
        return self.flash_engine.transfer_amount(transfer_request)

    def on_transfer(self, transfer_req):
        pass

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
            self.flash_engine.put_strategy_event(self)

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

    def on_flash_account(self, account):
        '''
        :param account:
        :return:
        更新 flash 这边情况信息
        '''
        pass

    def on_bar(self, bar):
        pass

    def on_trade(self, trade):
        """
        Callback of new trade data update.
        """
        pass

    def on_auction(self, auction):
        pass

    def on_auction_timer(self):
        pass

    def on_order(self, order):
        """
        Callback of new order data update.
        """
        pass
