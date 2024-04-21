# coding=utf-8

import importlib
import os
import traceback
from copy import copy
from pathlib import Path
from collections import defaultdict

from tumbler.function import load_json, get_vt_key, get_from_vt_key
from tumbler.object import (
    SubscribeRequest,
    OrderRequest,
    TradeDataLog,
    StopOrder,
    FlashCancelRequest
)
from tumbler.constant import OrderType, StopOrderStatus, Direction, Exchange, Offset

from tumbler.engine import BaseEngine
from tumbler.event import Event
from tumbler.event import (
    EVENT_TICK,
    EVENT_MERGE_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_ACCOUNT,
    EVENT_TRANSFER,
    EVENT_AUCTION,
    EVENT_AUCTION_TIMER,

    EVENT_TRADE_LOG,
    EVENT_STRATEGY_VARIABLES_LOG
)

from tumbler.apps.flash_0x.template import Flash0xTemplate
from tumbler.apps.flash_0x.base import (
    APP_NAME,
    STOP_ORDER_PREFIX
)


class Flash0xEngine(BaseEngine):
    """ 做市策略 处理引擎 """

    setting_filename = "flash_maker_setting.json"

    def __init__(self, main_engine, event_engine):
        super(Flash0xEngine, self).__init__(main_engine, event_engine, APP_NAME)

        self.strategy_setting = {}  # strategy_name: dict

        self.classes = {}  # class_name: stategy_class
        self.strategies = {}  # strategy_name: strategy

        self.symbol_strategy_map = defaultdict(list)  # vt_symbol: strategy list
        self.account_strategy_map = defaultdict(list)
        self.order_id_strategy_map = {}  # vt_order_id: strategy

        self.stop_order_count = 0  # for generating stop_order_id
        self.stop_orders = {}  # stop_order_id: stop_order

        self.strategy_order_id_map = defaultdict(set)  # strategy_name: order_id list

        self.vt_trade_ids = set()  # for filtering duplicate trade

        self.auction_id_strategy_map = {}

    def add_auction_query(self, strategy, auction_id):
        self.auction_id_strategy_map[auction_id] = strategy
        return self.main_engine.add_auction_query(auction_id, Exchange.LOAN.value)

    def flush_0x_order(self, symbol, direction, price, volume):
        """
        flush_0x_order
        :param symbol:
        :param direction:
        :param price:
        :param volume:
        :return True or false:
        """
        req = OrderRequest()
        req.symbol = symbol
        req.exchange = Exchange.FLASH.value
        req.vt_symbol = get_vt_key(symbol, req.exchange)
        req.price = price
        req.volume = volume
        req.type = OrderType.LIMIT.value
        req.direction = direction
        if req.direction == Direction.LONG.value:
            req.offset = Offset.OPEN.value
        else:
            req.offset = Offset.CLOSE.value

        ret = self.main_engine.send_order(req, Exchange.FLASH.value)
        return ret

    def get_orders(self, symbol, side):
        return self.main_engine.get_orders(symbol, side)

    def cancel_0x_order_by_order_id(self, order_id):
        return self.main_engine.cancel_0x_order_by_order_id(order_id)

    def new_flush_0x_order(self, symbol, direction, price, volume):
        req = OrderRequest()
        req.symbol = symbol
        req.exchange = Exchange.FLASH.value
        req.vt_symbol = get_vt_key(symbol, req.exchange)
        req.price = price
        req.volume = volume
        req.type = OrderType.LIMIT.value
        req.direction = direction
        if req.direction == Direction.LONG.value:
            req.offset = Offset.OPEN.value
        else:
            req.offset = Offset.CLOSE.value

        ret = self.main_engine.send_order(req, Exchange.FLASH.value)
        if isinstance(ret, list):
            return ret
        else:
            return False

    def cancel_0x_order(self, symbol, direction):
        """
        :param symbol:
        :param direction:
        :return True or false:
        """
        req = FlashCancelRequest()
        req.symbol = symbol
        req.exchange = Exchange.FLASH.value
        req.vt_symbol = get_vt_key(symbol, req.exchange)
        req.direction = direction
        return self.main_engine.cancel_order(req, Exchange.FLASH.value)

    def transfer_amount(self, transfer_request):
        return self.main_engine.transfer_amount(transfer_request, transfer_request.from_exchange)

    def send_order(self, strategy, symbol, exchange, direction, offset, price, volume):
        """
        Make a request , then send to main engine
        """
        return self.send_limit_order(strategy, symbol, exchange, direction, offset, price, volume)

    def send_stop_order(self, strategy, symbol, exchange, direction, offset, price, volume, stop):
        self.stop_order_count += 1
        stop_order_id = "{}_{}".format(STOP_ORDER_PREFIX, self.stop_order_count)

        stop_order = StopOrder()
        stop_order.symbol = symbol
        stop_order.exchange = exchange
        stop_order.vt_symbol = get_vt_key(stop_order.symbol, stop_order.exchange)
        stop_order.direction = direction
        stop_order.offset = offset
        stop_order.price = price
        stop_order.volume = volume
        stop_order.vt_order_id = stop_order_id
        stop_order.strategy_name = strategy.strategy_name

        self.stop_orders[stop_order.vt_order_id] = stop_order

        vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
        vt_order_ids.add(stop_order.vt_order_id)

        self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)

        ret = [[stop_order_id, copy(stop_order)]]
        return ret

    def send_limit_order(self, strategy, symbol, exchange, direction, offset, price, volume):
        req = OrderRequest()
        req.symbol = symbol
        req.exchange = exchange
        req.vt_symbol = get_vt_key(symbol, exchange)
        req.price = price
        req.volume = volume
        req.type = OrderType.LIMIT.value
        req.direction = direction
        req.offset = offset

        vt_order_id = self.main_engine.send_order(req, exchange)
        if vt_order_id:
            self.order_id_strategy_map[vt_order_id] = strategy
            self.strategy_order_id_map[strategy.strategy_name].add(vt_order_id)

            if isinstance(vt_order_id, bool):
                return vt_order_id
            else:
                local_order_id, exchange = get_from_vt_key(vt_order_id)

                ret = []
                order = req.create_order_data(local_order_id, exchange)
                ret.append([vt_order_id, copy(order)])
                return ret

        return [[None, None]]

    def write_log(self, msg, strategy=None):
        """
        Create cta engine log event.
        """
        if strategy:
            msg = "{}: {}".format(strategy.strategy_name, msg)

        self.main_engine.write_log(msg)

    def get_contract(self, vt_symbol):
        """
        Get contract data from vt_symbol
        """
        return self.main_engine.get_contract(vt_symbol)

    def get_account(self, vt_account_id, api_key=""):
        """
        得到 资产余额数据
        """
        return self.main_engine.get_account(vt_account_id, api_key)

    def init_engine(self, strategy_setting={}):
        self.load_strategy_class()
        self.load_strategy_setting(strategy_setting)

        self.register_event()

        self.write_log("market_maker is init successily!")

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        path1 = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(
            path1, "tumbler.apps.flash_0x.strategies")

        path2 = Path.cwd().joinpath("strategies")
        self.load_strategy_class_from_folder(path2, "strategies")

    def load_strategy_class_from_folder(self, path, module_name=""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(str(path)):
            for filename in filenames:
                strategy_module_name = None
                if filename.endswith(".py"):
                    strategy_module_name = ".".join(
                        [module_name, filename.replace(".py", "")])
                elif filename.endswith(".pyd"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])

                if strategy_module_name:
                    self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name):
        """
        Load strategy class from module file.
        """
        try:
            module = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (isinstance(value, type) and issubclass(value,
                                                           Flash0xTemplate) and value is not Flash0xTemplate):
                    self.classes[value.__name__] = value
        except:
            msg = "strategy_file {} load error ：\n{}".format(module_name, traceback.format_exc())
            self.write_log(msg)

    def load_strategy_setting(self, strategy_setting={}):
        """
        Load setting file.
        """
        if not strategy_setting:
            self.strategy_setting = load_json(self.setting_filename)
        else:
            self.strategy_setting = strategy_setting

        for strategy_name, strategy_config in self.strategy_setting.items():
            self.add_strategy(
                strategy_config["class_name"],
                strategy_name,
                strategy_config["setting"]
            )

    def init_all_strategies(self):
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def init_strategy(self, strategy_name):
        strategy = self.strategies[strategy_name]

        if strategy.inited:
            self.write_log("{} has inited!".format(strategy_name))
            return

        # Call on_init function of strategy
        self.call_strategy_func(strategy, strategy.on_init)

        # Subscribe market data
        for vt_symbol in strategy.vt_symbols_subscribe:
            contract = self.main_engine.get_contract(vt_symbol)

            if contract is not None:
                sub = SubscribeRequest()
                sub.symbol = contract.symbol
                sub.exchange = contract.exchange
                sub.vt_symbol = vt_symbol

                self.main_engine.subscribe(sub, contract.exchange)

        strategy.inited = True
        self.write_log("{} inited!".format(strategy_name))

    def add_strategy(self, class_name, strategy_name, setting):
        """
        Add a new strategy
        """
        if strategy_name in self.strategies:
            self.write_log("create strategy failed, name:{} is existed!".format(strategy_name))
            return

        strategy_class = self.classes.get(class_name, None)
        if not strategy_class:
            self.write_log("not found class:{}".format(class_name))
            return

        strategy = strategy_class(self, strategy_name, setting)
        self.strategies[strategy_name] = strategy

        vt_symbols_subscribe = setting.get("vt_symbols_subscribe", [])
        # Add vt_symbol to strategy map.
        for vt_symbol in vt_symbols_subscribe:
            strategies = self.symbol_strategy_map[vt_symbol]
            strategies.append(strategy)

        target_symbol = setting.get("target_symbol", None)
        if target_symbol:
            strategies = self.account_strategy_map[target_symbol]
            strategies.append(strategy)

        base_symbol = setting.get("base_symbol", None)
        if base_symbol:
            strategies = self.account_strategy_map[base_symbol]
            strategies.append(strategy)

        # Update to setting file.
        self.update_strategy_setting(strategy_name, setting)

    def update_strategy_setting(self, strategy_name, setting):
        """
        Update setting file.
        """
        strategy = self.strategies[strategy_name]

        self.strategy_setting[strategy_name] = {
            "class_name": strategy.__class__.__name__,
            "setting": setting,
        }

    def put_strategy_event(self, strategy):
        """
        Put an event to update strategy status.
        """
        data = strategy.get_data()
        event = Event(EVENT_STRATEGY_VARIABLES_LOG, data)
        self.event_engine.put(event)

    def start_all_strategies(self):
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def start_strategy(self, strategy_name):
        """
        Start a strategy.
        """
        strategy = self.strategies[strategy_name]
        if not strategy.inited:
            self.write_log("strategy:{} start failed, please init first!".format(strategy.strategy_name))
            return

        if strategy.trading:
            self.write_log("{} has started , please not run again!".format(strategy_name))
            return

        self.call_strategy_func(strategy, strategy.on_start)
        strategy.trading = True

    def stop_all_strategies(self):
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def stop_strategy(self, strategy_name):
        """
        Stop a strategy.
        """
        strategy = self.strategies[strategy_name]
        if not strategy.trading:
            return

        # Call on_stop function of the strategy
        self.call_strategy_func(strategy, strategy.on_stop)

        # Change trading status of strategy to False
        strategy.trading = False

        # Cancel all orders of the strategy
        self.cancel_all(strategy)

    def cancel_all(self, strategy):
        """
        Cancel all active orders of a strategy.
        """
        vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
        if not vt_order_ids:
            return

        for vt_order_id in copy(vt_order_ids):
            self.cancel_order(strategy, vt_order_id)

    def cancel_order(self, strategy, vt_order_id):
        if vt_order_id.startswith(STOP_ORDER_PREFIX):
            self.cancel_stop_order(strategy, vt_order_id)
        else:
            self.cancel_limit_order(strategy, vt_order_id)

    def cancel_stop_order(self, strategy, vt_order_id):
        """
        Cancel stop order by vt_order_id
        """
        stop_order = self.stop_orders.get(vt_order_id, None)
        if not stop_order:
            return
        strategy = self.strategies[stop_order.strategy_name]

        self.stop_orders.pop(stop_order.vt_order_id)

        vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
        if vt_order_id in vt_order_ids:
            vt_order_ids.remove(vt_order_id)

        stop_order.status = StopOrderStatus.CANCELLED.value
        self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)

        if not stop_order.is_active():
            self.stop_orders.pop(vt_order_id)

    def cancel_limit_order(self, strategy, vt_order_id):
        """
        Cancel existing order by vt_order_id.
        """
        order = self.main_engine.get_order(vt_order_id)
        if not order:
            self.write_log("cancel order failed! not found order:{}".format(vt_order_id), strategy)
            return

        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.exchange)

    def close(self):
        self.stop_all_strategies()

    def call_strategy_func(self, strategy, func, params=None):
        """
        Call function of a strategy and catch any exception raised.
        """
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg = "occour error! \n{}".format(traceback.format_exc())
            self.write_log(msg, strategy)

    def register_event(self):
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_MERGE_TICK, self.process_merge_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)
        self.event_engine.register(EVENT_TRANSFER, self.process_transfer_event)
        self.event_engine.register(EVENT_AUCTION, self.process_auction_event)
        self.event_engine.register(EVENT_AUCTION_TIMER, self.process_auction_timer_event)

    def process_transfer_event(self, event):
        transfer = event.data
        strategy_name = transfer.from_strategy_name

        strategy = self.strategies.get(strategy_name, None)
        if strategy:
            self.call_strategy_func(strategy, strategy.on_transfer, transfer)

    def process_account_event(self, event):
        try:
            account_data = event.data
            strategies = self.account_strategy_map[account_data.account_id]
            if not strategies:
                return

            if account_data.gateway_name in [Exchange.FLASH.value, Exchange.SUPER.value]:
                for strategy in strategies:
                    if strategy.inited:
                        self.call_strategy_func(strategy, strategy.on_flash_account, account_data)
        except Exception as ex:
            self.write_log("[process_account_event] ex:{}".format(ex))

    def process_tick_event(self, event):
        try:
            tick = event.data
            strategies = self.symbol_strategy_map[tick.vt_symbol]
            if not strategies:
                return

            for strategy in strategies:
                if strategy.inited:
                    self.call_strategy_func(strategy, strategy.on_tick, tick)
        except Exception as ex:
            self.write_log("[process_tick_event] ex:{}".format(ex))

    def process_merge_tick_event(self, event):
        try:
            merge_tick = event.data
            strategies = self.symbol_strategy_map[merge_tick.vt_symbol]
            if not strategies:
                return

            for strategy in strategies:
                if strategy.inited:
                    self.call_strategy_func(strategy, strategy.on_merge_tick, merge_tick)
        except Exception as ex:
            self.write_log("[process_merge_tick_event] ex:{}".format(ex))

    def process_order_event(self, event):
        try:
            order = event.data
            strategy = self.order_id_strategy_map.get(order.vt_order_id, None)
            if not strategy:
                return

            # Remove vt_order_id if order is no longer active.
            vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
            if order.vt_order_id in vt_order_ids and not order.is_active():
                vt_order_ids.remove(order.vt_order_id)

                # 不与 process_trade_event 起冲突情况下，pop掉这个没有用的东西
                if order.traded < 1e-6:
                    self.order_id_strategy_map.pop(order.vt_order_id)

            # Call strategy on_order function
            self.call_strategy_func(strategy, strategy.on_order, order)
        except Exception as ex:
            self.write_log("[process_order_event] ex:{}".format(ex))

    def process_auction_event(self, event):
        try:
            auction = event.data
            self.write_log("[process_auction_event] auction:{}".format(auction.__dict__))
            self.write_log("[process_auction_event] auction_id_strategy_map:{}".format(self.auction_id_strategy_map.keys()))
            strategy = self.auction_id_strategy_map.get(auction.auction_id, None)
            if strategy:
                strategy.on_auction(auction)
                if not auction.is_active():
                    self.auction_id_strategy_map.pop(auction.auction_id)
        except Exception as ex:
            self.write_log("[process_auction_event] ex:{}".format(ex))

    def process_auction_timer_event(self, event):
        try:
            for strategy_name, strategy in self.strategies.items():
                strategy.on_auction_timer()
        except Exception as ex:
            self.write_log("[process_auction_timer_event] ex:{}".format(ex))

    def process_trade_event(self, event):
        trade = event.data
        try:
            # Filter duplicate trade push
            if trade.vt_trade_id in self.vt_trade_ids:
                return
            self.vt_trade_ids.add(trade.vt_trade_id)

            strategy = self.order_id_strategy_map.get(trade.vt_order_id, None)
            if not strategy:
                return

            # strategy = self.strategies[list(self.strategies.keys())[0]]

            t = TradeDataLog()
            t.symbol = trade.symbol
            t.exchange = trade.exchange
            t.vt_symbol = trade.vt_symbol
            t.trade_id = trade.trade_id
            t.vt_trade_id = trade.vt_trade_id
            t.order_id = trade.order_id
            t.vt_order_id = trade.vt_order_id
            t.direction = trade.direction
            t.offset = trade.offset
            t.price = trade.price
            t.volume = trade.volume
            t.trade_time = trade.trade_time
            t.datetime = trade.datetime
            t.gateway_name = trade.gateway_name
            t.strategy_name = strategy.strategy_name

            event = Event(EVENT_TRADE_LOG, t)
            self.event_engine.put(event)

            self.call_strategy_func(strategy, strategy.on_trade, trade)
        except Exception as ex:
            self.write_log("[process_trade_event] ex:{}".format(ex))
