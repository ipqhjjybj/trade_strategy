# coding=utf-8

import importlib
import os
import traceback
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from copy import copy

from tumbler.function import save_json, load_json, get_vt_key, get_from_vt_key, get_round_order_price
from tumbler.object import (
    SubscribeRequest,
    OrderRequest,
    TradeDataLog,
    StopOrder
)
from tumbler.constant import OrderType, Direction, StopOrderStatus

from tumbler.engine import BaseEngine
from tumbler.event import Event
from tumbler.event import (
    EVENT_TICK,
    EVENT_MERGE_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_BBO_TICK,

    EVENT_TRADE_LOG,
    EVENT_STRATEGY_VARIABLES_LOG
)

from tumbler.apps.alpha_trader.template import AlphaTemplate
from tumbler.apps.alpha_trader.base import (
    APP_NAME
)
from tumbler.function.convert import PositionHolding
from tumbler.service.log_service import log_service_manager
from tumbler.apps.cta_strategy.engine import get_symbol_bars, STOP_ORDER_PREFIX
from tumbler.aggregation.bbo_aggregation import BBOApiTickerProducer


class AlphaEngine(BaseEngine):
    """ 做市策略 处理引擎 """

    setting_filename = "alpha_setting.json"

    def __init__(self, main_engine, event_engine):
        super(AlphaEngine, self).__init__(main_engine, event_engine, APP_NAME)

        self.strategy_setting = {}  # strategy_name: dict

        self.classes = {}  # class_name: stategy_class
        self.strategies = {}  # strategy_name: strategy

        self.symbol_strategy_map = defaultdict(list)  # vt_symbol: strategy list
        self.bbo_exchange_strategy_map = defaultdict(list)  # vt_symbol: strategy list
        self.order_id_strategy_map = {}  # vt_order_id: strategy

        self.stop_order_count = 0  # for generating stop_order_id
        self.stop_orders = {}  # stop_order_id: stop_order

        # 下面这个  strategy_order_id_map 存在一直变大的情况
        self.strategy_order_id_map = defaultdict(set)  # strategy_name: order_id list

        self.vt_trade_ids = set()  # for filtering duplicate trade

        self.position_dic = {}  # vt_symbol:positonHolding object

        self.bbo_ticker_producer = BBOApiTickerProducer(event_engine=event_engine)

    def subscribe_bbo_exchanges(self, strategy, exchange, inst_types=[]):
        self.write_log("[alpha_engine] [subscribe_bbo_exchanges] subscribe exchange:{}".format(exchange))
        self.bbo_ticker_producer.add_bbo_exchange(exchange, inst_types)

        strategies = self.bbo_exchange_strategy_map[exchange]
        if strategy not in strategies:
            strategies.append(strategy)

    def send_order(self, strategy, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        """
        Make a request , then send to main engine
        """
        if stop:
            return self.send_stop_order(strategy, symbol, exchange, direction, offset, price, volume, stop)
        else:
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
        vt_symbol = get_vt_key(symbol, exchange)

        if vt_symbol not in self.position_dic.keys():
            contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                self.init_position_dic(contract)

        pos_obj = self.position_dic.get(vt_symbol, None)
        if pos_obj:

            self.write_log("pos_obj:{}".format(pos_obj.__dict__))
            req_list = pos_obj.get_req_list(symbol, exchange, direction, price, volume)
            ret = []
            for req in req_list:
                self.write_log("send_limit_order, req:{}".format(req.__dict__))
                vt_order_id = self.main_engine.send_order(req, exchange)
                local_order_id, exchange = get_from_vt_key(vt_order_id)
                order = req.create_order_data(local_order_id, exchange)
                order.order_id = local_order_id
                order.exchange = exchange
                ret.append([vt_order_id, order])

                self.order_id_strategy_map[vt_order_id] = strategy
                self.strategy_order_id_map[strategy.strategy_name].add(vt_order_id)

            return ret
        else:
            self.write_log("[send_order] pos_obj:{} not found!".format(vt_symbol))
            return []

    def load_bar(self, vt_symbol, days, interval, callback):
        t_symbol = get_symbol_bars(vt_symbol)
        end = datetime.now()
        start = end - timedelta(days)

        self.write_log("[load_bar] t_symbol:{} interval:{} start:{} end:{}".
                       format(t_symbol, interval, start, end))
        bars = self.main_engine.query_history(t_symbol, interval, start, end)
        self.write_log("[load_bar] len bars:{}".format(len(bars)))
        for bar in bars:
            callback(bar)

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

    def get_account(self, vt_account_id):
        """
        得到 资产余额数据
        """
        return self.main_engine.get_account(vt_account_id)

    def init_engine(self, setting={}):
        self.load_strategy_class()
        self.load_strategy_setting(setting)
        self.register_event()

        self.write_log("cta_strategy is init successily!")

    def load_strategy_class(self):
        """
        Load strategy class from source code.
        """
        path1 = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(
            path1, "tumbler.apps.alpha_trader.strategies")

        path2 = Path.cwd().joinpath("strategies")
        self.load_strategy_class_from_folder(path2, "strategies")

    def load_strategy_class_from_folder(self, path, module_name=""):
        """
        Load strategy class from certain folder.
        """
        for dirpath, dirnames, filenames in os.walk(str(path)):
            for filename in filenames:
                if filename.endswith(".py"):
                    strategy_module_name = ".".join(
                        [module_name, filename.replace(".py", "")])
                elif filename.endswith(".pyd"):
                    strategy_module_name = ".".join(
                        [module_name, filename.split(".")[0]])

                self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name):
        """
        Load strategy class from module file.
        """
        try:
            module = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if isinstance(value, type) and issubclass(value, AlphaTemplate) and value is not AlphaTemplate:
                    self.classes[value.__name__] = value
        except:
            msg = "strategy_file {} load error ：\n{}".format(module_name, traceback.format_exc())
            self.write_log(msg)

    def load_strategy_setting(self, setting={}):
        """
        Load setting file.
        """
        if not setting:
            self.strategy_setting = load_json(self.setting_filename)
        else:
            self.strategy_setting = setting
        for strategy_name, strategy_config in self.strategy_setting.items():
            self.add_strategy(strategy_config["class_name"], strategy_name, strategy_config["setting"])

    def init_all_strategies(self):
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def init_position_dic(self, contract):
        if contract.vt_symbol not in self.position_dic.keys():
            self.position_dic[contract.vt_symbol] = PositionHolding(contract)

            long_positon_key = get_vt_key(contract.vt_symbol, Direction.LONG.value)
            short_position_key = get_vt_key(contract.vt_symbol, Direction.SHORT.value)

            long_position = self.main_engine.get_position(long_positon_key)
            if long_position:
                self.position_dic[contract.vt_symbol].update_position(long_position)

            short_position = self.main_engine.get_position(short_position_key)
            if short_position:
                self.position_dic[contract.vt_symbol].update_position(short_position)

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
                self.init_position_dic(contract)

                sub = SubscribeRequest()
                sub.symbol = contract.symbol
                sub.exchange = contract.exchange
                sub.vt_symbol = vt_symbol

                self.main_engine.subscribe(sub, contract.exchange)

        strategy.inited = True
        self.write_log("{} inited! position_dic:{}".format(strategy_name, self.position_dic))

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
        # save_json(self.setting_filename, self.strategy_setting)

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

    def check_stop_order(self, tick):
        for stop_order in list(self.stop_orders.values()):
            if stop_order.vt_symbol != tick.vt_symbol:
                continue

            long_triggered = (stop_order.direction == Direction.LONG.value and tick.last_price >= stop_order.price)
            short_triggered = (stop_order.direction == Direction.SHORT.value and tick.last_price <= stop_order.price)

            if long_triggered or short_triggered:
                strategy = self.strategies[stop_order.strategy_name]

                # To get excuted immediately after stop order is
                # triggered, use limit price if available, otherwise
                # use ask_price_5 or bid_price_5
                if stop_order.direction == Direction.LONG.value:
                    price = tick.ask_prices[5]
                    if not price:
                        price = tick.ask_prices[0] * 1.002
                else:
                    price = tick.bid_prices[5]
                    if not price:
                        price = tick.bid_prices[0] * 0.998

                contract = self.main_engine.get_contract(stop_order.vt_symbol)

                price = get_round_order_price(price, contract.price_tick)

                vt_order_ids = self.send_limit_order(
                    strategy,
                    stop_order.symbol,
                    stop_order.exchange,
                    stop_order.direction,
                    stop_order.offset,
                    price,
                    stop_order.volume
                )

                # Update stop order status if placed successfully
                if vt_order_ids:
                    # Remove from relation map.
                    self.stop_orders.pop(stop_order.vt_order_id)

                    strategy_vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
                    if stop_order.vt_order_id in strategy_vt_order_ids:
                        strategy_vt_order_ids.remove(stop_order.vt_order_id)

                    # Change stop order status to cancelled and update to strategy.
                    stop_order.status = StopOrderStatus.TRIGGERED.value
                    stop_order.vt_order_ids = vt_order_ids

                    self.call_strategy_func(strategy, strategy.on_stop_order, stop_order)

                    strategy_vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
                    for vt_order_id, order in vt_order_ids:
                        strategy_vt_order_ids.add(vt_order_id)

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
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_BBO_TICK, self.process_bbo_event)

    def process_bbo_event(self, event):
        bbo_ticker = event.data

        strategies = self.bbo_exchange_strategy_map[bbo_ticker.exchange]
        if not strategies:
            return

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_bbo_tick, bbo_ticker)

    def process_position_event(self, event):
        position = event.data
        log_service_manager.write_log("[process_position_event]:{}".format(position.__dict__))

        pos_obj = self.position_dic.get(position.vt_symbol, None)
        if pos_obj:
            pos_obj.update_position(position)

    def process_tick_event(self, event):
        tick = event.data
        strategies = self.symbol_strategy_map[tick.vt_symbol]
        if not strategies:
            return

        self.check_stop_order(tick)

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_tick, tick)

    def process_merge_tick_event(self, event):
        merge_tick = event.data

        strategies = self.symbol_strategy_map[merge_tick.vt_symbol]
        if not strategies:
            return

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_merge_tick, merge_tick)

    def process_order_event(self, event):
        order = event.data
        log_service_manager.write_log("process_order_event:{}".format(order.__dict__))
        strategy = self.order_id_strategy_map.get(order.vt_order_id, None)
        if not strategy:
            return

        pos_obj = self.position_dic.get(order.vt_symbol)
        if pos_obj:
            pos_obj.update_order(order)

        # Remove vt_order_id if order is no longer active.
        vt_order_ids = self.strategy_order_id_map[strategy.strategy_name]
        if order.vt_order_id in vt_order_ids and not order.is_active():
            vt_order_ids.remove(order.vt_order_id)

            # 不与 process_trade_event 起冲突情况下，pop掉这个没有用的东西
            if order.traded < 1e-6:
                self.order_id_strategy_map.pop(order.vt_order_id)

        # Call strategy on_order function
        self.call_strategy_func(strategy, strategy.on_order, order)

    def process_trade_event(self, event):
        trade = event.data

        log_service_manager.write_log("process_trade_event:{}".format(trade.__dict__))

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
