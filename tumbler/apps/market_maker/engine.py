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
    StopOrder
)
from tumbler.constant import OrderType, StopOrderStatus, Direction

from tumbler.engine import BaseEngine
from tumbler.event import Event
from tumbler.event import (
    EVENT_TICK,
    EVENT_MERGE_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_TRANSFER,
    EVENT_DICT_ACCOUNT,
    EVENT_TRADE_LOG,
    EVENT_REJECT_COVER_ORDER_REQ,
    EVENT_COVER_ORDER_REQ,
    EVENT_STRATEGY_VARIABLES_LOG,
    EVENT_BBO_TICK
)

from tumbler.apps.market_maker.template import MarketMakerTemplate
from tumbler.apps.market_maker.base import (
    APP_NAME,
    STOP_ORDER_PREFIX
)
from tumbler.object import BBOTickData

from tumbler.service import log_service_manager


class MakerMakerEngine(BaseEngine):
    """ 做市策略 处理引擎 """

    setting_filename = "market_maker_setting.json"

    def __init__(self, main_engine, event_engine):
        super(MakerMakerEngine, self).__init__(main_engine, event_engine, APP_NAME)

        self.strategy_setting = {}  # strategy_name: dict

        self.classes = {}  # class_name: stategy_class
        self.strategies = {}  # strategy_name: strategy

        self.symbol_strategy_map = defaultdict(list)  # vt_symbol: strategy list
        self.account_dict_strategy_map = defaultdict(list)  # btc.HUOBI: strategy list

        self.bbo_strategy_map = defaultdict(list)       # bbo ticker map
        self.trade_strategy_map = defaultdict(list)     # 交易列表
        self.cover_order_req_map = defaultdict(list)    # 回补订单列表
        self.reject_cover_order_req_map = defaultdict(list)  # 拒单列表

        self.order_id_strategy_map = {}  # vt_order_id: strategy

        self.stop_order_count = 0  # for generating stop_order_id
        self.stop_orders = {}  # stop_order_id: stop_order

        self.strategy_order_id_map = defaultdict(set)  # strategy_name: order_id list
        self.vt_trade_ids = set()  # for filtering duplicate trade

    def send_mq_msg(self, exchange_name, msg):
        return self.main_engine.send_mq_msg(exchange_name, msg)

    def transfer_amount(self, transfer_request):
        return self.main_engine.transfer_amount(transfer_request, transfer_request.from_exchange)

    def send_order(self, strategy, symbol, exchange, direction, offset, price, volume, stop=False):
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

    def get_account(self, vt_account_id):
        """
        得到 资产余额数据
        """
        return self.main_engine.get_account(vt_account_id)

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
            path1, "tumbler.apps.market_maker.strategies")

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
                                                           MarketMakerTemplate) and value is not MarketMakerTemplate):
                    self.classes[value.__name__] = value
        except:
            msg = "strategy_file {} load error ：\n{}".format(module_name, traceback.format_exc())
            self.write_log(msg)

    def load_strategy_setting(self, strategy_setting={}):
        """
        Load setting file.
        """
        self.strategy_setting = strategy_setting
        if not strategy_setting:
            self.strategy_setting = load_json(self.setting_filename)

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

        vt_trade_subscribe = setting.get("vt_trade_subscribe", [])
        for vt_symbol in vt_trade_subscribe:
            strategies = self.trade_strategy_map[vt_symbol]
            strategies.append(strategy)

        vt_cover_order_req_subscribe = setting.get("vt_cover_order_req_subscribe", [])
        for vt_symbol in vt_cover_order_req_subscribe:
            strategies = self.cover_order_req_map[vt_symbol]
            strategies.append(strategy)

        vt_reject_cover_order_req_subscribe = setting.get("vt_reject_cover_order_req_subscribe", [])
        for vt_symbol in vt_reject_cover_order_req_subscribe:
            strategies = self.reject_cover_order_req_map[vt_symbol]
            strategies.append(strategy)

        vt_account_name_subscribe = setting.get("vt_account_name_subscribe", [])
        for acct in vt_account_name_subscribe:
            strategies = self.account_dict_strategy_map[acct]
            strategies.append(strategy)

        vt_bbo_exchange_subscribe = setting.get("vt_bbo_subscribe", [])
        for bbo_exchange in vt_bbo_exchange_subscribe:
            strategies = self.bbo_strategy_map[bbo_exchange]
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
                if stop_order.direction == Direction.LONG:
                    price = tick.ask_prices[5]
                    if not price:
                        price = tick.ask_prices[0] * 1.01
                else:
                    price = tick.bid_prices[5]
                    if not price:
                        price = tick.bid_prices[0] * 0.99

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
        except Exception as ex:
            strategy.trading = False
            strategy.inited = False

            msg = "occour error! \n{} ex:{}".format(traceback.format_exc(), ex)
            self.write_log(msg, strategy)

    def register_event(self):
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_MERGE_TICK, self.process_merge_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_TRANSFER, self.process_transfer_event)
        self.event_engine.register(EVENT_DICT_ACCOUNT, self.process_dict_account_event)
        self.event_engine.register(EVENT_REJECT_COVER_ORDER_REQ, self.process_reject_cover_order_event)
        self.event_engine.register(EVENT_COVER_ORDER_REQ, self.process_cover_order_event)
        self.event_engine.register(EVENT_BBO_TICK, self.process_bbo_order_event)

    def process_bbo_order_event(self, event):
        bbo_ticker: BBOTickData = event.data
        strategies = self.bbo_strategy_map[bbo_ticker.exchange]
        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_bbo_tick, copy(bbo_ticker))

    def process_dict_account_event(self, event):
        dict_account = event.data
        strategies = self.account_dict_strategy_map[dict_account.account_name]
        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_dict_account, copy(dict_account))

    def process_transfer_event(self, event):
        transfer = event.data
        strategy_name = transfer.from_strategy_name

        strategy = self.strategies.get(strategy_name, None)
        if strategy:
            self.call_strategy_func(strategy, strategy.on_transfer, transfer)

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

    def process_reject_cover_order_event(self, event):
        reject_order_req = event.data
        strategies = self.reject_cover_order_req_map[reject_order_req.vt_symbol]
        if not strategies:
            return

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_reject_cover_order_request, reject_order_req)

    def process_cover_order_event(self, event):
        cover_order_req = event.data
        self.write_log("[process_cover_order_event] cover_order_req:{}".format(cover_order_req.__dict__))
        strategies = self.cover_order_req_map[cover_order_req.vt_symbol]
        if not strategies:
            return

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_cover_order_request, cover_order_req)

    def process_trade_event(self, event):
        trade = event.data
        self.write_log("[process_trade_event]:{}".format(trade.__dict__))

        # Filter duplicate trade push
        if trade.vt_trade_id in self.vt_trade_ids:
            return
        self.vt_trade_ids.add(trade.vt_trade_id)

        strategies = self.trade_strategy_map[trade.vt_symbol]
        if not strategies:
            return

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

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_trade, trade)

                t.strategy_name = strategy.strategy_name
                event = Event(EVENT_TRADE_LOG, t)
                self.event_engine.put(event)
