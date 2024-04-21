# coding=utf-8

import os
from copy import copy
from threading import Lock

from tumbler.event import Event, EventEngine
from tumbler.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG
)
from tumbler.function import TRADER_DIR, get_from_vt_key
from tumbler.constant import Exchange
from tumbler.object import LogData
from tumbler.service.mongo_service import mongo_service_manager
from tumbler.service.log_service import log_service_manager


class MainEngine:
    """
    Acts as the core of tumbler
    """

    def __init__(self, event_engine):
        if event_engine:
            self.event_engine = event_engine
        else:
            self.event_engine = EventEngine()
        self.event_engine.start()

        self.gateways = {}
        self.engines = {}
        self.apps = {}
        self.exchanges = []

        os.chdir(str(TRADER_DIR))  # Change working directory
        self.init_engines()  # Initialize function engines

    def add_engine(self, engine_class):
        """
        Add function engine.
        """
        engine = engine_class(self, self.event_engine)
        self.engines[engine.engine_name] = engine
        return engine

    def add_gateway(self, gateway_class):
        """
        Add gateway.
        """
        gateway = gateway_class(self.event_engine)
        self.gateways[gateway.gateway_name] = gateway

        # Add gateway supported exchanges into engine
        for exchange in gateway.exchanges:
            if exchange not in self.exchanges:
                self.exchanges.append(exchange)

        return gateway

    def add_app(self, app_class):
        """
        Add app.
        """
        app = app_class()
        self.apps[app.app_name] = app

        engine = self.add_engine(app.engine_class)
        return engine

    def init_engines(self):
        """
        Init all engines.
        """
        self.add_engine(StoreEngine)
        self.add_engine(LogEngine)
        self.add_engine(HotEngine)

    def write_log(self, msg, source=""):
        """
        Put log event with specific message.
        """
        log = LogData()
        log.msg = msg
        log.gateway_name = source

        event = Event(EVENT_LOG, log)
        self.event_engine.put(event)

    def get_gateway(self, gateway_name):
        """
        Return gateway object by name.
        """
        gateway = self.gateways.get(gateway_name, None)
        if not gateway:
            self.write_log("Not found gateway:{}".format(gateway_name))
        return gateway

    def get_engine(self, engine_name):
        """
        Return engine object by name.
        """
        engine = self.engines.get(engine_name, None)
        if not engine:
            self.write_log("Not found engine:{}".format(engine_name))
        return engine

    def get_default_setting(self, gateway_name):
        """
        Get default setting dict of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.get_default_setting()

    def get_all_gateway_names(self):
        """
        Get all names of gateways added in main engine.
        """
        return list(self.gateways.keys())

    def get_all_apps(self):
        """
        Get all app objects.
        """
        return list(self.apps.values())

    def get_all_exchanges(self):
        """
        Get all exchanges.
        """
        return self.exchanges

    def get_contract(self, vt_symbol):
        """
        Get contract data.
        """
        engine = self.get_engine("contract")
        if engine:
            return engine.get_contract(vt_symbol)

    def connect(self, setting, gateway_name):
        """
        Start connection of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.connect(setting)

    def subscribe(self, req, gateway_name):
        """
        Subscribe tick data update of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.subscribe(req)

    def unsubscribe(self, req, gateway_name):
        """
        UnSubscribe tick data update of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.unsubscribe(req)

    def cancel_order(self, req, gateway_name):
        """
        Send cancel order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.cancel_order(req)

    def transfer_amount(self, req, gateway_name):
        """
        Send transfer asset request
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.transfer_amount(req)

    def merge_utxo(self, req, gateway_name):
        """
        Send transfer asset request
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.merge_utxo(req)

    def add_auction_query(self, auction_id, gateway_name):
        """
        Add auction query
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.add_auction(auction_id)

    def send_order(self, req, gateway_name):
        """
        Send new order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_order(req)

    def send_orders(self, reqs, gateway_name):
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_orders(reqs)

    def get_orders(self, symbol, side):
        """
        get_orders
        """
        gateway = self.get_gateway(Exchange.FLASH.value)
        if gateway:
            return gateway.get_orders(symbol, side)

    def cancel_0x_order_by_order_id(self, order_id):
        gateway = self.get_gateway(Exchange.FLASH.value)
        if gateway:
            return gateway.cancel_0x_order_by_order_id(order_id)

    def cancel_orders(self, reqs, gateway_name):
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.cancel_orders(reqs)

    def query_history(self, vt_symbol, interval, start, end):
        symbol, exchange = get_from_vt_key(vt_symbol)
        bars = mongo_service_manager.load_bar_data(symbol, exchange, interval, start, end)
        return bars

    def close(self):
        """
        Make sure every gateway and app is closed properly before
        programme exit.
        """
        # Stop event engine first to prevent new timer event.
        self.event_engine.stop()

        for engine in self.engines.values():
            engine.close()

        for gateway in self.gateways.values():
            gateway.close()


class BaseEngine(object):
    """
    class for implementing an function engine.
    """

    def __init__(
            self,
            main_engine,
            event_engine,
            engine_name,
    ):
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.engine_name = engine_name

    def close(self):
        pass


class StoreEngine(BaseEngine):
    """
    Process save contract、account data, save , get from event_engine
    """

    def __init__(self, main_engine, event_engine):
        super(StoreEngine, self).__init__(main_engine, event_engine, "contract")

        self.accounts = {}
        self.contracts = {}

        self.mutex = Lock()

        self.add_function()
        self.register_event()

    def add_function(self):
        self.main_engine.get_contract = self.get_contract
        self.main_engine.get_account = self.get_account
        self.main_engine.get_all_accounts = self.get_all_accounts
        self.main_engine.get_all_contracts = self.get_all_contracts

    def register_event(self):
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)

    def process_contract_event(self, event):
        contract = event.data
        self.mutex.acquire()
        self.contracts[contract.vt_symbol] = copy(contract)
        self.mutex.release()

    def process_account_event(self, event):
        account = event.data
        self.mutex.acquire()
        self.accounts[account.vt_account_id] = account
        self.mutex.release()

    def get_contract(self, vt_symbol):
        self.mutex.acquire()
        data = self.contracts.get(vt_symbol, None)
        self.mutex.release()

        if not data:
            self.mutex.acquire()
            self.contracts[vt_symbol] = data
            self.mutex.release()
        return data

    def get_account(self, vt_account_id, api_key=""):
        """
        Get latest account data by vt_account_id.
        """
        self.mutex.acquire()
        data = self.accounts.get(vt_account_id, None)
        self.mutex.release()

        if data is None:
            self.mutex.acquire()
            self.accounts[vt_account_id] = data
            self.mutex.release()
        return data

    def get_all_accounts(self):
        """
        Get all account data.
        """
        self.mutex.acquire()
        data = list(self.accounts.values())
        self.mutex.release()
        return data

    def get_all_contracts(self):
        """
        Get all contract data.
        """
        self.mutex.acquire()
        data = list(self.contracts.values())
        self.mutex.release()
        return data


class LogEngine(BaseEngine):
    """
    Processes log event and output with logging module.
    """

    def __init__(self, main_engine, event_engine):
        super(LogEngine, self).__init__(main_engine, event_engine, "log")
        self.register_event()

    def register_event(self):
        self.event_engine.register(EVENT_LOG, self.process_log_event)

    def process_log_event(self, event):
        log = event.data
        log_service_manager.write_log('{}:{}'.format(log.gateway_name, log.msg), log.level)


class HotEngine(BaseEngine):
    """
    Provides order management system function for tumbler. (realtime data)
    """

    def __init__(self, main_engine, event_engine):

        super(HotEngine, self).__init__(main_engine, event_engine, "Hot")

        self.ticks = {}
        self.trades = {}
        self.positions = {}

        self.active_orders = {}

        self.add_function()
        self.register_event()

    def add_function(self):
        """Add query function to main engine."""
        self.main_engine.get_tick = self.get_tick
        self.main_engine.get_order = self.get_order
        self.main_engine.get_trade = self.get_trade
        self.main_engine.get_position = self.get_position
        self.main_engine.get_all_ticks = self.get_all_ticks
        self.main_engine.get_all_orders = self.get_all_orders
        self.main_engine.get_all_trades = self.get_all_trades
        self.main_engine.get_all_positions = self.get_all_positions
        self.main_engine.get_all_active_orders = self.get_all_active_orders

    def register_event(self):
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)

    def process_tick_event(self, event):
        tick = event.data
        self.ticks[tick.vt_symbol] = copy(tick)

    def process_order_event(self, event):
        order = event.data

        # If order is active, then update data in dict.
        if order.is_active():
            self.active_orders[order.vt_order_id] = order
        # Otherwise, pop inactive order from in dict
        elif order.vt_order_id in self.active_orders:
            self.active_orders.pop(order.vt_order_id)

    def process_trade_event(self, event):
        trade = event.data
        self.trades[trade.vt_trade_id] = trade

    def process_position_event(self, event):
        position = event.data
        self.positions[position.vt_position_id] = position

    def get_tick(self, vt_symbol):
        """
        Get latest market tick data by vt_symbol.
        """
        return self.ticks.get(vt_symbol, None)

    def get_order(self, vt_order_id):
        """
        Get latest order data by vt_order_id.
        """
        return self.active_orders.get(vt_order_id, None)

    def get_trade(self, vt_trade_id):
        """
        Get trade data by vt_trade_id.
        """
        return self.trades.get(vt_trade_id, None)

    def get_position(self, vt_position_id):
        """
        Get latest position data by vt_position_id.
        """
        return self.positions.get(vt_position_id, None)

    def get_all_ticks(self):
        """
        Get all tick data.
        """
        return list(self.ticks.values())

    def get_all_orders(self):
        """
        Get all order data.
        """
        return list(self.active_orders.values())

    def get_all_trades(self):
        """
        Get all trade data.
        """
        return list(self.trades.values())

    def get_all_positions(self):
        """
        Get all position data.
        """
        return list(self.positions.values())

    def get_all_active_orders(self, vt_symbol=""):
        """
        Get all active orders by vt_symbol.

        If vt_symbol is empty, return all active orders.
        """
        if not vt_symbol:
            return list(self.active_orders.values())
        else:
            active_orders = [
                order
                for order in self.active_orders.values()
                if order.vt_symbol == vt_symbol
            ]
            return active_orders
