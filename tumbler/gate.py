# coding=utf-8

from copy import copy

import tumbler.config as config
from tumbler.object import LogData
from tumbler.service import MQSender
from tumbler.event import (
    EVENT_BBO_TICK,
    EVENT_TICK,
    EVENT_MARKET_TRADE,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG,
    EVENT_TICK_REST,
    EVENT_TICK_WS,
    EVENT_DICT_ACCOUNT,
    Event
)


class BaseGateway(object):
    # Fields required in setting dict for connect function.
    default_setting = {}

    # Exchanges supported in the gateway.
    exchanges = []

    def __init__(self, event_engine, gateway_name):
        self.event_engine = event_engine
        self.gateway_name = gateway_name

    def on_event(self, e_type, data=None):
        """
        General event push.
        """
        event = Event(e_type, copy(data))
        self.event_engine.put(event)

    def on_market_trade(self, trade_data):
        """
        :param account:
        :return:
        """
        self.on_event(EVENT_MARKET_TRADE, trade_data)

    def on_ws_tick(self, tick):
        """
        Tick event push.
        Tick event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_TICK, tick)
        self.on_event(EVENT_TICK + tick.vt_symbol, tick)

    def on_rest_tick(self, tick):
        """
        Tick event push.
        Tick event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_TICK, tick)
        self.on_event(EVENT_TICK + tick.vt_symbol, tick)

    def on_rest_bbo_tick(self, bbo_tick):
        """
        BBO Tick event push.
        Tick event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_BBO_TICK, bbo_tick)
        self.on_event(EVENT_BBO_TICK + bbo_tick.exchange, bbo_tick)

    def on_trade(self, trade):
        """
        Trade event push.
        Trade event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_TRADE, trade)
        self.on_event(EVENT_TRADE + trade.vt_symbol, trade)

    def on_order(self, order):
        """
        Order event push.
        Order event of a specific vt_order_id is also pushed.
        """
        self.on_event(EVENT_ORDER, order)
        self.on_event(EVENT_ORDER + order.vt_order_id, order)

    def on_position(self, position):
        """
        Position event push.
        Position event of a specific vt_symbol is also pushed.
        """
        self.on_event(EVENT_POSITION, position)
        self.on_event(EVENT_POSITION + position.vt_symbol, position)

    def on_account(self, account):
        """
        Account event push.
        Account event of a specific vt_account_id is also pushed.
        """
        self.on_event(EVENT_ACCOUNT, account)
        self.on_event(EVENT_ACCOUNT + account.vt_account_id, account)

    def on_dict_account(self, dict_account):
        """
        DictAccount event push
        """
        self.on_event(EVENT_DICT_ACCOUNT, dict_account)
        self.on_event(EVENT_DICT_ACCOUNT + dict_account.account_name, dict_account)

    def on_log(self, log):
        """
        Log event push.
        """
        self.on_event(EVENT_LOG, log)

    def on_contract(self, contract):
        """
        Contract event push.
        """
        self.on_event(EVENT_CONTRACT, contract)

    def write_log(self, msg):
        """
        Write a log event from gateway.
        """
        log = LogData()
        log.msg = msg
        log.gateway_name = self.gateway_name
        self.on_log(log)

    def connect(self, setting):
        """
        Start gateway connection.

        to implement this method, you must:
        * connect to server if necessary
        * log connected if all necessary connection is established
        * do the following query and response corresponding on_xxxx and write_log
            * contracts : on_contract
            * account asset : on_account
            * account holding: on_position
            * orders of account: on_order
            * trades of account: on_trade
        * if any of query above is failed,  write log.

        future plan:
        response callback/change status instead of write_log

        """
        pass

    def close(self):
        """
        Close gateway connection.
        """
        pass

    def subscribe(self, req):
        """
        Subscribe tick data update.
        """
        pass

    def transfer_amount(self, req):
        """
        send a transfer request
        :param req:
        :return:
        """
        pass

    def send_order(self, req):
        """
        Send a new order to server.

        implementation should finish the tasks blow:
        * create an OrderData from req using OrderRequest.create_order_data
        * assign a unique(gateway instance scope) id to OrderData.order_id
        * send request to server
            * if request is sent, OrderData.status should be set to Status.SUBMITTING
            * if request is failed to sent, OrderData.status should be set to Status.REJECTED
        * response on_order:
        * return OrderData.vt_order_id

        :return str vt_order_id for created OrderData
        """
        pass

    def cancel_order(self, req):
        """
        Cancel an existing order.
        implementation should finish the tasks blow:
        * send request to server
        """
        pass

    def send_orders(self, reqs):
        """
        Send a batch of orders to server.
        Use a for loop of send_order function by default.
        Reimplement this function if batch order supported on server.
        """
        vt_order_ids = []

        for req in reqs:
            vt_order_id = self.send_order(req)
            vt_order_ids.append(vt_order_id)

        return vt_order_ids

    def cancel_orders(self, reqs):
        """
        Cancel a batch of orders to server.
        Use a for loop of cancel_order function by default.
        Reimplement this function if batch cancel supported on server.
        """
        for req in reqs:
            self.cancel_order(req)

    def query_account(self):
        """
        Query account balance.
        """
        pass

    def query_position(self):
        """
        Query holding positions.
        """
        pass

    def query_history(self, req):
        """
        Query bar history data.
        """
        pass

    def get_default_setting(self):
        """
        Return default setting dict.
        """
        return self.default_setting


class SimpleOrderManager(object):
    """
    Simple order management tool to support order record.
    """

    def __init__(self, gateway):
        self.gateway = gateway
        self.orders = {}

    def has_order_id(self, order_id):
        return order_id in self.orders.keys()

    def add_order_id(self, order):
        self.orders[order.order_id] = copy(order)

    def remove_order_id(self, order):
        if order.order_id in self.orders.keys():
            self.orders.pop(order.order_id)

    def get_all_order_ids(self):
        return copy(list(self.orders.keys()))

    def get_order(self, order_id):
        return self.orders.get(order_id, None)

    def on_order(self, order):
        if not order.is_active():
            self.remove_order_id(order)
        else:
            self.add_order_id(order)
        self.gateway.on_order(order)


class LocalOrderManager:
    """
    Management tool to support use local order id for trading.
    """

    def __init__(self, gateway):
        self.gateway = gateway

        # For generating local order_id
        self.order_prefix = ""
        self.order_count = 0
        self.orders = {}  # local_order_id:order

        # Map between local and system order_id
        self.local_sys_order_id_map = {}
        self.sys_local_order_id_map = {}

        # Push order data buf
        self.push_data_buf = {}  # sys_order_id:data

        # Callback for processing push order data
        self.push_data_callback = None

        # Cancel request buf
        self.cancel_request_buf = {}  # local_order_id:req

    def get_all_alive_system_id(self):
        """
        Get all live system order_id.
        """
        return self.sys_local_order_id_map.keys()

    def has_system_order(self, sys_order_id):
        """
        Check if the sys_order_id is running machine send!
        """
        return str(sys_order_id) in self.sys_local_order_id_map.keys()

    def new_local_order_id(self):
        """
        Generate a new local order_id.
        """
        self.order_count += 1
        local_order_id = str(self.order_count).rjust(8, "0")
        return local_order_id

    def get_local_order_id(self, sys_order_id):
        """
        Get local order_id with sys order_id.
        """
        local_order_id = self.sys_local_order_id_map.get(sys_order_id, None)
        if not local_order_id:
            local_order_id = self.new_local_order_id()
            self.update_order_id_map(local_order_id, sys_order_id)

        return local_order_id

    def get_sys_order_id(self, local_order_id):
        """
        Get sys order_id with local order_id.
        """
        sys_order_id = self.local_sys_order_id_map.get(local_order_id, None)
        return sys_order_id

    def update_order_id_map(self, local_order_id, sys_order_id):
        """
        Update order_id map.
        """
        self.sys_local_order_id_map[sys_order_id] = local_order_id
        self.local_sys_order_id_map[local_order_id] = sys_order_id

        self.check_cancel_request(local_order_id)
        self.check_push_data(sys_order_id)

    def check_push_data(self, sys_order_id):
        """
        Check if any order push data waiting.
        """
        if sys_order_id not in self.push_data_buf:
            return

        data = self.push_data_buf.pop(sys_order_id)
        if self.push_data_callback:
            self.push_data_callback(data)

    def add_push_data(self, sys_order_id, data):
        """
        Add push data into buf.
        """
        self.push_data_buf[sys_order_id] = data

    def get_order_with_sys_order_id(self, sys_order_id):
        local_order_id = self.sys_local_order_id_map.get(sys_order_id, None)
        if not local_order_id:
            return None
        else:
            return self.get_order_with_local_order_id(local_order_id)

    def get_order_with_local_order_id(self, local_order_id):
        order = self.orders.get(local_order_id, None)
        return copy(order)

    def on_order(self, order):
        """
        Keep an order buf before pushing it to gateway.
        """
        self.orders[order.order_id] = copy(order)
        self.gateway.on_order(copy(order))

        if not order.is_active():
            local_order_id = order.order_id
            sys_order_id = self.local_sys_order_id_map.get(local_order_id, None)
            self.orders.pop(order.order_id)
            if sys_order_id:
                self.local_sys_order_id_map.pop(local_order_id)
                if sys_order_id in self.sys_local_order_id_map.keys():
                    self.sys_local_order_id_map.pop(sys_order_id)

    def cancel_order(self, req):
        sys_order_id = self.get_sys_order_id(req.order_id)
        if not sys_order_id:
            self.cancel_request_buf[req.order_id] = req
            return

        self.gateway.cancel_order(req)

    def check_cancel_request(self, local_order_id):
        if local_order_id not in self.cancel_request_buf:
            return

        req = self.cancel_request_buf.pop(local_order_id)
        self.gateway.cancel_order(req)


class RabbitmqGateway(BaseGateway):
    """
    Gateway that to send data to rabbitmq.
    """

    def __init__(self, event_engine, gateway_name, flag_sender=False):
        super(RabbitmqGateway, self).__init__(event_engine, gateway_name)

        self.flag_sender = flag_sender
        self.sender_tick_dict = {}

    def service_deal_tick(self, tick):
        sender = self.sender_tick_dict.get(tick.symbol, None)
        if sender is None:
            sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                              user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                              exchange=tick.vt_symbol)
            self.sender_tick_dict[tick.symbol] = sender

        sender.send(tick.vt_symbol, tick.get_json_msg())

    def on_ws_tick(self, tick):
        self.on_event(EVENT_TICK_WS, tick)
        if self.flag_sender:
            self.service_deal_tick(tick)

    def on_rest_tick(self, tick):
        self.on_event(EVENT_TICK_REST, tick)
        if self.flag_sender:
            self.service_deal_tick(tick)
