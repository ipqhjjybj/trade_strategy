# coding=utf-8

from tumbler.constant import Exchange
from tumbler.gate import LocalOrderManager


class TestStrategy(object):

    def write_log(self, msg):
        print(msg)

    def write_important_log(self, msg):
        print(msg)


class TestGateway(object):
    # Fields required in setting dict for connect function.
    default_setting = {}

    # Exchanges supported in the gateway.
    exchanges = []

    trade_ws_api = None

    def __init__(self):
        """"""
        self.gateway_name = Exchange.OKEX5.value
        self.order_manager = LocalOrderManager(self)
        self.orders = {}

        self.ws_trade_api = None
        # self.order_manager = SimpleOrderManager()

    def on_rest_tick(self, tick):
        """
        Tick event push.
        Tick event of a specific vt_symbol is also pushed.
        """
        print(tick.__dict__)

    def on_ws_tick(self, tick):
        """
        Tick event push.
        Tick event of a specific vt_symbol is also pushed.
        """
        # print(tick.__dict__)
        print(str(tick.datetime), tick.bid_prices[0], tick.bid_volumes[0], tick.ask_prices[0], tick.ask_volumes[0])

    def on_trade(self, trade):
        """
        Trade event push.
        Trade event of a specific vt_symbol is also pushed.
        """
        print(trade.__dict__)

    def on_order(self, order):
        """
        Order event push.
        Order event of a specific vt_orderid is also pushed.
        """
        # pass
        print(order.__dict__)

    def on_position(self, position):
        """
        Position event push.
        Position event of a specific vt_symbol is also pushed.
        """

        print(position.__dict__)

    def on_account(self, account):
        """
        Account event push.
        Account event of a specific vt_accountid is also pushed.
        """
        # pass
        print(account.__dict__)

    def on_log(self, log):
        """
        Log event push.
        """
        print(log.__dict__)

    def on_contract(self, contract):
        """
        Contract event push.
        """
        pass
        # print(contract.__dict__)

    def write_log(self, msg):
        """
        Write a log event from gateway.
        """
        print(msg)

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

    def send_order(self, req):
        """
        Send a new order to server.

        implementation should finish the tasks blow:
        * create an OrderData from req using OrderRequest.create_order_data
        * assign a unique(gateway instance scope) id to OrderData.orderid
        * send request to server
            * if request is sent, OrderData.status should be set to Status.SUBMITTING
            * if request is failed to sent, OrderData.status should be set to Status.REJECTED
        * response on_order:
        * return OrderData.vt_orderid

        :return str vt_orderid for created OrderData
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
        vt_orderids = []

        for req in reqs:
            vt_orderid = self.send_order(req)
            vt_orderids.append(vt_orderid)

        return vt_orderids

    def cancel_orders(self, reqs):
        """
        Cancel a batch of orders to server.
        Use a for loop of cancel_order function by default.
        Reimplement this function if batch cancel supported on server.
        """
        for req in reqs:
            self.cancel_order(req)

    def get_order(self, order_id):
        return None

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

    def get_all_symbols(self, exchange_name):
        """
        """
        return set(["eos_usdt", "bch_usdt"])
