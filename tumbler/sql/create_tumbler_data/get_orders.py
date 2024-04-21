# coding=utf-8

from tumbler.service.mysql_service import MysqlService
from tumbler.record.client_quick_query import ClientPosPriceQuery

OPEN_ORDER_TABLE = "`tumbler`.`open_orders`"
TRADED_ORDER_TABLE = "`tumbler`.`traded_orders`"
ACCOUNT_TABLE = "`tumbler`.`realtime_fund`"
ORDER_FIELDS = "`symbol`, `side`, `order_id`, `open_price`, `deal_price`, `amount`, `filled_amount`, `status`, `strategy_type`, `client_id`, `order_time`, `update_time`"
ACCOUNT_FIELDS = "`date`,`accounts`,`account_type`"


class SaveDB(object):
    @staticmethod
    def conver_orders(orders_info, strategy_type):
        return [(order.symbol, order.direction, order.vt_order_id, '%.8lf' % order.price, '%.8lf' % order.deal_price,
                 '%.8lf' % order.volume,
                 '%.8lf' % order.traded, order.status, strategy_type, order.client_id, order.order_time,
                 order.cancel_time) for
                order in orders_info]

    @staticmethod
    def insert_open_orders(mysql_service_manager, symbol, strategy_type, orders_info):
        conn = mysql_service_manager.get_conn()
        cur = conn.cursor()
        sqll = "delete from {} where `symbol`='{}' and `strategy_type`='{}'".format(OPEN_ORDER_TABLE, symbol,
                                                                                    strategy_type)
        cur.execute(sqll)
        if len(orders_info) > 0:
            # sqll = "insert into {}({}) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')".format(
            #     OPEN_ORDER_TABLE, ORDER_FIELDS)
            sqll = "insert into {}({}) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(
                OPEN_ORDER_TABLE, ORDER_FIELDS)
            cur.executemany(sqll, SaveDB.conver_orders(orders_info, strategy_type))
        conn.commit()

    @staticmethod
    def insert_traded_orders(mysql_service_manager, symbol, strategy_type, orders_info):
        conn = mysql_service_manager.get_conn()
        cur = conn.cursor()
        order_ids = ["'" + order.vt_order_id + "'" for order in orders_info]
        if order_ids:
            sqll = "delete from {} where `symbol`='{}' and `order_id` in ({}) ".format(TRADED_ORDER_TABLE, symbol,
                                                                                       ','.join(order_ids))
            cur.execute(sqll)

        if len(orders_info) > 0:
            sqll = "insert into {}({}) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(TRADED_ORDER_TABLE,
                                                                                           ORDER_FIELDS)
            cur.executemany(sqll, SaveDB.conver_orders(orders_info, strategy_type))
        conn.commit()

    @staticmethod
    def insert_realtime_funds(mysql_service_manager, date, account_info, account_type):
        conn = mysql_service_manager.get_conn()
        cur = conn.cursor()
        sqll = "insert into {}({}) values('{}','{}','{}')".format(ACCOUNT_TABLE, ACCOUNT_FIELDS, date, account_info, account_type)
        cur.execute(sqll)
        conn.commit()


class MonitorOrders(object):
    def __init__(self, _setting):
        self.setting = _setting
        self.mysql_service_manager = MysqlService()

    def run(self):
        arr = self.setting.get("config", {})
        for dic in arr:
            strategy_type = dic.get("strategy_type", "")
            info = dic.get("info", [])
            for d in info:
                apikey = d.get('apikey', "")
                secret_key = d.get('secret_key', "")
                passphrase = d.get('passphrase', "")
                address = d.get('address', "")
                symbol_list = d.get("symbol_list", [])
                exchange = d.get("exchange", "")
                for symbol in symbol_list:
                    orders = ClientPosPriceQuery.query_open_orders(exchange, symbol, apikey, secret_key, passphrase,
                                                                   address, "")
                    print(orders)
                    if orders:
                        SaveDB.insert_open_orders(self.mysql_service_manager, symbol, strategy_type, orders)
                    orders = ClientPosPriceQuery.query_traded_orders(exchange, symbol, apikey, secret_key, passphrase,
                                                                     address, "")
                    if orders:
                        SaveDB.insert_traded_orders(self.mysql_service_manager, symbol, strategy_type, orders)
