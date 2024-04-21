# coding=utf-8


import tumbler.data.local_data_produce as ldp
from tumbler.service.mysql_service import MysqlService


def loop1():
    mysql_service_manager = MysqlService()

    day_symbols = mysql_service_manager.get_mysql_distinct_symbol(table='kline_1day')
    for symbol in day_symbols:
        ldp.get_hour_bar_from_mysql(symbol=symbol)


# loop1()


# ldp.get_minute_bar_from_mongo(symbol="link_usdt")
# ldp.get_minute_bar_from_mysql(symbol="etc_usdt")
# ldp.get_minute_bar_from_mysql(symbol="link_usdt")

# ldp.get_hour_bar_from_mysql(symbol="aave_btc")
# ldp.get_hour_bar_from_mysql(symbol="bnb_btc")
# ldp.get_hour_bar_from_mysql(symbol="btc_usdt")
# ldp.get_day_bar()
print(ldp.get_all_day_bar())
