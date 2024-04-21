# coding=utf-8

from datetime import datetime, timedelta

from tumbler.constant import Exchange, Interval, Status, Direction

import tumbler.data.local_data_produce as ldp

# ldp.go_to_fix_mongodb(symbol="btc_usdt", interval=Interval.MINUTE.value)

symbols = ["btc_usdt", "eth_usdt", "bnb_usdt", "bnb_usdt",
           "ltc_usdt", "bch_usdt", "dash_usdt", "xrp_usdt", "ada_usdt"]

for symbol in symbols:
    ldp.go_to_fix_mongodb(symbol=symbol, interval=Interval.HOUR.value)
