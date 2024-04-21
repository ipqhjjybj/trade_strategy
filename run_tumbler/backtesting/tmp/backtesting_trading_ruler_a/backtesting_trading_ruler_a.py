# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

import talib

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.trading_ruler_a_strategy import TradingRulerAStrategy


def run():
    symbol = "btc_usdt"
    setting = {
        "symbol_pair": "btc_usdt",
        "vt_symbols_subscribe": [
            f"{symbol}.OKEXS",
        ],
        "class_name": "TradingRulerAStrategy",
        "bar_window": 4,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01,
            "M": 30,
            "N": 60,
            "target_symbol_min_need": 0,
            "target_symbol_percent_use": 80,
            "base_symbol_min_need": 0,
            "base_symbol_percent_use": 15
        }
    }
    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_hour1.csv" \
        .format(symbol)

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.HOUR.value,
                          start=datetime(2021, 1, 1, 8, 10), rate=0,
                          slippage=0.01, size=1, price_tick=0.01, capital=0, end=datetime(2022, 12, 1, 9, 50),
                          mode=BacktestingMode.BAR.value)

    engine.add_strategy(TradingRulerAStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
