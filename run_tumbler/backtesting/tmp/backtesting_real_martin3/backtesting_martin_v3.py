# coding=utf-8

import time
from datetime import datetime

from tumbler.engine import MainEngine
from tumbler.event import EventEngine
from tumbler.event import (
    EVENT_MERGE_TICK,
    EVENT_LOG
)

from tumbler.constant import Exchange, Interval

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.martin_v3_strategy import MartinV3Strategy


def run():
    '''
       基准 1000000
    '''
    setting = {
        "spread": 0.25,
        "put_order_num": 4,
        "max_loss_buy_num": 28,
        "max_loss_sell_num": 28,
        "support_long": True,
        "support_short": True
    }
    '''
           基准 0
           2019, 3, 1 ----- 2020, 7, 1  -25166.859999999902
           2017, 8, 1 ----- 2020, 7, 1  489569.2999999989
    '''
    setting = {
        "spread": 0.2,
        "put_order_num": 4,
        "max_loss_buy_num": 28,
        "max_loss_sell_num": 28,
        "support_long": True,
        "support_short": True
    }

    engine = BacktestingEngine()

    # engine.set_parameters(vt_symbol="eth_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2017, 8, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 7, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2017, 8, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 5, 29, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2019, 3, 1, 8, 10), rate=0,
                          slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 7, 1, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2020, 1, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 3, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2020, 3, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 5, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2020, 5, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 7, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2019, 1, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2019, 4, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2019, 4, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2019, 7, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2019, 7, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2019, 12, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    engine.add_strategy(MartinV3Strategy, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    print(setting)
    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
