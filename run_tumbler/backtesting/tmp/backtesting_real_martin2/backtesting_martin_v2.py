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
from tumbler.apps.cta_strategy.strategies.martin_v2_strategy import MartinV2Strategy


def run():
    '''
    0.2 目前效果最好
    '''
    '''
        基准 1000000
        2017-08-17 -- 2020-05-29        1772816.889, 赚钱
        2019-01-01 -- 2019-04-01        1007000, 赚钱
        2019-04-01 -- 2019-07-01        970000, 亏钱
        2019-07-01 -- 2019-12-01        999000, 亏钱
        2020-01-01 -- 2020-03-01        1015000, 赚钱
        2020-03-01 -- 2020-05-01        950000, 亏钱
        2020-05-01 -- 2020-07-01        1020000, 赚钱
    '''
    # setting = {
    #     "put_order_num": 4,
    #     "max_loss_buy_num": 28,
    #     "max_loss_sell_num": 28,
    #     "support_long": True,
    #     "support_short": True
    # }

    '''
        基准 1000000  btc_usdt
        2017-08-17 -- 2020-05-29        1751294.7300000002 赚钱  7倍
        2020-03-01 -- 2020-07-01        1050000, 赚钱
        
        基准 1000000  btc_usdt
        2017-08-17 -- 2020-05-29        1050000 赚钱 5倍
        
    '''
    setting = {
        "spread": 0.2,
        "put_order_num": 4,
        "max_loss_buy_num": 28,
        "max_loss_sell_num": 28,
        "support_long": True,
        "support_short": True
    }

    '''
        基准 1000000
        2017-08-17 -- 2020-05-29        1632679.749999999 赚钱
        2019-03-17 -- 2020-07-01        1150000, 赚钱
    '''
    # setting = {
    #     "spread": 0.3,
    #     "put_order_num": 4,
    #     "max_loss_buy_num": 28,
    #     "max_loss_sell_num": 28,
    #     "support_long": True,
    #     "support_short": True
    # }

    '''
       基准 1000000
       2017-08-17 -- 2020-05-29 1321347.4199999995
    '''
    # setting = {
    #     "spread": 0.4,
    #     "put_order_num": 4,
    #     "max_loss_buy_num": 28,
    #     "max_loss_sell_num": 28,
    #     "support_long": True,
    #     "support_short": True
    # }

    '''
       基准 1000000
       2019-03-17 -- 2020-07-01 1115000
    '''
    # setting = {
    #     "spread": 0.25,
    #     "put_order_num": 4,
    #     "max_loss_buy_num": 28,
    #     "max_loss_sell_num": 28,
    #     "support_long": True,
    #     "support_short": True
    # }

    engine = BacktestingEngine()

    engine.set_parameters(vt_symbol="eth_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2017, 8, 1, 8, 10), rate=0,
                          slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 7, 1, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2017, 8, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 5, 29, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2019, 3, 1, 8, 10), rate=0,
    #                       slippage=0.00, size=1, price_tick=0.01, capital=0, end=datetime(2020, 7, 1, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

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

    engine.add_strategy(MartinV2Strategy, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    print(setting)
    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
