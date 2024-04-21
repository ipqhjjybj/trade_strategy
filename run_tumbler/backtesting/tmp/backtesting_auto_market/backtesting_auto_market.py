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
from tumbler.event import EventEngine
from tumbler.engine import MainEngine
from tumbler.gateway.huobi import HuobiGateway
from tumbler.gateway.gateio import GateioGateway
from tumbler.gateway.okex import OkexGateway
from tumbler.apps.market_maker import MarketMakerApp
from tumbler.apps.monitor import MonitorApp
from tumbler.event import EVENT_LOG, EVENT_TRADE, Event
from tumbler.object import SubscribeRequest, TradeData

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.auto_market_strategy import AutoMarketStrategy


def run():
    '''
    基准 1000000
    2020-01-01 -- 2020-02-01   赚钱
    下面全是亏钱的
    2020-02-01 -- 2020-03-01   600000
    2020-03-01 -- 2020-04-01   -5000000
    2020-04-01 -- 2020-05-01   -1000000
    2020-05-01 -- 2020-06-01   400000
    2020-06-01 -- 2020-07-01   -500000
    2020-07-01 -- 2020-08-01   0
    2020-08-01 -- 2020-09-01   400000

    '''
    setting = {
        'pos': 0,
        'fixed_spread': 0.1,
        'fixed_volume': 1,
        'price_tick': 0.01
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2020, 8, 1, 8, 10), rate=0,
                          slippage=0.00, size=1, price_tick=0.000001, capital=0, end=datetime(2020, 9, 1, 9, 50),
                          mode=BacktestingMode.BAR.value)

    engine.add_strategy(AutoMarketStrategy, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
