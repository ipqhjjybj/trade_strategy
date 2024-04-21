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
from tumbler.apps.cta_strategy.strategies.minute_boll_test_strategy import MinuteBollTestStrategy


def run():
    setting = {
        "backtesting": True,
        "symbol": "btc_usdt",
        "exchange": "OKEX",
        "bollinger_lengths": 100,
        "offset": 3,
        "exchange_info": {
            "price_tick": 0.1,
            "volume_tick": 1
        }
    }

    symbol = "btc_usdt"

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol="ada_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2017, 11, 1, 8, 10), rate=0,
                          slippage=0.00, size=1, price_tick=0.000001, capital=0, end=datetime(2018, 3, 29, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="eth_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2016,11,1,8,10) ,rate = 0,
    # slippage=10,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2019,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(
        symbol)
    engine.add_strategy(MinuteBollTestStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
