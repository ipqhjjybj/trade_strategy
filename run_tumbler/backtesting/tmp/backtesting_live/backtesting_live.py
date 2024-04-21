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
from tumbler.function import load_json

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.market_maker.strategies.grid_maker_v1_strategy import GridMakerV1Strategy
from tumbler.apps.cta_strategy.strategies.live_strategy import LiveStrategy


def run():
    setting = {
        "symbol_pair": "btc_usdt",
        "vt_symbols_subscribe": [
            "btc_usdt.OKEXS",
        ],
        "class_name": "LiveStrategy",
        "signals": {
            "ema": {
                "fast_window": 5,
                "slow_window": 15
            },
            # "dmi": {
            #     "length": 14
            # }
        },
        "bar_window": 1,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01,
            "target_symbol_min_need": 0,
            "target_symbol_percent_use": 80,
            "base_symbol_min_need": 0,
            "base_symbol_percent_use": 15
        }
    }
    symbol = "btc_usdt"
    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_hour1.csv".format(
        symbol)
    # filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(
    #     symbol)

    engine = BacktestingEngine()

    engine.set_parameters(vt_symbol=f"{symbol}.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2017, 11, 1, 8, 10), rate=0.001,
                          slippage=0.00, size=1, price_tick=0.00000001, capital=0, end=datetime(2022, 1, 29, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,10,1,8,10) ,rate = 0,
    # slippage=10,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,10,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2019,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="XBTUSD.BITMEX", interval=Interval.MINUTE.value, start=datetime(2014,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,1,1) ,rate = 0,
    #     slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,30), mode = BacktestingMode.BAR.value)

    engine.add_strategy(LiveStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
