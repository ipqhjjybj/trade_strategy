# coding=utf-8

import time
from datetime import datetime

from tumbler.engine import MainEngine
from tumbler.aggregation import Aggregation

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
from tumbler.apps.cta_strategy.strategies.ema_dmi_v2_strategy import EmaDmiV2Strategy


def run():
    setting = {
        "ema_short": 5,
        "ema_long": 10,
        "adx_length": 18,
        "bar_window1": 4,
        "bar_window2": 24
    }

    engine = BacktestingEngine()

    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2018, 1, 1, 8, 10), rate=0,
                          slippage=10, size=1, price_tick=0.01, capital=0, end=datetime(2022, 1, 1, 9, 50),
                          mode=BacktestingMode.BAR.value)

    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv" \
        .format("btc_usdt")

    engine.add_strategy(EmaDmiV2Strategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
