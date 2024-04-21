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
from tumbler.function import parse_get_data_third_part_setting, parse_get_monitor_setting, load_json
from tumbler.function import parse_get_exchange_symbol_setting

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.market_maker.strategies.grid_analyse import GridAnalyseStrategy 

def run():
    setting = {"put_nums":100, "profit_rate":6, "put_rate":10}
    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,1,1) ,rate = 0,
        slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = None, mode = BacktestingMode.BAR.value)

    engine.add_strategy(GridAnalyseStrategy, setting)

    engine.load_data()

    engine.run_backtesting()

    df = engine.calculate_result()

    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()