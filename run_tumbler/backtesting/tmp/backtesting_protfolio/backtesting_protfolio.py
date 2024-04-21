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

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode, OptimizationSetting
from tumbler.apps.cta_strategy.strategies.haitun_strategy import HaitunStrategy 
from tumbler.apps.cta_strategy.strategies.ema_strategy import EmaStrategy 


def run_backtesting(strtegy_class, setting):
    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2014,11,1,8,10) ,rate = 0,
    slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2019,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="XBTUSD.BITMEX", interval=Interval.MINUTE.value, start=datetime(2014,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,1,1) ,rate = 0,
    #     slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,30), mode = BacktestingMode.BAR.value)

    engine.add_strategy(strtegy_class, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    # engine.calculate_statistics()

    # engine.show_chart()

    return df

def show_portafolio(df):
    engine = BacktestingEngine()
    engine.calculate_statistics(df)
    engine.show_chart(df)
    

def run():
    setting_ema1 = {
        "fast_window":5,
        "slow_window":20,
        "bar_window":4
    }

    setting_ema2 = {
        "fast_window":7,
        "slow_window":30,
        "bar_window":4
    }

    setting_ema3 = {
        "fast_window":7,
        "slow_window":30,
        "bar_window":6
    }

    setting_ema4 = {
        "fast_window":7,
        "slow_window":30,
        "bar_window":12
    }


    # setting_haitun = {
    #     "fast_window":12,
    #     "slow_window":26,
    #     "macd_window":9,
    #     "bar_window":4
    # }

    d1 = run_backtesting(EmaStrategy, setting_ema1)
    d2 = run_backtesting(EmaStrategy, setting_ema2)
    d3 = run_backtesting(EmaStrategy, setting_ema3)
    d4 = run_backtesting(EmaStrategy, setting_ema4)
    #d2 = run_backtesting(HaitunStrategy, setting_haitun)

    dfp = d1 + d2 + d3 + d4
    dfp.dropna()
    show_portafolio(dfp)
    
    input()


if __name__ == "__main__":
    run()