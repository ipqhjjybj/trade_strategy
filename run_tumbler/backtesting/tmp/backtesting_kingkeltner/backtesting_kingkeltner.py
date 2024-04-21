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
from tumbler.function import parse_get_data_third_part_setting, parse_get_monitor_setting, load_json
from tumbler.function import parse_get_exchange_symbol_setting

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.kingkeltner_strategy import KingKeltnerStrategy 

def run():
    setting = {
        "avg_length":40,
        "atr_length":40,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01
        }
    }
    
    engine = BacktestingEngine()

    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,1,1,8,10) ,rate = 0,
    slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,2,28,9,50), mode = BacktestingMode.BAR.value)
    
    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2019,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2019,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="XBTUSD.BITMEX", interval=Interval.MINUTE.value, start=datetime(2014,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,1,1) ,rate = 0,
    #     slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2019,12,30), mode = BacktestingMode.BAR.value)

    engine.add_strategy(KingKeltnerStrategy, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()