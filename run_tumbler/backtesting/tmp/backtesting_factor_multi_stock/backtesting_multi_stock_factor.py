# coding=utf-8

from datetime import datetime
from tumbler.constant import Interval

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.factor_multi_strategy import FactorMultiStrategy


def compute_factor(df):
    pass


def run():
    setting = {
        "bar_window": 4,
        "is_backtesting": True,
        "train_datetime": datetime(2019, 3, 1, 1, 1),
        "compute_factor": compute_factor,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01
        }
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2017, 7, 1, 8, 10), rate=0,
                          slippage=0.00, size=1, price_tick=0.000001,
                          capital=0, end=datetime(2020, 6, 29, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2016, 11, 1, 8, 10), rate=0,
    #                       slippage=10, size=1, price_tick=0.01, capital=0, end=datetime(2020, 1, 29, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="eth_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2016,11,1,8,10) ,rate = 0,
    # slippage=10,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    engine.add_strategy(FactorMultiStrategy, setting)
    engine.load_data()
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
