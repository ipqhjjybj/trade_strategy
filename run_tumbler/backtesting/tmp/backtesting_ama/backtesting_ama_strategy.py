# coding=utf-8

from datetime import datetime

from tumbler.constant import Interval
from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.market_maker.strategies.market_ama_v1_strategy import AMAMakerV1Strategy
from tumbler.function.technique import PD_Technique


def run_minute():
    symbol, sllippage, bar_window, rate = "btc_usdt", 2, 30, 0.001
    # symbol, sllippage, bar_window, rate = "btc_usdt", 0, 15, 0  # 赚钱的 ，如果slippage是0，加了手续费就亏
    vt_symbol = "{}.BINANCE".format(symbol)

    setting = {
        "backtesting": True,
        "symbol": "btc_usdt",
        "exchange": "OKEX",
        "pool_setting_list": [
            ("p1", 2, 30000),
            ("p2", 2, 30000),
            # ("p3", 1, 1000),
            # ("p4", 1, 1000)
        ],
        "profit_rate": 0.3,
        "fee_rate": 0.1,
        "inc_spread": 0.1
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol=vt_symbol, interval=Interval.MINUTE.value,
                          start=datetime(2016, 3, 1, 8, 10), rate=rate,
                          slippage=sllippage, size=1, price_tick=0.000001, capital=0, end=datetime(2022, 3, 20, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2016, 11, 1, 8, 10), rate=0,
    #                       slippage=10, size=1, price_tick=0.01, capital=0, end=datetime(2020, 1, 29, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="eth_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2016,11,1,8,10) ,rate = 0,
    # slippage=10,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value, start=datetime(2017,11,1,8,10) ,rate = 0,
    # slippage=0.01,  size=1, price_tick=0.01, capital = 0, end = datetime(2020,1,29,9,50), mode = BacktestingMode.BAR.value)

    #filename = "/Users/szh/git/tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(symbol)
    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(symbol)
    engine.add_strategy(AMAMakerV1Strategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run_minute()
