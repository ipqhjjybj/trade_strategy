# coding=utf-8

from datetime import datetime
from tumbler.constant import Interval

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.factor_multi_period_strategy import FactorMultiPeriodStrategy
from tumbler.apps.cta_strategy.strategies.factor_multi_period_v3_strategy import FactorMultiPeriodV3Strategy
from tumbler.function.technique import PD_Technique
from tumbler.constant import EvalType


def func_hour12(df):
    func_arr = [
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 10, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 15, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 45, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 60, name="pos")[name]',
            1
        ]
    ]
    df = PD_Technique.eval(df, func_arr, name="pos")
    return df


def func_hour6(df):
    func_arr = [
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 10, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 15, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 45, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 60, name="pos")[name]',
            1
        ]
    ]
    df = PD_Technique.eval(df, func_arr, name="pos")
    return df


def func_hour4(df):
    func_arr = [
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 10, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 15, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 60, name="pos")[name]',
            1
        ]
    ]
    df = PD_Technique.eval(df, func_arr, name="pos")
    return df


def func_hour2(df):
    func_arr = [
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 10, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 15, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 45, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 60, name="pos")[name]',
            1
        ]
    ]
    df = PD_Technique.eval(df, func_arr, name="pos")
    return df


def func_hour1(df):
    func_arr = [
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 15, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 20, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 10, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 30, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 45, name="pos")[name]',
            1
        ],
        [
            EvalType.SIMPLE_FUNC.value,
            'PD_Technique.ema_strategy(df, 5, 60, name="pos")[name]',
            1
        ]
    ]
    df = PD_Technique.eval(df, func_arr, name="pos")
    return df


def run():
    bar_period_factor = [
        (func_hour1, 1, Interval.HOUR.value),
        (func_hour2, 2, Interval.HOUR.value),
        (func_hour4, 4, Interval.HOUR.value),
        (func_hour6, 6, Interval.HOUR.value),
        (func_hour12, 12, Interval.HOUR.value),
    ]

    # symbol, sllippage, rate = "eth_usdt", 0.1, 0.001
    symbol, sllippage, rate = "btc_usdt", 2, 0.001
    vt_symbol = "{}.BINANCE".format(symbol)

    setting = {
        "is_backtesting": True,
        "bar_period_factor": bar_period_factor,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01,
            "volume_tick": 0.00000001,
        }
    }

    engine = BacktestingEngine()
    # engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
    #                       start=datetime(2020, 1, 1, 8, 10), rate=rate,
    #                       slippage=sllippage, size=1, price_tick=0.000001,
    #                       capital=0, end=datetime(2020, 6, 29, 9, 50),
    #                       mode=BacktestingMode.BAR.value)

    engine.set_parameters(vt_symbol="btc_usdt.BINANCE", interval=Interval.MINUTE.value,
                          start=datetime(2017, 1, 1, 8, 10), rate=rate,
                          slippage=sllippage, size=1, price_tick=0.000001,
                          capital=0, end=datetime(2022, 6, 29, 9, 50),
                          mode=BacktestingMode.BAR.value)

    # filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(
    #     symbol)

    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_hour1.csv".format(
        symbol)

    engine.add_strategy(FactorMultiPeriodV3Strategy, setting)
    # engine.load_data()
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
