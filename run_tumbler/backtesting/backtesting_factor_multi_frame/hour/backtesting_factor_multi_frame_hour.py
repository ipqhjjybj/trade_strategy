# coding=utf-8

from datetime import datetime
from tumbler.constant import Interval

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.factor_multi_period_strategy import FactorMultiPeriodStrategy
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
    # 3
    df = PD_Technique.three_line_strategy(df, 5, 10, 20, name="pos_1")
    df = PD_Technique.roc_strategy(df, n=30, name="pos_2")
    df = PD_Technique.kingkeltner_strategy(df, n=30, name="pos_3")
    df["pos"] = df["pos_1"] + df["pos_2"] + df["pos_3"]
    df = df.drop(["pos_1", "pos_2", "pos_3"], axis=1)
    return df


def func_hour4(df):
    # 9
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos_1")
    df = PD_Technique.ema_strategy(df, 5, 20, name="pos_2")
    df = PD_Technique.three_line_strategy(df, 5, 10, 20, name="pos_3")
    df = PD_Technique.one_line_strategy(df, n=30, name="pos_4")
    df = PD_Technique.roc_strategy(df, n=30, name="pos_5")
    df = PD_Technique.four_week_strategy(df, n=30, name="pos_6")
    df = PD_Technique.kingkeltner_strategy(df, n=30, name="pos_7")
    df["pos"] = df["pos_1"] * 2 + df["pos_2"] + df["pos_3"] * 2 + df["pos_4"] + df["pos_5"] + \
                df["pos_6"] + df["pos_7"]
    df = df.drop(["pos_1", "pos_2", "pos_3", "pos_4", "pos_5", "pos_6", "pos_7"], axis=1)
    return df


def func_hour2(df):
    # 4
    df = PD_Technique.ema_strategy(df, 5, 20, name="pos_2")
    df = PD_Technique.three_line_strategy(df, 5, 10, 20, name="pos_3")
    df["pos"] = df["pos_2"] + df["pos_3"] * 3
    df = df.drop(["pos_2", "pos_3"], axis=1)
    return df


def func_hour1(df):
    # 7
    #df = PD_Technique.ema_strategy(df, 5, 15, name="pos")
    #df = PD_Technique.dmi_strategy(df, 14, name="pos")
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos")
    # df = PD_Technique.ema_strategy(df, 5, 20, name="pos_2")
    # df = PD_Technique.three_line_strategy(df, 5, 10, 20, name="pos_3")
    # df = PD_Technique.four_week_strategy(df, n=40, name="pos_4")
    # df["pos"] = df["pos_1"] * 4 + df["pos_2"] + df["pos_3"] + df["pos_4"]
    # df = df.drop(["pos_1", "pos_2", "pos_3", "pos_4"], axis=1)
    return df


def run():
    bar_period_factor = [
        (func_hour1, 1, Interval.HOUR.value),
        # (func_hour2, 2, Interval.HOUR.value),
        # (func_hour4, 4, Interval.HOUR.value),
        # (func_hour6, 6, Interval.HOUR.value),
        # (func_hour12, 12, Interval.HOUR.value),
    ]

    # symbol, sllippage, rate = "eth_usdt", 0.1, 0.001
    # symbol, sllippage, rate = "btc_usdt", 2, 0.001
    symbol, sllippage, rate = "eth_btc", 0, 0.001
    vt_symbol = "{}.BINANCE".format(symbol)

    setting = {
        "is_backtesting": True,
        "bar_period_factor": bar_period_factor,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.00000001
        }
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol=vt_symbol, interval=Interval.MINUTE.value,
                          start=datetime(2017, 7, 1, 8, 10), rate=rate,
                          slippage=sllippage, size=1, price_tick=0.000001,
                          capital=0, end=datetime(2022, 6, 29, 9, 50),
                          mode=BacktestingMode.BAR.value)

    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_hour1.csv".format(
        symbol)
    # filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(
    #     symbol)

    engine.add_strategy(FactorMultiPeriodStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
