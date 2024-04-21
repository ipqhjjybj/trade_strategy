# coding=utf-8

from datetime import datetime

import talib as ta
import numpy as np

from tumbler.constant import Interval
from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.factor_strategy import FactorStrategy
from tumbler.function.technique import PD_Technique, FundManagement


def func(df):
    # 这个很好,目前是最好的 ，收益 22800
    # df = PD_Technique.three_line_strategy(df, 5, 10, 20, name="pos")
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos")

    # 收益 15000
    # df = PD_Technique.four_week_strategy(df, 30, name="pos")

    # 还可以  收益大概18000
    # df = PD_Technique.ema_strategy(df, 5, 20, name="pos")

    # 这个也可以吧，收益大概15000
    # df = PD_Technique.kingkeltner(df, n=40, name="pos")

    # 混合策略测试
    # df = PD_Technique.boll_strategy(df, 50, 1, name="pos_1")
    # df = PD_Technique.ema_strategy(df, 5, 20, name="pos_2")
    # df["pos"] = df["pos_1"] + df["pos_2"]
    # df = df.drop(["pos_1", "pos_2"], axis=1)

    # 反趋势策略测试
    # 测试效果很差
    # df = PD_Technique.boll_reverse(df, 20, 4, name="pos")
    # df = PD_Technique.boll_reverse_mid(df, 20, 0.1, name="pos")

    # sllippage = 0
    # rate = 0.1
    # size = 1
    # df = FundManagement.adjust_positions(df, entry_max_nums=5,
    #                                      name_tot_pos="pos", min_level_percent=0.1,
    #                                      sllippage=sllippage, rate=rate, size=size)

    # sharpe = PD_Technique.assume_strategy(df)
    # print("sharpe:{}".fo)

    # df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
    #
    #print(df)
    # df = PD_Technique.quick_compute_current_drawdown(df, name_cur_down="cur_down", name_max_drawdown="max_down")
    # print(df)

    return df


# 正收益，还可以吧
# cci 9000
# def func(df):
#     df = PD_Technique.cci(df, n=20, constant=0.015, name="cci")
#     df["pos"] = df["cci"].apply(lambda x: -1.0 if x < -80 else (1.0 if x > 80 else 0.0))
#     df = df.drop(labels=["cci"], axis=1)
#     return df

# cci 这两个差不多，收益10000 左右
# cci 10000
# def func(df):
#     df = PD_Technique.cci(df, n=20, constant=0.015, name="cci")
#
#     df["pos"] = df["cci"].apply(lambda x: 1 if x > 80 else 0) + df["cci"].apply(lambda x: -1 if x < -80 else 0)
#     df["pos"][df["pos"] == 0] = np.NAN
#     df["pos"] = df["pos"].fillna(method='ffill')
#     df = df.drop(labels=["cci"], axis=1)
#     return df

# 看着很一般吧
# trix
# def func(df):
#     df["trix_5"] = ta.TRIX(df.close, 5)
#     df["trix_30"] = ta.TRIX(df.close, 30)
#     df['tmp_trix'] = df["trix_5"] / df["trix_30"] - 1
#     df["pos"] = df["tmp_trix"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
#     df = df.drop(labels=["trix_5", "trix_30", "tmp_trix"], axis=1)
#     return df


def run_minute():
    symbol, sllippage, bar_window, rate = "btc_usdt", 2, 30, 0.001
    #symbol, sllippage, bar_window, rate = "btc_usdt", 0, 15, 0  # 赚钱的 ，如果slippage是0，加了手续费就亏
    vt_symbol = "{}.BINANCE".format(symbol)

    setting = {
        "bar_window": bar_window,
        "interval": Interval.MINUTE.value,
        "is_backtesting": True,
        "compute_factor": func,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01
        }
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol=vt_symbol, interval=Interval.MINUTE.value,
                          start=datetime(2017, 10, 1, 8, 10), rate=rate,
                          slippage=sllippage, size=1, price_tick=0.000001, capital=0, end=datetime(2020, 6, 29, 9, 50),
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
    engine.add_strategy(FactorStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


def run_hour():
    #symbol, sllippage = "eth_usdt", 0.1
    #symbol, sllippage = "btc_usdt", 3
    symbol, sllippage = "btc_usdt", 10
    vt_symbol = "{}.BINANCE".format(symbol)

    setting = {
        "bar_window": 1,
        "interval": Interval.HOUR.value,
        "is_backtesting": True,
        "compute_factor": func,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.01
        }
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol=vt_symbol, interval=Interval.MINUTE.value,
                          start=datetime(2017, 10, 1, 8, 10), rate=0,
                          slippage=sllippage, size=1, price_tick=0.000001, capital=0, end=datetime(2020, 6, 29, 9, 50),
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
    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_hour1.csv".format(symbol)
    engine.add_strategy(FactorStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run_hour()
    #run_minute()
