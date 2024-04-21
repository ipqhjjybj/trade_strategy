# coding=utf-8
import pandas as pd
import matplotlib.pyplot as plt

import talib

from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.bar import BarGenerator
from tumbler.function.technique import PD_Technique, FundManagement, MultiIndexMethod
from tumbler.constant import Direction
from tumbler.constant import Direction, EvalType


def test_alpha_strategy(df, name=None):
    '''
    双EMA 策略
    这个很好, btc:18000, eth:1100, ltc:略有盈利
    '''
    fast_length = 5
    slow_length = 20
    name_short_ema = "ema_{}".format(fast_length)
    name_long_ema = "ema_{}".format(slow_length)
    PD_Technique.ema(df, fast_length, name=name_short_ema)
    PD_Technique.ema(df, slow_length, name=name_long_ema)

    df["alpha"] = (df[name_short_ema] - df[name_long_ema]) / df["close"]

    # print(df["alpha"])
    # df.dropna()
    df["alpha"].plot()
    plt.show()

    # df[name] = df["alpha"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))

    input()
    return df


def analyse(symbol="btc_usdt", sllippage=0, rate=0, size=1, period=5):
    filepath = "/Users/shenzhuoheng/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_1m_{}.csv" \
        .format(symbol, period)
    df = pd.read_csv(filepath)

    # df = PD_Technique.boll_strategy(df, 50, 1, name="pos")
    # df = PD_Technique.ema_strategy(df, 5, 20, name="pos")
    df = PD_Technique.four_week_strategy(df, 20, name="pos")
    # df = PD_Technique.cci_strategy(df, 20, 80, name="pos")
    # df = PD_Technique.kingkeltner_strategy(df, 40, name="pos")
    # df = PD_Technique.boll_reverse(df, 20, 1, name="pos")

    df["pos"] = df["pos"] * -1

    df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
    print(df)

    df.to_csv("./out.csv")

    df["income"].plot()

    plt.show()


def analyse_talib(filepath, sllippage=0, rate=0.0, size=1):
    df = pd.read_csv(filepath)

    # df["val"] = talib.RSI(df["close"], 5)
    # df["val"] = talib.RSI(df["close"], 5)
    # df["pos"] = df["val"].apply(lambda x: -1.0 if x < 20 else (1.0 if x > 80 else 0.0))
    # df["pos"] = df["pos"] * -1
    #
    # df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
    # print(df)

    # df = PD_Technique.osc_strategy(df, fast_length=5, slow_length=20, ma_osc=20, name="pos")
    # df = PD_Technique.trix_strategy(df, n=20, ma_length=3, name="pos")
    df = PD_Technique.one_line_strategy(df, n=30, name="pos")
    # df = PD_Technique.macd_strategy(df, fast_length=12, slow_length=26, macd_length=9, name="pos")
    # df = PD_Technique.boll_strategy(df, n=50, offset=1, name="pos")
    # df = PD_Technique.ema_strategy(df, fast_length=5, slow_length=60, name="pos")
    # df = PD_Technique.dmi_strategy(df, length=14, name="pos")
    # df = PD_Technique.ema_slope_trend_follower(df, ma_average_type="EMA", slopeflen=5, slopeslen=21,
    #                                            trendfilter=True, trendfilterperiod=200, trendfiltertype="EMA",
    #                                            volatilityfilter=False, volatilitystdevlength=20,
    #                                            volatilitystdevmalength=30,
    #                                            name="pos")
    # df = PD_Technique.ema_slope_trend_follower(df, ma_average_type="EMA", slopeflen=5, slopeslen=21,
    #                                            trendfilter=True, trendfilterperiod=200, trendfiltertype="EMA",
    #                                            volatilityfilter=False, volatilitystdevlength=20,
    #                                            volatilitystdevmalength=30,
    #                                            name="pos")
    # df = PD_Technique.three_line_strategy(df, fast_length=5, mid_length=10, long_length=20, name="pos")
    # df = PD_Technique.roc_strategy(df, n=65, name="pos")
    # df = PD_Technique.regression_strategy(df, n=20, name="pos")
    # df = PD_Technique.four_week_strategy(df, n=30, name="pos")
    # df = PD_Technique.kingkeltner_strategy(df, n=30, name="pos")
    # df = PD_Technique.ema_rsi_strategy(df, fast_length=5, slow_length=20, rsi_length=7, rsi_buy_value=70,
    #                                    rsi_sell_value=30, name="pos")
    # print(df)
    # df=eval("PD_Technique.kingkeltner_strategy(df, n=30, name=\"pos\")")
    func_arr = [

        # ("crossdown(open, ref(llv(close, 200), 1))", 0.05, 1)
    ]
    # df = PD_Technique.eval_many_s(df, func_arr, name="pos")
    # print(df)

    # fast_length = 5
    # slow_length = 60
    # b_rate = 0.01
    # func_arr = [
    #     [
    #         EvalType.MANY_CONDITIONS_FUNC.value,
    #         f"gt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 + {b_rate}))",
    #         f"reverse(gt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 + {b_rate})))",
    #         Direction.LONG.value,
    #         1
    #     ],
    #     [
    #         EvalType.MANY_CONDITIONS_FUNC.value,
    #         f"lt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 - {b_rate}))",
    #         f"reverse(lt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 - {b_rate})))",
    #         Direction.SHORT.value,
    #         1
    #     ],
    # ]
    # df = PD_Technique.eval(df, func_arr, name="pos")

    df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
    # df = PD_Technique.quick_compute_current_drawdown(df, name_cur_down="cur_down", name_max_drawdown="max_down")
    ans_dic = PD_Technique.assume_strategy(df)
    print("sharpe_val:{}, trade_times:{}, total_income:{}, rate:{}"
          .format(ans_dic["sharpe_ratio"], ans_dic["trade_times"], ans_dic["total_income"], ans_dic["rate"]))
    return df


def hour_analyse_talib(symbol="btc_usdt", sllippage=10, rate=0.0, size=1, period=4):
    filepath = "/Users/shenzhuoheng/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_1h_{}.csv" \
        .format(symbol, period)

    df = analyse_talib(filepath, sllippage=sllippage, rate=rate, size=size)

    # df = df.drop(labels=["symbol", "exchange", "datetime", "open", "high", "low", "close", "volume"], axis=1)
    df.to_csv("./out.csv")
    df["income"].plot()
    plt.show()


if __name__ == '__main__':
    # data_analyse()
    # minute_analyse_talib()

    # test()
    # hour_analyse_talib(symbol="eth_usdt", sllippage=0, rate=0.001, size=1, period=4)
    # hour_analyse_talib(symbol="btc_usdt", sllippage=0, rate=0.001, size=1, period=12)
    # hour_analyse_talib(symbol="sol_usdt", sllippage=0, rate=0.001, size=1, period=6)
    hour_analyse_talib(symbol="bnb_usdt", sllippage=0, rate=0.001, size=1, period=12)

