# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.function.technique import Technique, PD_Technique


def new_upload(df):
    mysql_service = MysqlService()
    conn = mysql_service.get_conn()
    sqll = "select distinct factor_code from factor_1day"
    cur = conn.cursor()
    cur.execute(sqll)

    my_result = cur.fetchall()
    factor_codes = [x[0] for x in list(my_result)]
    print("factor_codes:", factor_codes)

    use_factor_columns = df.columns
    print("use_factor_columns:", use_factor_columns)

    drop_columns = [x for x in factor_codes if x in use_factor_columns]
    print("drop_columns:", drop_columns)

    df = df.drop(columns=drop_columns)
    print(df)

    mysql_service_manager = MysqlService.get_mysql_service()
    mysql_service_manager.replace_factor_from_pd(df, Interval.DAY.value)


def run():
    # ma可以研究下，做均值反转策略

    '''
    def f(x):
        upper, middle, lower = talib.BBANDS(x, ...) #enter the parameter you need
            return pd.DataFrame({'upper':upper, 'middle':middle, 'lower':lower },
                                index=x.index)
    df[['upper','middle','lower']] = df.groupby('type').apply(lambda x : f(x.x))

    or:

    df["roc30"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 30), index=x.index)
    )

    '''
    mysql_service_manager = MysqlService.get_mysql_service()
    symbols = mysql_service_manager.get_mysql_distinct_symbol(table=MysqlService.get_kline_table(Interval.DAY.value))

    bars = mysql_service_manager.get_bars(symbols=[], period=Interval.DAY.value,
                                          start_datetime=datetime(2017, 1, 1),
                                          end_datetime=datetime.now() + timedelta(hours=10),
                                          sort_way="symbol")

    bars = BarData.suffix_filter(bars, suffix="_usdt")

    # bars = mysql_service_manager.get_bars(symbols=["bnb_usdt"], period=Interval.DAY.value,
    #                                       start_datetime=datetime(2017, 1, 1),
    #                                       end_datetime=datetime.now() + timedelta(hours=10),
    #                                       sort_way="symbol")
    bars.sort()

    df = BarData.get_pandas_from_bars(bars)
    df = df.set_index(["symbol", "datetime"]).sort_index().reset_index()

    # boll
    def f_boll(x):
        upper, middle, lower = talib.BBANDS(x)  # enter the parameter you need
        return pd.DataFrame({'upper': (upper - x) / x, 'middle': (middle - x) / x, 'lower': (lower - x) / x}
                            , index=x.index)

    df[['BOLL_UP', 'BOLL_MIDDLE', 'BOLL_LOWER']] = df.groupby(by=['symbol']).apply(lambda x: f_boll(x.close))

    # # macd
    # def f_macd(x):
    #     macd, macdsignal, macdhist = talib.MACD(x, fastperiod=12, slowperiod=26, signalperiod=9)
    #     return pd.DataFrame({'macd': macd, "macdsignal": macdsignal, "macdhist": macdhist})
    #
    # df[['MACD', 'MACDSIGNAL', 'MACDHIST']] = df.groupby(by=['symbol']).apply(lambda x: f_macd(x.close))

    # MA/CLOSE
    # for period in [5, 10, 20, 30, 60, 120]:
    #     df[f"MA{period}"] = df.groupby(by=['symbol']).apply(
    #         lambda x: pd.DataFrame((talib.MA(x.close, period) - x.close) / x.close, index=x.index)
    #     )

    # for period in [5, 10, 20, 30, 60, 120]:
    #     df[f"NEGMA{period}"] = df.groupby(by=['symbol']).apply(
    #         lambda x: pd.DataFrame((talib.MA(x.close, period) - x.close) / x.close * -1, index=x.index)
    #     )

    # 这个指标还行
    # df["NATR"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.NATR(x.high, x.low, x.close), index=x.index)
    # )

    # df["NEGNATR"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.NATR(x.high, x.low, x.close) * -1, index=x.index)
    # )

    t = 10  # list(range(6, 21))
    #
    # df["PPO"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.PPO(x.close, fastperiod=12, slowperiod=26, matype=0), index=x.index)
    # )
    #
    # df["MOM"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.MOM(x.close, timeperiod=10), index=x.index)
    # )
    #
    # df["WMA"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.WMA(x.close, timeperiod=30) / x.close - 1, index=x.index)
    # )
    #
    # df["HT_TRENDLINE"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.HT_TRENDLINE(x.close) / x.close - 1, index=x.index)
    # )
    #
    # df["EMA"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.EMA(x.close) / x.close - 1, index=x.index)
    # )
    #
    # df["CCI"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.CCI(x.high, x.low, x.close, timeperiod=t), index=x.index)
    # )
    #
    # df["CMO"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.CMO(x.close), index=x.index)
    # )
    #
    # df["ADOSC"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ADOSC(x.high, x.low, x.close, x.volume, fastperiod=t - 3, slowperiod=4 + t),
    #                            index=x.index)
    # )
    #
    # df["ADX"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ADX(x.high, x.low, x.close, timeperiod=t), index=x.index)
    # )
    #
    # df["WILLR"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.WILLR(x.high, x.low, x.close, timeperiod=t), index=x.index)
    # )
    #
    # df["RSI14"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.RSI(x.close, timeperiod=14), index=x.index)
    # )
    #
    # df["TRIX14"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.TRIX(x.close, timeperiod=30) / x.close, index=x.index)
    # )
    #
    # df["SAR"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.SAR(x.high, x.low), index=x.index)
    # )
    #
    # def f_aroon(df):
    #     aroon_up, aroon_down = talib.AROON(high=df.high, low=df.low, timeperiod=14)  # enter the parameter you need
    #     return pd.DataFrame({'aroon_up': aroon_up, 'aroon_down': aroon_down}, index=df.index)
    #
    # df[['AROON_UP', 'AROON_DOWN']] = df.groupby(by=['symbol']).apply(lambda x: f_aroon(x))
    #
    # df["BOP"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.BOP(x.open, x.high, x.low, x.close), index=x.index))
    #
    # def f_slowd(df):
    #     slowk, slowd = talib.STOCH(df.high, df.low, df.close, fastk_period=14,
    #                                slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    #     return pd.DataFrame({'stoch': slowk / slowd}, index=df.index)
    #
    # df["SLOWD"] = df.groupby(by=['symbol']).apply(lambda x: f_slowd(x))
    #
    # df["ULTOSC"] = df.groupby(by=['symbol']).apply(lambda x: pd.DataFrame(
    #     talib.ULTOSC(df.high, df.low, df.close, timeperiod1=7, timeperiod2=14, timeperiod3=28), index=x.index))
    #
    # df["AD"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.AD(x.high, x.low, x.close, x.volume), index=x.index)
    # )
    #
    # df["OBV"] = df.groupby(by=['symbol']).apply(lambda x: pd.DataFrame(talib.OBV(x.close, x.volume), index=x.index))
    #
    # df["ATR"] = df.groupby(by=['symbol']).apply(lambda x: pd.DataFrame(
    #     talib.ATR(x.high, x.low, x.close, timeperiod=14), index=x.index))

    new_upload(df)

    return df


if __name__ == "__main__":
    df = run()
