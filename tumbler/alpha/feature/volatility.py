# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.function.technique import Technique, PD_Technique


def run():
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
    # df.to_csv("test.csv")
    # Technique.er(am.close_array, 20)
    # df["ER10"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 10), index=x.index)
    # )
    # Technique.ema_std(am.close_array, 20)

    # df["EMA_STD20"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.EMA(x.close, 20), index=x.index)
    # )
    df["EMA_STD20"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.STDDEV(talib.EMA(x.close, 20).fillna(value=0), 5), index=x.index)
    )
    #df.to_csv("test-out.csv")

    mysql_service_manager.replace_factor_from_pd(df, Interval.DAY.value)

    return df


if __name__ == "__main__":
    df = run()
    # df.to_csv(".tumbler/debug3.csv")
    # print(df)
