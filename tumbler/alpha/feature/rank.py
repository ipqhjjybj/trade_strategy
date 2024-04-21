# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData, FactorData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.function.technique import Technique, PD_Technique

use_start_time = datetime(2017, 1, 1)
use_end_time = datetime.now() + timedelta(hours=10)


def merge_factor(df, factor_codes):
    global use_start_time, use_end_time
    if isinstance(factor_codes, str):
        factor_codes = [factor_codes]
    mysql_service_manager = MysqlService.get_mysql_service()
    factor_ret = mysql_service_manager.get_factors(
        factor_codes=factor_codes,
        interval=Interval.DAY.value,
        start_dt=use_start_time,
        end_dt=use_end_time
    )
    for factor_code in factor_codes:
        factor_df = FactorData.get_factor_df(factor_ret, factor_code)
        df = pd.merge(df, factor_df, how='left', left_on=['symbol', 'datetime'], right_on=['symbol', 'datetime'])

        rank_code = f"rank_{factor_code}"
        df[rank_code] = df.groupby(by=['datetime']).apply(
            lambda x: pd.DataFrame(pd.qcut(df[factor_code], [0, 0.2, 0.4, 0.6, 0.8, 1.0]
                                           , labels=[rank_code + "_1", rank_code + "_2",
                                                     rank_code + "_3", rank_code + "_4",
                                                     rank_code + "_5"
                                                     ]), index=x.index))

        return df


def make_feature(df):
    global use_start_time, use_end_time
    factor_codes = []
    factor_codes += ["DROC5", "DROC10", "DROC20", "DROC30", "DROC60", "DROC90"]
    factor_codes += ["ER10", "ER20", "ER30", "ER60", "ER90", "ER120"]
    factor_codes += ["DER10", "DER20", "DER30", "DER60", "DER90", "DER120"]
    factor_codes += ["ROC1", "ROC3", "ROC5", "ROC10", "ROC20", "ROC30", "ROC60", "ROC90", "120"]
    factor_codes += ["SIZE"]

    df = merge_factor(df, factor_codes)
    df = df.drop(columns=factor_codes)
    return df


def run():
    global use_start_time, use_end_time
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
                                          start_datetime=use_start_time,
                                          end_datetime=use_end_time,
                                          sort_way="symbol")

    bars = BarData.suffix_filter(bars, suffix="_usdt")

    # bars = mysql_service_manager.get_bars(symbols=["bnb_usdt"], period=Interval.DAY.value,
    #                                       start_datetime=datetime(2017, 1, 1),
    #                                       end_datetime=datetime.now() + timedelta(hours=10),
    #                                       sort_way="symbol")
    bars.sort()

    df = BarData.get_pandas_from_bars(bars)
    df = df.set_index(["symbol", "datetime"]).sort_index().reset_index()

    df = make_feature(df)
    mysql_service_manager.replace_factor_from_pd(df, Interval.DAY.value)

    return df


if __name__ == "__main__":
    df = run()
    # df.to_csv(".tumbler/debug3.csv")
    # print(df)
