# coding=utf-8
from datetime import datetime, timedelta

import pandas as pd
import matplotlib.pyplot as plt

import talib

from tumbler.alpha.alpha101 import Alphas
from tumbler.alpha.strategy_module import FactorModel
from tumbler.function.technique import PD_Technique, FundManagement, MultiIndexMethod, AlphaManager

from tumbler.object import BarData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.function.technique import Technique, PD_Technique


def func(obj, df, func_name):
    # df 是multiindex
    # 索引是[symbol，datetime]
    df[func_name.upper()] = obj.call_func(func_name)


def run():
    mysql_service_manager = MysqlService.get_mysql_service()
    symbols = mysql_service_manager.get_mysql_distinct_symbol(table=MysqlService.get_kline_table(Interval.DAY.value))

    bars = mysql_service_manager.get_bars(symbols=[], period=Interval.DAY.value,
                                          start_datetime=datetime(2017, 1, 1),
                                          end_datetime=datetime.now() + timedelta(hours=10),
                                          sort_way="symbol")

    bars = BarData.suffix_filter(bars, suffix="_usdt")

    df = BarData.get_pandas_from_bars(bars)
    df = df.set_index(["symbol", "datetime"]).sort_index()

    alpha_obj = Alphas(df)

    for func_name in alpha_obj.config_funcs:
        if func_name.startswith("alpha"):
            print(func_name)
            try:
                func(alpha_obj, df, func_name)
            except Exception as ex:
                print(ex)

    # func(alpha_obj, df, "alpha001")

    # df.to_csv("alpha.csv")

    df = df.reset_index()

    mysql_service_manager.replace_factor_from_pd(df, Interval.DAY.value)
    return df


if __name__ == "__main__":
    run()
