# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData, FundamentalData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService


def run():
    '''
    def f(x):
        upper, middle, lower = talib.BBANDS(x, ...) #enter the parameter you need
            return pd.DataFrame({'upper':upper, 'middle':middle, 'lower':lower },
                                index=x.index)
    df[['upper','middle','lower']] = df.groupby('type').apply(lambda x : f(x.x))

    or:

    pd_data["roc30"] = pd_data.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 30), index=x.index)
    )

    '''
    mysql_service_manager = MysqlService.get_mysql_service()

    bars = mysql_service_manager.get_bars(symbols=[], period=Interval.DAY.value,
                                          start_datetime=datetime(2017, 1, 1),
                                          end_datetime=datetime.now() + timedelta(hours=10),
                                          sort_way="symbol")

    bars = BarData.suffix_filter(bars, suffix="_usdt")
    bars.sort()

    price_df = BarData.get_pandas_from_bars(bars)
    print(price_df)

    fd_arr = mysql_service_manager.get_fundamentals()
    fd_df = FundamentalData.get_pandas_from_fd_arr(fd_arr)

    print(fd_df)

    supply_df = fd_df[["symbol", "max_supply"]]

    print(supply_df)

    merge_df = pd.merge(price_df, supply_df, how='left', on='symbol')

    # merge_df.to_csv(".tumbler/merge_df.csv")
    return merge_df


def test():
    mysql_service_manager = MysqlService.get_mysql_service()
    merge_df = pd.read_csv(".tumbler/merge_df.csv", index_col=0)
    # merge_df["total_market_cap"] = merge_df["max_supply"] * merge_df["close"]
    #
    # merge_df = merge_df.drop(columns=["max_supply"])
    # mysql_service_manager.replace_factor_from_pd(merge_df, Interval.DAY.value)

    merge_df["size"] = np.log(merge_df["max_supply"] * merge_df["close"] + 1e-8)
    merge_df = merge_df.drop(columns=["max_supply"])
    merge_df.to_csv("merge_df2.csv")
    mysql_service_manager.replace_factor_from_pd(merge_df, Interval.DAY.value)


if __name__ == "__main__":
    # pd_data = run()
    test()
    # pd_data.to_csv(".tumbler/debug3.csv")
    # print(pd_data)
