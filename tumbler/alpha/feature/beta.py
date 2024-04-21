# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from tumbler.object import BarData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService


def cal_beta(df, rolling_periods):
    # Calc the stock and market retuens by computing log(n)/log(n-1)
    df[['A', 'B']] = np.log(
        df[['bench_close', 'work_close']] / df[['bench_close', 'work_close']].shift(1))

    df = df.drop(["bench_close", "work_close"], axis=1)

    beta = df.rolling(rolling_periods).cov(df, pairwise=True).drop(['A'], axis=1) \
        .unstack(1) \
        .droplevel(0, axis=1) \
        .apply(lambda row: row['A'] / row['B'], axis=1)

    return beta


def get_same_period_df(bench_bars, work_bars):
    bench_dic = BarData.get_dict_from_bars(bench_bars)
    work_dic = BarData.get_dict_from_bars(work_bars)
    union_keys = [x for x in list(bench_dic.keys()) if x in list(work_dic.keys())]
    union_keys.sort()

    dic = {"date": [], "bench_close": [], "work_close": []}
    for key_datetime_str in union_keys:
        dic["date"].append(key_datetime_str[:19])
        dic["bench_close"].append(bench_dic[key_datetime_str]["close"])
        dic["work_close"].append(work_dic[key_datetime_str]["close"])
    df = pd.DataFrame(dic)
    df.set_index('date', inplace=True)
    return df


def run(bench_symbol="btc_usdt", beta_arrs=[30, 60, 90, 180, 365], suffix="_usdt"):
    mysql_service_manager = MysqlService.get_mysql_service()
    symbols = mysql_service_manager.get_mysql_distinct_symbol(table=MysqlService.get_kline_table(Interval.DAY.value))
    symbols = [symbol for symbol in symbols if symbol.endswith(suffix)]
    bench_bars = mysql_service_manager.get_bars(symbols=[bench_symbol], period=Interval.DAY.value,
                                                start_datetime=datetime(2017, 1, 1),
                                                end_datetime=datetime.now() + timedelta(hours=10),
                                                sort_way="symbol")
    bench_bars.sort()
    for symbol in symbols:
        bars = mysql_service_manager.get_bars(symbols=[symbol], period=Interval.DAY.value,
                                              start_datetime=datetime(2017, 1, 1),
                                              end_datetime=datetime.now() + timedelta(hours=10),
                                              sort_way="symbol")
        bars.sort()

        ori_df = get_same_period_df(bench_bars, bars)
        for beta_period in beta_arrs:
            beta = cal_beta(ori_df.copy(), beta_period)
            beta = beta.dropna()
            factor_code = f"beta{beta_period}"

            ret = list(zip([factor_code] * len(beta.index), [symbol] * len(beta.index), list(beta.index), list(beta)))
            mysql_service_manager.replace_factor(ret, symbol, Interval.DAY.value, factor_code)


if __name__ == "__main__":
    run()
