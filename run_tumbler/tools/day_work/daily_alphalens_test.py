# coding=utf-8

import os
from copy import copy
from datetime import datetime, timedelta

import talib
import pandas as pd
import numpy as np

import alphalens as al

from tumbler.service.mysql_service import MysqlService
from tumbler.record.client_quick_query import get_future_symbols
from tumbler.function import datetime_from_str_to_datetime
from tumbler.function.technique import Technique


sys_path_dir = ".tumbler"
day_binance_file = os.path.join(sys_path_dir, "binance_day.csv")

mysql_service_manager = MysqlService()


def day_factor_func(df):
    # val_arr = talib.ADX(df["high"], df["low"], df["close"], timeperiod=14) * \
    #           talib.ADXR(df["high"], df["low"], df["close"], timeperiod=14)
    # val_arr = talib.ROC(df["close"], timeperiod=30)
    val_arr = Technique.boll(df["close"], 20, dev=1)
    return val_arr


def get_use_symbols(suffix, all_data=True):
    day_symbols = mysql_service_manager.get_mysql_distinct_symbol(table='kline_1day')
    day_symbols = [x for x in day_symbols if x.endswith(suffix)]
    if not all_data:
        day_symbols = [x for x in day_symbols if x in get_future_symbols(reload=False)]
    else:
        day_symbols = day_symbols
    return day_symbols


def get_df():
    df = pd.read_csv(day_binance_file)
    date_list = list(df["date"])
    date_list = [datetime_from_str_to_datetime(x) for x in date_list]
    df["date"] = date_list
    return df


def make_alphalen_price_df(df, use_symbols, suffix="_usdt"):
    save_filepath = f".tumbler/price{suffix}.csv"
    if os.path.exists(save_filepath):
        df = pd.read_csv(save_filepath, index_col=0)
        df.index = [datetime_from_str_to_datetime(x) for x in list(df.index)]
        return df
    date_list = list(set(list(df["date"])))
    date_list.sort()

    price_df = pd.DataFrame(np.random.rand(len(date_list), 1), index=date_list, columns=["test"])
    for symbol in use_symbols:
        tdf = copy(df[df.symbol == symbol])
        price_df[symbol] = np.nan
        for dt_str, close_price in zip(list(tdf["date"]), list(tdf["close"])):
            price_df.loc[dt_str, symbol] = close_price
    price_df = price_df.drop(columns=["test"])
    price_df.to_csv(save_filepath)
    return price_df


def make_alphalen_factor_df(df, use_symbols, suffix="_usdt"):
    index_array = []
    factor_array = []
    for symbol in use_symbols:
        if symbol.endswith(suffix):
            tdf = copy(df[df.symbol == symbol])
            if len(tdf.index) == 0:
                continue

            val_arr = day_factor_func(tdf)
            for date, factor_val in zip(tdf.date, val_arr):
                index_array.append((date, symbol))
                factor_array.append(factor_val)

    index = pd.MultiIndex.from_tuples(index_array, names=['date', 'symbol'])
    df = pd.DataFrame(factor_array, index=index)
    df.to_csv(".tumbler/factor.csv")
    print(df)
    return df


def run(suffix="_usdt", all_data=True, start_time=datetime(2017, 1, 1), end_time=datetime.now() + timedelta(hours=3)):
    day_symbols = get_use_symbols(suffix, all_data)
    print(day_symbols)
    df = get_df()
    print(df)
    price_df = make_alphalen_price_df(df, day_symbols, suffix)
    print(price_df)

    factor_df = make_alphalen_factor_df(df, day_symbols, suffix)
    print(factor_df)

    df = al.utils.get_clean_factor_and_forward_returns(factor_df, price_df)
    print(df)
    df.to_csv(".tumbler/alphalens.csv")
    return df


factor_df = run(suffix="_usdt", all_data=True)
