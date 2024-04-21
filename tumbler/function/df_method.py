# encoding: UTF-8

# 合并数据
from copy import copy
import pandas as pd

from datetime import datetime, timedelta
import numpy as np


def connect_df(df_arr):
    sum_len = 0
    new_df_arr = []
    for df in df_arr:
        n_df = copy(df)
        n_df.index = n_df.index + sum_len
        new_df_arr.append(n_df)
        sum_len += len(n_df)
    return pd.concat(new_df_arr)


def get_rows(df, not_include_fields=[]):
    columns = [key for key in df.columns if key not in not_include_fields]

    array = []
    for index, row in df.iterrows():
        to_append_arr = [row["datetime"]]
        dic = {}
        for key in columns:
            dic[key] = row[key]
        to_append_arr.append(dic)
        array.append(to_append_arr)
    return array


def get_end_datetime(str_datetime, period, use_type):
    d = datetime.strptime(str_datetime, "%Y-%m-%d %H:%M:%S")
    if use_type == "1h":
        d = d + timedelta(hours=1 * period)
    elif use_type == "1m":
        d = d + timedelta(minutes=1 * period)
    elif use_type == "1d":
        d = d + timedelta(days=1 * period)
    return d


def judge_datetime_ge(base_datetime, base_period, base_type, from_datetime, from_period, from_type):
    return get_end_datetime(base_datetime, base_period, base_type) >= get_end_datetime(from_datetime, from_period,
                                                                                       from_type)


def go_merge_df(base_df, from_df, base_period=15, base_type="1m", from_period=4, from_type="1h",
                not_include_fields=["symbol", "exchange", "datetime", "open", "high", "low", "close"]):
    base_row = get_rows(base_df, [])
    from_row = get_rows(from_df, not_include_fields)

    new_from_keys = [key for key in from_df.columns if key not in not_include_fields]
    new_key_name_dict = {}
    new_from_dict = {}
    for key in new_from_keys:
        new_key = "{}_{}_{}".format(from_type, from_period, key)
        new_key_name_dict[key] = new_key
        new_from_dict[new_key] = []

    # from_index 表示该条信息是已经能获得的信息
    from_index = -1
    n_from = len(from_row)

    array_dict = {}
    for i in range(len(base_row)):
        # print("DO i:{}".format(i))
        base_datetime, base_dict = base_row[i]

        # from_datetime >= from_row[i][0]
        while from_index + 1 < n_from and judge_datetime_ge(
                base_datetime, base_period, base_type, from_row[from_index + 1][0], from_period, from_type):
            # print("change:", base_datetime,  from_row[from_index + 1][0])
            from_index = from_index + 1

        if from_index == -1:
            for key, new_key in new_key_name_dict.items():
                new_from_dict[new_key].append(np.nan)
        else:
            for key, new_key in new_key_name_dict.items():
                new_from_dict[new_key].append(from_row[from_index][1][key])

    ret_df = copy(base_df)
    for key in new_from_dict.keys():
        ret_df[key] = np.array(new_from_dict[key])
    return ret_df



