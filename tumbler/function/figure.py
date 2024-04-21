# encoding: UTF-8
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def arr_plot(arr):
    plt.figure(figsize=(10, 16))

    balance_plot = plt.subplot(2, 2, 1)
    balance_plot.set_title("Balance")

    t = pd.DataFrame(np.array(arr), columns=['balance'])
    t["balance"].plot()
    plt.show()


def dic_plot(dic):
    x_arr = list(dic.keys())
    x_arr.sort()
    y_arr = []
    for x in x_arr:
        y_arr.append(dic[x])

    pd_data = pd.DataFrame(np.array(y_arr), columns=['balance'])
    pd_data["date"] = x_arr
    pd_plot(pd_data, x_lable="date", y_lable="balance")


def pd_plot(pd_data, x_lable="date", y_lable="value"):
    '''
    绘制带日期的一根线
    res = pd.DataFrame(
        {
            "date": dt_arr, "ema200": sum_ema_200, "tengluo": sum_tengluolines,
            "ema_tengluo": talib.EMA(np.array(sum_tengluolines), timeperiod=14),
            "ori": ori_lines
        }
    )
    figure.pd_plot(res, x_lable="date", y_lable="ema_tengluo")
    '''
    pd_plot_serveral_lines(pd_data, x_lable, [y_lable])


def pd_plot_serveral_lines(pd_data, x_lable="date", y_lable_arr=[]):
    '''
    绘制带日期的一根线
    res = pd.DataFrame(
        {
            "date": dt_arr, "ema200": sum_ema_200, "tengluo": sum_tengluolines,
            "ema_tengluo": talib.EMA(np.array(sum_tengluolines), timeperiod=14),
            "ori": ori_lines
        }
    )
    figure.pd_plot(res, x_lable="date", y_lable="ema_tengluo")
    '''
    fig, ax = plt.subplots()

    x_data = pd_data[x_lable]
    if not isinstance(x_data[x_data.index[0]], datetime):
        x_data = [datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in x_data]

    for y_lable in y_lable_arr:
        y_data = pd_data[y_lable]

        ax.plot(x_data, y_data, label=y_lable)

    fig.autofmt_xdate()
    plt.legend()
    plt.show()


def pd_plot_serveral_figures(pd_data, x_lable="date", y_label_arr1=[], y_lable_arr2=[]):
    '''
    绘制带日期的一根线
    res = pd.DataFrame(
        {
            "date": dt_arr, "ema200": sum_ema_200, "tengluo": sum_tengluolines,
            "ema_tengluo": talib.EMA(np.array(sum_tengluolines), timeperiod=14),
            "ori": ori_lines
        }
    )
    figure.pd_plot(res, x_lable="date", y_lable="ema_tengluo")
    '''

    x_data = pd_data[x_lable]
    if not isinstance(x_data[x_data.index[0]], datetime):
        x_data = [datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in x_data]

    fig, ax = plt.subplots()
    for y_lable in y_label_arr1:
        ax = plt.subplot(211)
        y_data = pd_data[y_lable]
        ax.plot(x_data, y_data, label=y_lable)
        # plt.plot(x_data, y_data, label=y_lable)
        # fig.autofmt_xdate()
    plt.legend()

    for y_lable in y_lable_arr2:
        ax = plt.subplot(212)
        y_data = pd_data[y_lable]
        ax.plot(x_data, y_data, label=y_lable)
        # plt.plot(x_data, y_data, label=y_lable)
        # fig.autofmt_xdate()

    fig.autofmt_xdate()
    plt.legend()
    plt.show()
