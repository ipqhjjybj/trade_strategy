# coding=utf-8

import pandas as pd
import os
from datetime import datetime

import numpy as np

from tumbler.function.technique import PD_Technique
from tumbler.service import log_service_manager

df_instance_dic = {}

config = {
    "start_time": None,
    "end_time": datetime(2020, 6, 6),
    "slippage": 0,
    "rate": 0.001,
    "size": 1,
    "symbols": ["btc_usdt", "eth_usdt"],
    "periods": [1, 2, 4, 6, 12]
}


def set_config(params):
    for k, v in params.items():
        if k in config.keys():
            config[k] = v


def get_df(symbol="btc_usdt", period=4, _type="1h"):
    filepath = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_{}_{}.csv" \
        .format(symbol, _type, period)

    if not os.path.exists(filepath):
        log_service_manager.write_log(f"[get_df] {symbol} {period} not found!")
        return None

    if filepath in df_instance_dic.keys():
        df = df_instance_dic[filepath]
    else:
        df = pd.read_csv(filepath)
        df_instance_dic[filepath] = df

    if config["start_time"] or config["end_time"]:
        df = filter_df(df, config["start_time"], config["end_time"])

    log_service_manager.write_log("[get_df] filepath:{}".format(filepath))
    return df


def get_list_reports(df, fun_str_list, file_name="assume_report.csv", sort_by="avg_sharpe",
                     flag_write_file=True, flag_get_diff_report=False, reverse=True):
    report_list = []
    for func_arr in fun_str_list:
        func_str = str(func_arr)
        df = PD_Technique.eval(df, func_arr, name="pos")

        df = PD_Technique.quick_income_compute(df, config["slippage"], config["rate"], config["size"], name="income")
        ans_dic = PD_Technique.assume_strategy(df)

        tot_ans_dic = ans_dic
        tot_ans_dic["code"] = func_str.replace(',', "::")

        if flag_get_diff_report:
            ans_dic = assume_one_func_str(func_arr)

            for k, v in ans_dic.items():
                tot_ans_dic[k] = v

        if sort_by:
            report_list.append((tot_ans_dic[sort_by], tot_ans_dic))
        else:
            report_list.append((tot_ans_dic["sharpe_ratio"], tot_ans_dic))

    if reverse:
        report_list.sort(reverse=True)

    if flag_write_file:
        f = open(file_name, "w")
        labels = ["code", "sharpe_ratio", "trade_times", "total_income", "trade_times_per_day"]
        if flag_get_diff_report:
            labels.extend(["avg_sharpe", "win_times", "loss_times", "sum_trade_times", "avg_trade_times_per_day",
                           "sharpe_list", "rate_list"])
        f.write("{}\n".format(','.join(labels)))
        for _, tot_ans_dic in report_list:
            arr = [tot_ans_dic[k] for k in labels]
            arr = [str(x) for x in arr]
            line = ','.join(arr)
            f.write(line + "\n")
        f.close()
    return report_list


def filter_df(df, start_time=None, end_time=None):
    '''
    df, start_time=datetime(2005, 1, 1), end_time=datetime(2022, 1, 1)
    '''
    if start_time:
        df = df[start_time.strftime("%Y-%m-%d") <= df.datetime]
    if end_time:
        df = df[end_time.strftime("%Y-%m-%d") >= df.datetime]
    return df


def assume(func_arr, symbol, period, start_time=None, end_time=None,
           _type="1h", show_figure=False):
    df = get_df(symbol, period=period, _type=_type)

    if start_time or end_time:
        df = filter_df(df, start_time, end_time)

    df = PD_Technique.eval(df, func_arr)
    df = PD_Technique.quick_income_compute(df, config["slippage"], config["rate"], config["size"], name="income")

    if show_figure:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 16))

        balance_plot = plt.subplot(2, 2, 1)
        balance_plot.set_title("Balance")
        df["income"].plot()

        # drawdown_plot = plt.subplot(4, 1, 2)
        # drawdown_plot.set_title("Drawdown")
        # drawdown_plot.fill_between(range(len(df)), df["drawdown"].values)

        # pnl_plot = plt.subplot(4, 1, 3)
        # pnl_plot.set_title("Daily Pnl")
        # df["net_pnl"].plot(kind="bar", legend=False, grid=False, xticks=[])

        # signal_plot = plt.subplot(2, 2, 2)
        # signal_plot.set_title("signal display")
        #
        # df["signal"].plot()

        plt.show()
    return df


def recent_rise(func_arr, symbol, period, start_time=None, end_time=None, _type="1h"):
    df = get_df(symbol, period=period, _type=_type)

    if start_time or end_time:
        df = filter_df(df, start_time, end_time)

    df = PD_Technique.eval(df, func_arr)
    df = PD_Technique.quick_income_compute(df, config["slippage"], config["rate"], config["size"], name="income")

    ans_dic = PD_Technique.assume_strategy_rise(df)

    return ans_dic


def write_recent_rise_report(func_display_arr, file_name="recent_rise_report.csv", sort_by_key=None, reverse=True):
    ret = []

    for fd in func_display_arr:
        log_service_manager.write_log("now work :{}".format(fd["name"]))
        ans_dic = recent_rise(fd["func_arr"], symbol=fd["symbol"], period=fd["period"])
        line = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
            fd["name"], fd["symbol"], fd["period"], ans_dic["1d"], ans_dic["3d"],
            ans_dic["7d"], ans_dic["1m"], ans_dic["3m"], ans_dic["6m"], ans_dic["1y"], ans_dic["2y"], ans_dic["all"],
            ans_dic["1-3m"], ans_dic["1-6m"], ans_dic["3-6m"], ans_dic["1m-1y"], ans_dic["3m-1y"],  ans_dic["6m-1y"],
            ans_dic["1m-2y"], ans_dic["3m-2y"], ans_dic["6m-2y"], ans_dic["1y-2y"],
            ans_dic["6m-9m"], ans_dic["9m-12m"], ans_dic["12m-15m"], ans_dic["15m-18m"],
            ans_dic["18m-21m"], ans_dic["21m-24m"]
        )
        log_service_manager.write_log(line)

        if sort_by_key:
            ret.append((ans_dic[sort_by_key], line))
        else:
            ret.append((fd["name"], line))

    ret.sort()
    if reverse:
        ret.reverse()

    f = open(file_name, "w")
    f.write("name,symbol,period,1d,3d,7d,1m,3m,6m,1y,2y,all,1-3m,1-6m,3-6m,1m-1y,3m-1y,6m-1y,1m-2y,3m-2y,6m-2y,1y-2y,"
            "6m-9m,9m-12m,12m-15m,15m-18m,18m-21m,21m-24m\n")
    for _, line in ret:
        f.write(line + '\n')
    f.close()


def write_diff_symbols_compare_report(func_arr, symbols=config["symbols"],
                                      periods=config["periods"],
                                      file_name="diff_symbols_report.csv", sort_by_key="sharpe_ratio",
                                      reverse=True, _type="1h", flag_write_file=True):
    ret = []
    for symbol in symbols:
        for period in periods:
            df = get_df(symbol, period=period, _type=_type)
            if df is None or len(df.index) == 0:
                log_service_manager.write_log(f"[write_diff_symbols_compare_report] {symbol} {period} data empty!")
                continue

            df = PD_Technique.eval(df, func_arr)
            df = PD_Technique.quick_income_compute(df, config["slippage"], config["rate"], config["size"],
                                                   name="income")

            ans_dic = PD_Technique.assume_strategy(df)

            rd = {
                "func_arr": str(func_arr).replace(",", "::"),
                "symbol": symbol,
                "period": period,
                "trade_total_days": ans_dic["trade_total_days"],
                "trade_times_per_day": ans_dic["trade_times_per_day"],
                "sharpe_ratio": ans_dic["sharpe_ratio"],
                "trade_times": ans_dic["trade_times"],
                "total_income": ans_dic["total_income"],
                "rate": ans_dic["rate"],
                "df": ans_dic["df"]
            }

            if sort_by_key:
                ret.append((ans_dic[sort_by_key], rd))
            else:
                ret.append(("{}{}".format(symbol, period), rd))

    ret.sort()
    if reverse:
        ret.reverse()

    if flag_write_file:
        f = open(file_name, "w")
        f.write("func_str,symbol,period,sharpe,trade_times,total_income,rate\n")
        for _, rd in ret:
            line = "{},{},{},{},{},{},{}".format(rd["func_arr"], rd["symbol"], rd["period"],
                                                 rd["sharpe_ratio"], rd["trade_times"],
                                                 rd["total_income"], rd["rate"])
            f.write(line + "\n")
        f.close()
    return ret


def get_daily_merge_figure(df_arr, use_per_stock_money=100000, show_figure=True):
    '''
    根据最后一天的日期价格， 按照每个跑100000， 重新调整交易报告
    然后按调整完的交易报告，累加所有净值
    '''

    def judge_income_use_close(balance_arr, c, um=100000):
        xishu = um / c
        for i in range(len(balance_arr)):
            balance_arr[i] = xishu * balance_arr[i]

        return balance_arr

    def get_new_balances_use_dates(balance_arr, ori_date_arr):
        d = {}
        for i in range(len(ori_date_arr)):
            d[ori_date_arr[i]] = balance_arr[i]
        for date in all_dates:
            if date not in d.keys():
                d[date] = np.nan

        items = list(d.items())
        items.sort()
        v_arr = [x[1] for x in items]
        last_v = 0
        for i in range(len(v_arr)):
            if str(v_arr[i]) == str(np.nan):
                v_arr[i] = last_v
            last_v = v_arr[i]
        return np.array(v_arr)

    if len(df_arr) == 0:
        log_service_manager.write_log("[get_daily_merge_figure] df_arr is zero!")
        return

    if isinstance(df_arr[0], tuple):
        df_arr = [d[1] for d in df_arr]

    if isinstance(df_arr[0], dict):
        df_arr = [d["df"] for d in df_arr]

    all_date_sets = set([])
    for df in df_arr:
        for d in list(df["date"]):
            all_date_sets.add(d)

    all_dates = list(all_date_sets)
    all_dates.sort()

    sum_ans = np.zeros(len(all_dates))
    for df in df_arr:
        if len(df["close"]) > 0:
            last_close_price = df["close"][len(df["close"])-1]
            df["balance"] = judge_income_use_close(df["balance"], last_close_price, use_per_stock_money)
            sum_ans += get_new_balances_use_dates(df["balance"], df["date"])

    df = pd.DataFrame(sum_ans, columns=['balance'])
    strategy_dic = PD_Technique.assume_strategy_df(df)
    if show_figure:
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10, 16))

        balance_plot = plt.subplot(2, 2, 1)
        balance_plot.set_title("Balance")
        df["balance"].plot()

        plt.show()

    return strategy_dic


def statistics_diff_symbols_compare_report(ret):
    if not ret:
        return {
            "sum_rate": 0,
            "avg_rate": 0
        }
    sum_rate = 0
    for _, rd in ret:
        sum_rate += rd["rate"]
    return {
        "sum_rate": sum_rate,
        "avg_rate": sum_rate * 1.0 / len(ret)
    }


def assume_one_func_str(func_arr, symbols=config["symbols"],
                        periods=config["periods"],
                        file_name="diff_symbols_report.csv", sort_by_key="sharpe_ratio",
                        reverse=True, _type="1h", flag_write_file=False):
    ret = write_diff_symbols_compare_report(func_arr=func_arr, symbols=symbols,
                                            periods=periods, file_name=file_name, sort_by_key=sort_by_key,
                                            reverse=reverse, _type="1h", flag_write_file=flag_write_file)

    sum_trade_times = 0
    sum_avg_trade_times_per_day = 0
    sum_sharpe = 0
    win_times = 0
    loss_times = 0
    sharpe_list = []
    rate_list = []
    for _, rd in ret:
        sum_trade_times += rd["trade_times"]
        sum_avg_trade_times_per_day += rd["trade_times_per_day"]
        sum_sharpe += rd["sharpe_ratio"]
        sharpe_list.append("[{}:{}]".format(str(rd["period"]), str(rd["sharpe_ratio"])))
        rate_list.append("[{}:{}]".format(str(rd["period"]), str(rd["rate"])))
        if rd["total_income"] > 0:
            win_times += 1
        else:
            loss_times += 1

    return {
        "sharpe_list": '::'.join(sharpe_list),
        "rate_list": '::'.join(rate_list),
        "sum_sharpe": sum_sharpe,
        "avg_sharpe": sum_sharpe * 1.0 / len(ret),
        "sum_trade_times": sum_trade_times,
        "avg_trade_times_per_day": sum_avg_trade_times_per_day * 1.0 / len(ret),
        "win_times": win_times,
        "loss_times": loss_times
    }
