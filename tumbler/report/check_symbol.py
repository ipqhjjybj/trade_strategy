# coding=utf-8
from datetime import datetime

from tumbler.service.mysql_service import MysqlService
from tumbler.data.download.download_manager import DownloadManager
from tumbler.data.binance_data import BinanceClient
from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.technique import PD_Technique
import tumbler.function.figure as figure


def check_symbol(symbol, func, show_figure=True, flag_download_data=True):
    mysql_service_manager = MysqlService()
    binance_client = BinanceClient()

    if flag_download_data:
        manager = DownloadManager(mysql_service_manager, binance_client)
        manager.recovery_all_kline(include_symbols=[symbol], suffix="",
                                   periods=[Interval.HOUR.value],
                                   start_datetime=datetime(2021, 1, 1), end_datetime=datetime.now())

    bars = mysql_service_manager.get_bars(symbols=[symbol], period=Interval.HOUR.value)

    df = BarData.get_pandas_from_bars(bars)

    df = func(df)
    df.to_csv(f"{symbol}.csv")
    df = PD_Technique.quick_income_compute(df, sllippage=0, rate=0.001, size=1, name="income", debug=False)
    # df = PD_Technique.quick_compute_current_drawdown(df, name_cur_down="cur_down", name_max_drawdown="max_down")
    ans_dic = PD_Technique.assume_strategy(df)
    print("sharpe_val:{}, trade_times:{}, total_income:{}, rate:{}"
          .format(ans_dic["sharpe_ratio"], ans_dic["trade_times"], ans_dic["total_income"], ans_dic["rate"]))
    if show_figure:
        figure.pd_plot(df, x_lable="datetime", y_lable="income")
    return df
