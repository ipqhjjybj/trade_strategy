from datetime import timedelta
from typing import Dict, Optional, Tuple
from abc import ABC, abstractmethod

import talib
from pandas import DataFrame
import pandas as pd

from tumbler.constant import Direction
from tumbler.function import datetime_from_str_to_time, datetime_from_str_to_datetime
from tumbler.function import timeframe_to_minutes, timeframe_to_seconds


class LocalTrade(object):
    enter_time = ""
    exit_time = ""
    enter_price = 0
    exit_price = 0
    exit_reason = ""
    size = 0
    direction = ""

    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])

    @staticmethod
    def get_pandas_from_list(trade_arr):
        dic = {"enter_time": [], "exit_time":[], "enter_price":[], "exit_price":[],
               "exit_reason": [], "size": [], "direction":[]}
        for trade in trade_arr:
            dic["enter_time"].append(trade.enter_time)
            dic["exit_time"].append(trade.exit_time)
            dic["enter_price"].append(trade.enter_price)
            dic["exit_price"].append(trade.exit_price)
            dic["exit_reason"].append(trade.exit_reason)
            dic["size"].append(trade.size)
            dic["direction"].append(trade.direction)
        return pd.DataFrame(dic, columns=["direction", "enter_time", "exit_time", "enter_price", "exit_price",
                                          "exit_reason", "size"])


class MyStrategy(ABC):
    '''
    这是个简单信号, 只会有单品种一笔交易，不涉及加仓等复杂逻辑
    策略模板，对每笔交易，通过 df 运算，计算买点，卖点
    '''

    # 是否支持开多
    support_long = True

    # 是否支持开空
    support_short = True

    # associated minimal roi
    # 是否有止盈上的支持
    minimal_roi: Dict
    support_take_profit = False

    # trailing stoploss
    # 支持Atr系数 止损
    atr_mul = 4.0
    atr_length = 14
    support_atr_stop = False

    # trailing percent stoploss
    # 支持百分比止损
    stop_pct_percent = 0.1
    support_pct_stop = False

    #
    timeframe = "4h"

    def __init__(self, config: dict) -> None:
        d = self.__dict__
        for key in config:
            d[key] = config[key]

        self.size = 1

        self.enter_time = ""  # 入场时机
        self.exit_time = ""  # 出场时机
        self.exit_reason = ""  # 出场原因

        self.enter_price = 0  # 入场价位, 简单点之间用收盘价
        self.exit_price = 0  # 出场价位

        ## 用于止损
        self.high_after_entry = 0
        self.low_after_entry = 0

        self.timeframe_seconds = timeframe_to_seconds(self.timeframe)
        self.timeframe_minutes = timeframe_to_minutes(self.timeframe)

        self.trade_list = []

    @abstractmethod
    def populate_indicators(self, df: DataFrame, metadata: dict) -> DataFrame:
        """
        Populate indicators that will be used in the Buy and Sell strategy
        :param df:
        :param dataframe: DataFrame with data from the exchange
        :param metadata: Additional information, like the currently traded pair
        :return: a Dataframe with all mandatory indicators for the strategies
        """
        return df

    @abstractmethod
    def populate_buy_trend(self, df: DataFrame, metadata: dict) -> DataFrame:
        """
        Based on TA indicators, populates the buy signal for the given dataframe
        :param df:
        :param dataframe: DataFrame
        :param metadata: Additional information, like the currently traded pair
        :return: DataFrame with buy column
        """
        return df

    @abstractmethod
    def populate_sell_trend(self, df: DataFrame, metadata: dict) -> DataFrame:
        """
        Based on TA indicators, populates the sell signal for the given dataframe
        :param df:
        :param dataframe: DataFrame
        :param metadata: Additional information, like the currently traded pair
        :return: DataFrame with sell column
        """
        return df

    def min_roi_reached_entry(self, trade_dur: int) -> Tuple[Optional[int], Optional[float]]:
        """
        Based on trade duration defines the ROI entry that may have been reached.
        :param trade_dur: trade duration in minutes
        :return: minimal ROI entry value or None if none proper ROI entry was found.
        """
        # Get highest entry in ROI dict where key <= trade-duration
        roi_list = list(filter(lambda x: int(x) <= trade_dur, self.minimal_roi.keys()))
        if not roi_list:
            return None, None
        roi_entry = max(roi_list)
        return roi_entry, self.minimal_roi[str(roi_entry)]

    def is_support_stop_loss(self):
        return self.support_atr_stop or self.support_pct_stop

    # enter_time, exit_time, enter_price, exit_price, exit_reason
    def finished_trade(self, size, direction):
        self.enter_time = str(datetime_from_str_to_datetime(self.enter_time) +
                              timedelta(seconds=self.timeframe_seconds))
        self.exit_time = str(datetime_from_str_to_datetime(self.exit_time) + timedelta(seconds=self.timeframe_seconds))
        self.trade_list.append(LocalTrade(
            enter_time=self.enter_time,
            exit_time=self.exit_time,
            enter_price=self.enter_price,
            exit_price=self.exit_price,
            exit_reason=self.exit_reason,
            size=size,
            direction=direction
        ))

    def get_trade_df(self):
        return LocalTrade.get_pandas_from_list(self.trade_list)

    def output_info(self):
        max_trade_start_time = ""
        max_trade_end_time = ""
        max_trade_duration = 0
        for trade in self.trade_list:
            time_duration = datetime_from_str_to_time(trade.exit_time) - datetime_from_str_to_time(trade.enter_time)
            if time_duration > max_trade_duration:
                max_trade_duration = time_duration
                max_trade_start_time = trade.enter_time
                max_trade_end_time = trade.exit_time
        print(f"max_trade_duration:{max_trade_duration}, bars:{max_trade_duration/self.timeframe_seconds} "
              f"start_time:{max_trade_start_time}, "
              f"end_time:{max_trade_end_time}")

    def clear_info(self):
        self.enter_time = ""  # 入场时机
        self.exit_time = ""  # 出场时机
        self.exit_reason = ""  # 出场原因

        self.enter_price = 0  # 入场价位, 简单点之间用收盘价
        self.exit_price = 0  # 出场价位

        ## 用于止损
        self.high_after_entry = 0
        self.low_after_entry = 0

        self.trade_list = []

    def get_pos(self, df: DataFrame, metadata: dict, name="pos", flag_record_trade=False):
        self.clear_info()

        df["buy"] = 0
        df["sell"] = 0
        df = self.populate_indicators(df, metadata)
        df = self.populate_buy_trend(df, metadata)
        df = self.populate_sell_trend(df, metadata)

        if self.support_atr_stop:
            df["_atr_val"] = talib.ATR(df.high, df.low, df.close, timeperiod=self.atr_length)

        ll = len(df.close)
        pos_arr = []
        now_pos = 0

        for i in range(ll):
            if now_pos != 0:
                if self.support_take_profit:
                    time_duration = datetime_from_str_to_time(df.datetime.iloc[i]) \
                                    - datetime_from_str_to_time(self.enter_time)
                    roi_entry_minutes, need_profit_rate = self.min_roi_reached_entry(time_duration)
                    now_profit_rate = (df.close.iloc[i] - self.enter_price) / self.enter_price * now_pos / abs(now_pos)
                    if now_profit_rate >= need_profit_rate:
                        now_pos = 0
                        self.exit_reason = "profit take"
                        self.exit_price = df.close.iloc[i]
                        self.exit_time = df.datetime.iloc[i]
                        if flag_record_trade:
                            if now_pos > 0:
                                self.finished_trade(self.size, Direction.LONG.value)
                            else:
                                self.finished_trade(self.size, Direction.SHORT.value)

                if self.is_support_stop_loss():
                    self.high_after_entry = max(self.high_after_entry, df.high.iloc[i])
                    self.low_after_entry = min(self.low_after_entry, df.low.iloc[i])

                    if self.support_atr_stop:
                        atr_val = df["_atr_val"].iloc[i] * self.atr_mul
                        if now_pos > 0:
                            if df.close.iloc[i] < self.high_after_entry - atr_val:
                                now_pos = 0
                                self.exit_reason = "stoploss atr"
                                self.exit_price = df.close.iloc[i]
                                self.exit_time = df.datetime.iloc[i]
                                if flag_record_trade:
                                    self.finished_trade(self.size, Direction.LONG.value)

                        if now_pos < 0:
                            if df.close.iloc[i] > self.low_after_entry + atr_val:
                                now_pos = 0
                                self.exit_reason = "stoploss atr"
                                self.exit_price = df.close.iloc[i]
                                self.exit_time = df.datetime.iloc[i]

                                if flag_record_trade:
                                    self.finished_trade(self.size, Direction.SHORT.value)

                    if self.support_pct_stop:
                        if now_pos > 0:
                            if df.close.iloc[i] < self.high_after_entry * (1 - self.stop_pct_percent):
                                now_pos = 0
                                self.exit_reason = "stoploss pct"
                                self.exit_price = df.close.iloc[i]
                                self.exit_time = df.datetime.iloc[i]

                                if flag_record_trade:
                                    self.finished_trade(self.size, Direction.LONG.value)

                        if now_pos < 0:
                            if df.close.iloc[i] > self.low_after_entry * (1 + self.stop_pct_percent):
                                now_pos = 0
                                self.exit_reason = "stoploss pct"
                                self.exit_price = df.close.iloc[i]
                                self.exit_time = df.datetime.iloc[i]

                                if flag_record_trade:
                                    self.finished_trade(self.size, Direction.SHORT.value)

            if now_pos == 0:
                if df.buy.iloc[i] and self.support_long:
                    now_pos = self.size
                elif df.sell.iloc[i] and self.support_short:
                    now_pos = -1 * self.size
                if now_pos:
                    self.enter_time = df.datetime.iloc[i]
                    self.enter_price = df.close.iloc[i]

                    if self.is_support_stop_loss():
                        self.high_after_entry = df.close.iloc[i]
                        self.low_after_entry = df.close.iloc[i]
            elif now_pos > 0:
                if df.sell.iloc[i]:
                    now_pos = 0

                    self.exit_time = df.datetime.iloc[i]
                    self.exit_price = df.close.iloc[i]
                    self.exit_reason = "signal sell"

                    if flag_record_trade:
                        self.finished_trade(self.size, Direction.LONG.value)

                    if self.support_short:
                        now_pos = -1 * self.size
                        self.enter_time = df.datetime.iloc[i]
                        self.enter_price = df.close.iloc[i]
            else:
                if df.buy.iloc[i]:
                    now_pos = 0

                    self.exit_time = df.datetime.iloc[i]
                    self.exit_price = df.close.iloc[i]
                    self.exit_reason = "signal buy"

                    if flag_record_trade:
                        self.finished_trade(self.size, Direction.SHORT.value)

                    if self.support_long:
                        now_pos = self.size
                        self.enter_time = df.datetime.iloc[i]
                        self.enter_price = df.close.iloc[i]

            pos_arr.append(now_pos)

        df[name] = pd.Series(pos_arr)
        return df
