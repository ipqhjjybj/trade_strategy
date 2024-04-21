# coding=utf-8

import numpy as np
import talib
import pandas as pd
from copy import copy

from datetime import timedelta
# from numba import njit

from tumbler.object import BarData, NewMergeTicker
from tumbler.constant import Interval


def seconds_generator(tickers, window):
    pre_second = -1
    n = len(tickers)
    accu_volume = 0
    accu_amount = 0
    accu_buy_price_volume = 0
    accu_sell_price_volume = 0
    accu_ask_fill = 0
    accu_bid_fill = 0

    ret = []
    for i in range(n):
        ticker = tickers[i]
        accu_volume += ticker["volume"]
        accu_amount += ticker["amount"]
        accu_buy_price_volume += ticker["max_buy_price_volume"]
        accu_sell_price_volume += ticker["min_sell_price_volume"]
        accu_ask_fill += ticker["askFill"]
        accu_bid_fill += ticker["bidFill"]

        dt = ticker["date"]
        if dt.second != pre_second:
            if dt.second % window == 0:
                new_ticker = NewMergeTicker()
                new_ticker.symbol = ticker["symbol"]
                new_ticker.accu_volume = accu_volume
                new_ticker.accu_amount = accu_amount
                new_ticker.accu_buy_price_volume = accu_buy_price_volume
                new_ticker.accu_sell_price_volume = accu_sell_price_volume
                new_ticker.accu_ask_fill = accu_ask_fill
                new_ticker.accu_bid_fill = accu_bid_fill
                new_ticker.date = dt

                for i in range(1, 11):
                    new_ticker.bid_prices[i - 1] = ticker["bid{}".format(i)]
                    new_ticker.ask_prices[i - 1] = ticker["ask{}".format(i)]
                    new_ticker.bid_volumes[i - 1] = ticker["bidvol{}".format(i)]
                    new_ticker.ask_volumes[i - 1] = ticker["askvol{}".format(i)]
                ret.append(new_ticker.get_np_array())

                accu_volume = 0
                accu_amount = 0
                accu_buy_price_volume = 0
                accu_sell_price_volume = 0
                accu_ask_fill = 0
                accu_bid_fill = 0

            pre_second = dt.second

    return np.concatenate(ret)


class BarGenerator:
    """
    For:
    1. generating 1 minute bar data from tick data
    2. generateing x minute bar/x hour bar data from 1 minute data

    Notice:
    1. for x minute bar, x must be able to divide 60: 2, 3, 5, 6, 10, 15, 20, 30
    2. for x hour bar, x can be any number
    """

    def __init__(self, on_bar, window=0, on_window_bar=None, interval=Interval.MINUTE.value, quick_minute=0):
        self.bar = None
        self.on_bar = on_bar

        self.interval = interval
        self.interval_count = 0

        self.window = window
        self.window_bar = None
        self.on_window_bar = on_window_bar

        self.last_tick = None
        self.last_bar = None

        self.add_quick_minute = quick_minute

    def update_tick(self, tick):
        """
        Update new tick data into generator.
        """
        new_minute = False

        # Filter tick data with 0 last price
        if not tick.last_price:
            return

        tick.datetime = tick.datetime + timedelta(minutes=self.add_quick_minute)
        if not self.bar:
            new_minute = True
        elif self.bar.datetime.minute != tick.datetime.minute:
            self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)
            self.on_bar(self.bar)
            new_minute = True

        if new_minute:
            self.bar = BarData()
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange
            self.bar.vt_symbol = tick.vt_symbol

            self.bar.open_price = tick.last_price
            self.bar.high_price = tick.last_price
            self.bar.low_price = tick.last_price
            self.bar.close_price = tick.last_price

            self.bar.datetime = tick.datetime
            self.bar.date = self.bar.datetime.strftime("%y-%m-%d")
            self.bar.time = self.bar.datetime.strftime("%H:%M:%S")

            self.bar.interval = Interval.MINUTE.value
            self.bar.open_interest = tick.open_interest
            self.bar.gateway_name = tick.gateway_name
        else:
            self.bar.high_price = max(self.bar.high_price, tick.last_price)
            self.bar.low_price = min(self.bar.low_price, tick.last_price)
            self.bar.close_price = tick.last_price
            self.bar.open_interest = tick.open_interest
            self.bar.datetime = tick.datetime

        if self.last_tick:
            volume_change = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_change, 0)

        self.last_tick = tick

    def new_bar(self, bar):
        # Generate timestamp for bar data
        if self.interval == Interval.MINUTE.value:
            # dt = bar.datetime.replace(second=0, microsecond=0)
            # append two minutes
            dt = bar.datetime.replace(second=0, microsecond=0)
        elif self.interval == Interval.DAY.value:
            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
        else:
            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)

        self.window_bar = BarData()
        self.window_bar.symbol = bar.symbol
        self.window_bar.exchange = bar.exchange
        self.window_bar.datetime = dt
        self.window_bar.gateway_name = bar.gateway_name
        self.window_bar.open_price = bar.open_price
        self.window_bar.high_price = bar.high_price
        self.window_bar.low_price = bar.low_price

    def update_bar(self, bar):
        """
        Update 1 minute bar into generator
        """
        # If not inited, creaate window bar object
        if not self.window_bar:
            self.new_bar(bar)
        # Otherwise, update high/low price into window bar
        else:
            self.window_bar.high_price = max(
                self.window_bar.high_price, bar.high_price)
            self.window_bar.low_price = min(
                self.window_bar.low_price, bar.low_price)

        # Update close price/volume into window bar
        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += float(bar.volume)
        self.window_bar.open_interest = bar.open_interest

        # Check if window bar completed
        finished = False

        if self.interval == Interval.MINUTE.value:
            # x-minute bar
            if not (bar.datetime.minute + 1) % self.window:
                finished = True
        elif self.interval == Interval.HOUR.value:
            if self.last_bar and bar.datetime.hour != self.last_bar.datetime.hour:
                # 1-hour bar
                if self.window == 1:
                    finished = True
                # x-hour bar
                else:
                    self.interval_count += 1

                    if not self.interval_count % self.window:
                        finished = True
                        self.interval_count = 0

        elif self.interval == Interval.DAY.value:
            if self.last_bar and (bar.datetime - timedelta(hours=8)).date() !=\
                    (self.last_bar.datetime - timedelta(hours=8)).date():
                # 1-day bar
                if self.window == 1:
                    finished = True
                # x-day bar
                else:
                    self.interval_count += 1

                    if not self.interval_count % self.window:
                        finished = True
                        self.interval_count = 0

        if finished:
            self.on_window_bar(self.window_bar)
            self.window_bar = None

            self.new_bar(bar)

        # Cache last bar object
        self.last_bar = bar

    def merge_bar_not_minute(self, bar):
        if not self.window_bar:
            self.new_bar(bar)
        else:
            self.window_bar.high_price = max(self.window_bar.high_price, bar.high_price)
            self.window_bar.low_price = min(self.window_bar.low_price, bar.low_price)

        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += float(bar.volume)
        self.window_bar.open_interest = bar.open_interest

        self.interval_count += 1

        if not (self.interval_count % self.window):
            self.on_window_bar(self.window_bar)
            self.window_bar = None

    def is_new_day(self, bar):
        return not self.last_bar or \
            (bar.datetime - timedelta(hours=8)).day != (self.last_bar.datetime - timedelta(hours=8)).day

    def is_new_afternoon(self, bar):
        return self.last_bar and (bar.datetime - timedelta(hours=8)).hour >= 12 and \
            (self.last_bar.datetime - timedelta(hours=8)).hour < 12

    def generate(self):
        """
        Generate the bar data and call callback immediately.
        """
        self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)
        self.on_bar(self.bar)
        self.bar = None


class PandasDeal(object):
    """
    用于处理各个周期的pandas数据
    """

    def __init__(self, func, window, interval, quick_minute=1, factor_nums=1, strategy=None):
        print(window, interval, quick_minute, factor_nums)
        self.strategy = strategy
        self.window = window
        self.interval = interval
        self.df = None
        self.func = func
        self.bg = BarGenerator(self.on_bar, window=window, on_window_bar=self.on_window_bar,
                               interval=interval, quick_minute=quick_minute)
        self.factor_nums = factor_nums
        self.init_array = []

        self.target_pos = 0
        self.trading = False

        self.last_bar = None
        self.last_window_bar = None

    def start_trading(self):
        self.trading = True

    def on_init(self):
        self.start_trading()

        self.compute_df()
        self.df = self.func(self.df)
        n = self.df.shape[0] - 1
        self.target_pos = self.df["pos"][n]

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.last_bar = copy(bar)
        self.bg.update_bar(bar)

    def get_inc_period(self):
        if self.interval == Interval.MINUTE.value:
            return timedelta(minutes=self.window)
        elif self.interval == Interval.HOUR.value:
            return timedelta(hours=self.window)
        else:
            return timedelta(days=self.window)

    def on_window_bar(self, bar: BarData):
        if self.last_window_bar and self.strategy:
            s1 = (bar.datetime - self.get_inc_period()).replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            s2 = self.last_window_bar.datetime.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            if s1 != s2:
                self.strategy.write_important_log(
                    "[PandasDeal] [on_bar] {} {} check bar time error! bar.datetime:{} last_window_bar.datetime:{}"
                        .format(self.interval, self.window, s1, s2))

        self.last_window_bar = copy(bar)
        self.strategy.write_log("[PandasDeal] [last_window_bar] {} {} {}".format(self.interval, self.window, bar.datetime))
        if not self.trading:
            self.init_array.append(bar.get_dict())
        else:
            self.work_df(bar)

    def work_df(self, bar: BarData):
        if self.df is not None:
            n = self.df.shape[0]
            # print("work_df", "n:", n, "shape", self.df.loc[n-1], self.factor_nums, bar.get_np_array(self.factor_nums))
            self.df.loc[n] = bar.get_np_array(self.factor_nums)
        else:
            self.init_array.append(bar.get_dict())
            self.compute_df()
            n = self.df.shape[0] - 1
            print("n:{}".format(n))
        self.df = self.func(self.df)
        self.target_pos = self.df["pos"][n]

    def compute_df(self):
        self.df = pd.DataFrame(self.init_array, columns=BarData.get_columns())
        self.df["pos"] = None

    def get_pos(self):
        if str(self.target_pos) == str(np.nan):
            self.target_pos = 0
        return self.target_pos

    def get_df(self):
        return self.df


class NewPandasDeal(object):
    """
    用于处理各个周期的pandas数据
    """

    def __init__(self, func, window, interval, quick_minute=1):
        print(window, interval, quick_minute)
        self.window = window
        self.interval = interval
        self.df = None
        self.func = func
        self.bg = BarGenerator(self.on_bar, window=window, on_window_bar=self.on_window_bar,
                               interval=interval, quick_minute=quick_minute)
        self.init_array = []
        self.trading = False

    def start_trading(self):
        self.trading = True

    def on_init(self):
        self.start_trading()

        self.compute_df()
        self.df = self.func(self.df)

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.bg.update_bar(bar)

    def on_window_bar(self, bar: BarData):
        if not self.trading:
            self.init_array.append(bar.get_dict())
        else:
            self.work_df(bar)

    def work_df(self, bar: BarData):
        if self.df is not None:
            n, m = self.df.shape
            self.df.loc[n] = bar.get_np_array(m - len(bar.get_columns()))
        else:
            self.init_array.append(bar.get_dict())
            self.compute_df()
        self.df = self.func(self.df)

    def compute_df(self):
        self.df = pd.DataFrame(self.init_array, columns=BarData.get_columns())

    def get_last_value_from_key(self, key, default_val=None):
        if self.df and key in self.df.columns:
            return self.df[key][self.df.shape[0] - 1]
        return default_val

    def get_pos(self):
        return self.get_last_value_from_key("pos", 0)

    def get_last_items(self):
        if self.df:
            keys = self.df.columns
            values = list(self.df.loc[self.df.shape[0] - 1])
            d = {}
            for key, value in zip(keys, values):
                if key not in ["datetime"]:
                    d[key + "_" + self.interval] = value
            return d
        return {}

    def get_df(self):
        return self.df


class ArrayManager(object):
    """
    For:
    1. time series container of bar data
    2. calculating technical indicator value
    """

    def __init__(self, size=100):
        """Constructor"""
        self.count = 0
        self.size = size
        self.inited = False

        self.open_array = np.zeros(size)
        self.high_array = np.zeros(size)
        self.low_array = np.zeros(size)
        self.close_array = np.zeros(size)
        self.volume_array = np.zeros(size)

        self.symbol_array = []
        self.exchange_array = []

        self.datetime_array = [None] * size

    def update_bar(self, bar):
        """
        Update new bar data into array manager.
        """
        self.symbol_array.append(bar.symbol)
        if len(self.symbol_array) > self.size:
            self.symbol_array.pop(0)
        self.exchange_array.append(bar.exchange)
        if len(self.exchange_array) > self.size:
            self.exchange_array.pop(0)

        self.count += 1
        if not self.inited and self.count >= self.size:
            self.inited = True

        self.open_array[:-1] = self.open_array[1:]
        self.high_array[:-1] = self.high_array[1:]
        self.low_array[:-1] = self.low_array[1:]
        self.close_array[:-1] = self.close_array[1:]
        self.volume_array[:-1] = self.volume_array[1:]
        self.datetime_array[:-1] = self.datetime_array[1:]

        self.open_array[-1] = bar.open_price
        self.high_array[-1] = bar.high_price
        self.low_array[-1] = bar.low_price
        self.close_array[-1] = bar.close_price
        self.volume_array[-1] = bar.volume
        self.datetime_array[-1] = bar.datetime

    def to_pandas_data(self):
        dic = {"symbol": self.symbol_array, "exchange": self.exchange_array, "datetime": self.datetime_array,
               "open": self.open_array, "high": self.high_array, "low": self.low_array,
               "close": self.close_array, "volume": self.volume_array}

        return pd.DataFrame(dic)

    @property
    def open(self):
        """
        Get open price time series.
        """
        return self.open_array

    @property
    def high(self):
        """
        Get high price time series.
        """
        return self.high_array

    @property
    def low(self):
        """
        Get low price time series.
        """
        return self.low_array

    @property
    def close(self):
        """
        Get close price time series.
        """
        return self.close_array

    @property
    def volume(self):
        """
        Get trading volume time series.
        """
        return self.volume_array

    def ma(self, n, array=False):
        """
        MA
        """
        result = talib.MA(self.close, n)
        if array:
            return result
        return result[-1]

    def sma(self, n, array=False):
        """
        Simple moving average.
        """
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    def kama(self, n, array=False):
        """
        KAMA.
        """
        result = talib.KAMA(self.close, n)
        if array:
            return result
        return result[-1]

    def wma(self, n, array=False):
        """
        WMA.
        """
        result = talib.WMA(self.close, n)
        if array:
            return result
        return result[-1]

    def apo(self, n, array=False):
        """
        APO.
        """
        result = talib.APO(self.close, n)
        if array:
            return result
        return result[-1]

    def cmo(self, n, array=False):
        """
        CMO.
        """
        result = talib.CMO(self.close, n)
        if array:
            return result
        return result[-1]

    def mom(self, n, array=False):
        """
        MOM. --> MTM
        """
        result = talib.MOM(self.close, n)
        if array:
            return result
        return result[-1]

    def ppo(self, n, array=False):
        """
        PPO.
        """
        result = talib.PPO(self.close, n)
        if array:
            return result
        return result[-1]

    def roc(self, n, array=False):
        """
        ROC.
        """
        result = talib.ROC(self.close, n)
        if array:
            return result
        return result[-1]

    def rocr(self, n, array=False):
        """
        ROCR.
        """
        result = talib.ROCR(self.close, n)
        if array:
            return result
        return result[-1]

    def rocp(self, n, array=False):
        """
        ROCP.
        """
        result = talib.ROCP(self.close, n)
        if array:
            return result
        return result[-1]

    def rocr_100(self, n, array=False):
        """
        ROCR100.
        """
        result = talib.ROCR100(self.close, n)
        if array:
            return result
        return result[-1]

    def trix(self, n, array=False):
        """
        TRIX.
        """
        result = talib.TRIX(self.close, n)
        if array:
            return result
        return result[-1]

    def std(self, n, array=False):
        """
        Standard deviation.
        """
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    def obv(self, n, array=False):
        """
        OBV.
        """
        result = talib.OBV(self.close, self.volume)
        if array:
            return result
        return result[-1]

    def cci(self, n, array=False):
        """
        Commodity Channel Index (CCI).
        """
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def atr(self, n, array=False):
        """
        Average True Range (ATR).
        """
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def natr(self, n, array=False):
        """
        NATR.
        """
        result = talib.NATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def rsi(self, n, array=False):
        """
        Relative Strenght Index (RSI).
        """
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    def macd(self, fast_period, slow_period, signal_period, array=False):
        """
        MACD.
        """
        macd, signal, hist = talib.MACD(
            self.close, fast_period, slow_period, signal_period
        )
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    def adx(self, n, array=False):
        """
        ADX.
        """
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def adxr(self, n, array=False):
        """
        ADXR.
        """
        result = talib.ADXR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def dx(self, n, array=False):
        """
        DX.
        """
        result = talib.DX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def minus_di(self, n, array=False):
        """
        MINUS_DI.
        """
        result = talib.MINUS_DI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def plus_di(self, n, array=False):
        """
        PLUS_DI.
        """
        result = talib.PLUS_DI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def willr(self, n, array=False):
        """
        WILLR.
        """
        result = talib.WILLR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def ultosc(self, array=False):
        """
        Ultimate Oscillator.
        """
        result = talib.ULTOSC(self.high, self.low, self.close)
        if array:
            return result
        return result[-1]

    def trange(self, array=False):
        """
        TRANGE.
        """
        result = talib.TRANGE(self.high, self.low, self.close)
        if array:
            return result
        return result[-1]

    def boll(self, n, dev, array=False):
        """
        Bollinger Channel.
        """
        mid = self.ma(n, array)
        std = self.std(n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

    def keltner(self, n, dev, array=False):
        """
        Keltner Channel.
        """
        mid = self.sma(n, array)
        atr = self.atr(n, array)

        up = mid + atr * dev
        down = mid - atr * dev

        return up, down

    def donchian(self, n, array=False):
        """
        Donchian Channel.
        """
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)

        if array:
            return up, down
        return up[-1], down[-1]

    def aroon(self, n, array=False):
        """
        Aroon indicator.
        """
        aroon_up, aroon_down = talib.AROON(self.high, self.low, n)

        if array:
            return aroon_up, aroon_down
        return aroon_up[-1], aroon_down[-1]

    def aroonosc(self, n, array=False):
        """
        Aroon Oscillator.
        """
        result = talib.AROONOSC(self.high, self.low, n)

        if array:
            return result
        return result[-1]

    def minus_dm(self, n, array=False):
        """
        MINUS_DM.
        """
        result = talib.MINUS_DM(self.high, self.low, n)

        if array:
            return result
        return result[-1]

    def plus_dm(self, n, array=False):
        """
        PLUS_DM.
        """
        result = talib.PLUS_DM(self.high, self.low, n)

        if array:
            return result
        return result[-1]

    def mfi(self, n, array=False):
        """
        Money Flow Index.
        """
        result = talib.MFI(self.high, self.low, self.close, self.volume, n)
        if array:
            return result
        return result[-1]

    def ad(self, n, array=False):
        """
        AD.
        """
        result = talib.AD(self.high, self.low, self.close, self.volume, n)
        if array:
            return result
        return result[-1]

    def adosc(self, n, array=False):
        """
        ADOSC.
        """
        result = talib.ADOSC(self.high, self.low, self.close, self.volume, n)
        if array:
            return result
        return result[-1]

    def bop(self, array=False):
        result = talib.BOP(self.open, self.high, self.low, self.close)

        if array:
            return result
        return result[-1]


# @njit
# def ref(x, d):
#     d = int(d)
#     n = len(x)
#     res = np.roll(x, d)
#     res[:min(n, d)] = np.nan
#     return res


# @njit
# def llv(x, d):
#     d = int(d)
#     n = len(x)
#     res = np.empty_like(x)
#     cm = x[0]
#     for i in range(min(d, n)):
#         ix = x[i]
#         if ix < cm:
#             cm = ix
#         res[i] = cm
#     for i in range(d, n):
#         cm = x[i - d + 1]
#         for j in range(i - d + 2, i + 1):
#             ix = x[j]
#             if ix < cm:
#                 cm = ix
#         res[i] = cm
#     return res


# @njit
# def hhv(x, d):
#     d = int(d)
#     n = len(x)
#     res = np.empty_like(x)
#     cm = x[0]
#     for i in range(min(d, n)):
#         ix = x[i]
#         if ix > cm:
#             cm = ix
#         res[i] = cm
#     for i in range(d, n):
#         cm = x[i - d + 1]
#         for j in range(i - d + 2, i + 1):
#             ix = x[j]
#             if ix > cm:
#                 cm = ix
#         res[i] = cm
#     return res

def every(x, n):
    return (x.rolling(window=n, center=False).sum()) == n


def sum(x, n):
    return x.rolling(window=n, center=False).sum()


def if_else(c, a, b):
    return np.where(c, a, b)


def ref(x, d):
    return x.shift(d)


def llv(x, d):
    return x.rolling(window=d, center=False).min()
    # return getattr(x.rolling(d, min_periods=1), "min")()
    # return Min(x, d)


def hhv(x, d):
    return x.rolling(window=d, center=False).max()
    # return getattr(x.rolling(d, min_periods=1), "max")()
    # return Max(x, d)


def kelch_mid(high, low, close, d):
    x = (high + low + close) / 3.0
    return getattr(x.rolling(d, min_periods=1), "mean")()


def kelch_down(high, low, close, d):
    x = ((4 * high - 2 * low + close) / 3)
    return getattr(x.rolling(d, min_periods=1), "mean")()


def kelch_up(high, low, close, d):
    x = (-2 * high + 4 * low + close) / 3
    return getattr(x.rolling(d, min_periods=1), "mean")()


def sma(arr, n):
    return talib.SMA(arr, n)


def roc(arr, n):
    return talib.ROC(arr, n)


def ma(arr, n):
    return talib.MA(arr, n)


def ema(arr, n):
    return talib.EMA(arr, n)


def boll_up(arr, n, nbdev=1):
    mid = talib.MA(arr, n)
    std = talib.STDDEV(arr, n)

    up = mid + std * nbdev
    return up


def boll_down(arr, n, nbdev=1):
    mid = talib.MA(arr, n)
    std = talib.STDDEV(arr, n)

    down = mid - std * nbdev
    return down


def stddev(arr, n):
    return talib.STDDEV(arr, n)


def atr(H, L, C, n):
    return talib.ATR(H, L, C, n)


def rsi(arr, n):
    return talib.RSI(arr, n)


def adx(H, L, C, n):
    return talib.ADX(H, L, C, n)


def ht_trend(x):
    return talib.HT_TRENDLINE(x)


def delay_(x, d):
    d = int(d)
    n = len(x)
    res = np.roll(x, d)
    res[:min(n, d)] = np.nan
    return res


def trail_stop_pct(close, high, low, insig, pct):
    pos = np.empty_like(close)
    n1 = len(close)
    p = 0
    high_after_entry = 0
    low_after_entry = 0
    for i in range(n1):
        if p > 0:
            high_after_entry = max(high[i], high_after_entry)
            stop_prc = high_after_entry * (1 - pct)
            if close[i] < stop_prc:
                p = 0
            if insig[i] < 0:
                p = 0
        elif p < 0:
            low_after_entry = min(low[i], low_after_entry)
            stop_prc = low_after_entry * (1 + pct)
            if close[i] > stop_prc:
                p = 0
            if insig[i] > 0:
                p = 0
        elif p == 0:
            if insig[i] > 0:
                p = 1
                high_after_entry = close[i]
            elif insig[i] < 0:
                p = -1
                low_after_entry = close[i]
        pos[i] = p

    outsig = pos - delay_(pos, 1)
    if n1 > 0:
        outsig[0] = 0

    # print("total_trade:{}".format(2 * sum([x for x in outsig if x == 1])))

    return pos
    # outsig = pos - delay_(pos, 1)
    # if n1 > 0:
    #     outsig[0] = 0
    # return outsig


def trail_stop_atr(close, high, low, insig, period_atr, ratio_atr):
    pos = np.empty_like(close)
    n1 = len(close)
    p = 0
    high_after_entry = 0
    low_after_entry = 0
    atr_arr = atr(high, low, close, period_atr) * ratio_atr
    for i in range(n1):
        if p > 0:
            high_after_entry = max(high[i], high_after_entry)
            stop_prc = high_after_entry - atr_arr[i]
            if close[i] < stop_prc:
                p = 0
            if insig[i] < 0:
                p = 0
        elif p < 0:
            low_after_entry = min(low[i], low_after_entry)
            stop_prc = low_after_entry + atr_arr[i]
            if close[i] > stop_prc:
                p = 0
            if insig[i] > 0:
                p = 0
        elif p == 0:
            if insig[i] > 0:
                p = 1
                high_after_entry = close[i]
            elif insig[i] < 0:
                p = -1
                low_after_entry = close[i]
        pos[i] = p

    outsig = pos - delay_(pos, 1)
    if n1 > 0:
        outsig[0] = 0

    return pos


def crossup(x, y):
    '''
    金叉买入死叉平掉
    '''
    n = len(x)
    pos = np.zeros(len(x))
    for i in range(n):
        if x[i] > y[i]:
            pos[i] = 1
        else:
            pos[i] = 0
    return pos


def crossdown(x, y):
    '''
    死叉卖出，金叉平掉
    '''
    n = len(x)
    pos = np.zeros(len(x))
    for i in range(n):
        if x[i] > y[i]:
            pos[i] = 0
        else:
            pos[i] = -1
    return pos


# @njit
def crossup_s__(x, y):
    n = len(x)
    res = np.zeros(len(x))
    ix1 = x[0]
    iy1 = y[0]
    for j in range(1, n):
        ix = x[j]
        iy = y[j]
        if ix > iy and ix1 < iy1:
            res[j] = 1
        ix1 = ix
        iy1 = iy
    return res


def crossup_s(x, y, pct, close, high, low):
    '''
    金叉买入， 最高点开始的百分比止损出场
    计算完入场信号，后开始计算止损
    '''
    in_sig = crossup_s__(x, y)
    return trail_stop_pct(close, high, low, in_sig, pct)


def crossup_a(x, y, period_atr, ratio_atr, close, high, low):
    '''
    金叉买入， 最高点开始的ATR百分比止损出场
    计算完入场信号，后开始计算止损
    '''
    in_sig = crossup_s__(x, y)
    return trail_stop_atr(close, high, low, in_sig, period_atr, ratio_atr)


def crossup_c(x, y):
    return crossup_s__(x, y)


def and_c(x, y):
    for i in range(len(x)):
        if x[i] > 0 and y[i] > 0:
            x[i] = 1
        else:
            x[i] = 0
    return x


def or_c(x, y):
    for i in range(len(x)):
        if x[i] > 0 or y[i] > 0:
            x[i] = 1
        else:
            x[i] = 0
    return x


def reverse_c(x):
    for i in range(len(x)):
        if x[i] > 0:
            x[i] = 0
        else:
            x[i] = 1
    return x


def crossdown_s__(x, y):
    n = len(x)
    res = np.zeros(len(x))
    ix1 = x[0]
    iy1 = y[0]
    for j in range(1, n):
        ix = x[j]
        iy = y[j]
        if ix < iy and ix1 > iy1:
            res[j] = -1
        ix1 = ix
        iy1 = iy
    return res


def crossdown_s(x, y, pct, close, high, low):
    '''
    死叉卖出， 最低点开始的百分比止损出场
    计算完入场信号，后开始计算止损
    '''
    in_sig = crossdown_s__(x, y)
    return trail_stop_pct(close, high, low, in_sig, pct)


def crossdown_a(x, y, period_atr, ratio_atr, close, high, low):
    '''
    死叉卖出， 最低点开始的ATR百分比止损出场
    计算完入场信号，后开始计算止损
    '''
    in_sig = crossdown_s__(x, y)
    return trail_stop_atr(close, high, low, in_sig, period_atr, ratio_atr)


def crossdown_c(x, y):
    return crossdown_s__(x, y) * -1


def gt_c(x, val):
    return x > val


def lt_c(x, val):
    return x < val


def sar_long__(v):
    res = np.zeros(len(v))
    n = v.shape[0]
    iv2 = v[1]
    iv1 = v[2]
    for i in range(3, n):
        iv = v[i]
        if iv1 < iv2 and iv1 < iv:
            res[i] = 1
        elif iv1 > iv2 and iv1 > iv:
            res[i] = 0
        else:
            res[i] = res[i - 1]

        iv2 = iv1
        iv1 = iv
    return res


def sar_short__(v):
    res = np.zeros(len(v))
    n = v.shape[0]
    iv2 = v[1]
    iv1 = v[2]
    for i in range(3, n):
        iv = v[i]
        if iv1 > iv2 and iv1 > iv:
            res[i] = -1
        elif iv1 < iv2 and iv1 < iv:
            res[i] = 0
        else:
            res[i] = res[i - 1]
        iv2 = iv1
        iv1 = iv
    return res


def sar_long(H, L, p):
    v = talib.SAR(H, L, p, p)
    return sar_long__(v)


def sar_short(H, L, p):
    v = talib.SAR(H, L, p, p)
    return sar_short__(v)


def sar_long_s__(v):
    res = np.zeros(len(v))
    n = v.shape[0]
    iv2 = v[1]
    iv1 = v[2]
    for i in range(3, n):
        iv = v[i]
        if iv1 < iv2 and iv > iv1:
            res[i] = 1
        iv2 = iv1
        iv1 = iv
    return res


def sar_short_s__(v):
    res = np.zeros(len(v))
    n = v.shape[0]
    iv2 = v[1]
    iv1 = v[2]
    for i in range(3, n):
        iv = v[i]
        if iv1 > iv2 and iv < iv1:
            res[i] = -1
        iv2 = iv1
        iv1 = iv
    return res


def sar_long_s(H, L, p, pct, close, high, low):
    v = talib.SAR(H, L, p, p)
    in_sig = sar_long_s__(v)
    return trail_stop_pct(close, high, low, in_sig, pct)
    # out_sig = trail_stop(close, high, low, in_sig, pct)
    # return merge_sig(in_sig, out_sig)


def sar_long_a(H, L, p, atr_period, atr_ratio, close, high, low):
    v = talib.SAR(H, L, p, p)
    in_sig = sar_long_s__(v)
    return trail_stop_atr(close, high, low, in_sig, atr_period, atr_ratio)


def sar_short_s(H, L, p, pct, close, high, low):
    v = talib.SAR(H, L, p, p)
    in_sig = sar_short_s__(v)
    return trail_stop_pct(close, high, low, in_sig, pct)
    # out_sig = trail_stop(close, high, low, in_sig, pct)
    # return merge_sig(in_sig, out_sig)


def sar_short_a(H, L, p, atr_period, atr_ratio, close, high, low):
    v = talib.SAR(H, L, p, p)
    in_sig = sar_short_s__(v)
    return trail_stop_atr(close, high, low, in_sig, atr_period, atr_ratio)
    # out_sig = trail_stop(close, high, low, in_sig, pct)
    # return merge_sig(in_sig, out_sig)


def bbandup(x, n, d):
    return talib.BBANDS(x, n, d, d)[0]


def bbanddown(x, n, d):
    return talib.BBANDS(x, n, d, d)[2]
