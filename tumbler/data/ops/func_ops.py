# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


from __future__ import division
from __future__ import print_function

import numpy as np
import pandas as pd
import talib

from scipy.stats import percentileofscore

from .base import Expression, ExpressionOps
from tumbler.service.log_service import get_module_logger

try:
    from ._libs.rolling import rolling_slope, rolling_rsquare, rolling_resi
    from ._libs.expanding import expanding_slope, expanding_rsquare, expanding_resi
except ImportError as err:
    print(
        "#### Do not import qlib package in the repository directory in case of importing qlib from . without compiling #####"
    )
    raise

__all__ = (
    "Ref",
    "Max",
    "Min",
    "Sum",
    "Mean",
    "Std",
    "Var",
    "Skew",
    "Kurt",
    "Med",
    "Mad",
    "Slope",
    "Rsquare",
    "Resi",
    "Rank",
    "Quantile",
    "Count",
    "EMA",
    "WMA",
    "Corr",
    "Cov",
    "Delta",
    "Abs",
    "Sign",
    "Log",
    "Power",
    "Add",
    "Sub",
    "Mul",
    "Div",
    "Greater",
    "Less",
    "And",
    "Or",
    "Not",
    "Gt",
    "Ge",
    "Lt",
    "Le",
    "Eq",
    "Ne",
    "Mask",
    "IdxMax",
    "IdxMin",
    "If",
    "BBANDS",
    "MACD",
)

np.seterr(invalid="ignore")


def Ref(series, N):
    if series.empty:
        return series  # Pandas bug, see: https://github.com/pandas-dev/pandas/issues/21049
    elif N == 0:
        series = pd.Series(series.iloc[0], index=series.index)
    else:
        series = series.shift(N)  # copy
    return series


def Rolling(series, N, func):
    if N == 0:
        series = getattr(series.expanding(min_periods=1), func)()
    elif 0 < N < 1:
        series = series.ewm(alpha=N, min_periods=1).mean()
    else:
        series = getattr(series.rolling(N, min_periods=1), func)()
        # series.iloc[:self.N-1] = np.nan
    # series[isnull] = np.nan
    return series


def Max(series, N):
    return Rolling(series, N, "max")


def Min(series, N):
    return Rolling(series, N, "min")


def Sum(series, N):
    return Rolling(series, N, "sum")


def Mean(series, N):
    return Rolling(series, N, "mean")


def Std(series, N):
    return Rolling(series, N, "std")


def Var(series, N):
    return Rolling(series, N, "var")


def Skew(series, N):
    if N != 0 and N < 3:
        raise ValueError("The rolling window size of Skewness operation should >= 3")
    return Rolling(series, N, "skew")


def Kurt(series, N):
    if N != 0 and N < 4:
        raise ValueError("The rolling window size of Kurtosis operation should >= 5")
    return Rolling(series, N, "kurt")


def Med(series, N):
    return Rolling(series, N, "median")


def Mad(series, N):
    def mad(x):
        x1 = x[~np.isnan(x)]
        return np.mean(np.abs(x1 - x1.mean()))

    if N == 0:
        series = series.expanding(min_periods=1).apply(mad, raw=True)
    else:
        series = series.rolling(N, min_periods=1).apply(mad, raw=True)
    return series


def Slope(series, N):
    if N == 0:
        series = pd.Series(expanding_slope(series.values), index=series.index)
    else:
        series = pd.Series(rolling_slope(series.values, N), index=series.index)
    return series


def Rsquare(series, N):
    if N == 0:
        series = pd.Series(expanding_rsquare(series.values), index=series.index)
    else:
        series = pd.Series(rolling_rsquare(series.values, N), index=series.index)
        series.loc[np.isclose(series.rolling(N, min_periods=1).std(), 0, atol=2e-05)] = np.nan
    return series


def Resi(series, N):
    if N == 0:
        series = pd.Series(expanding_resi(series.values), index=series.index)
    else:
        series = pd.Series(rolling_resi(series.values, N), index=series.index)
    return series


def Rank(series, N):
    def rank(x):
        if np.isnan(x[-1]):
            return np.nan
        x1 = x[~np.isnan(x)]
        if x1.shape[0] == 0:
            return np.nan
        return percentileofscore(x1, x1[-1]) / len(x1)

    if N == 0:
        series = series.expanding(min_periods=1).apply(rank, raw=True)
    else:
        series = series.rolling(N, min_periods=1).apply(rank, raw=True)
    return series


def Quantile(series, N, qscore):
    if N == 0:
        series = series.expanding(min_periods=1).quantile(qscore)
    else:
        series = series.rolling(N, min_periods=1).quantile(qscore)
    return series


def Count(series, N):
    return Rolling(series, N, "count")


def EMA(series, N):
    def exp_weighted_mean(x):
        a = 1 - 2 / (1 + len(x))
        w = a ** np.arange(len(x))[::-1]
        w /= w.sum()
        return np.nansum(w * x)

    if N == 0:
        series = series.expanding(min_periods=1).apply(exp_weighted_mean, raw=True)
    elif 0 < N < 1:
        series = series.ewm(alpha=N, min_periods=1).mean()
    else:
        series = series.ewm(span=N, min_periods=1).mean()
    return series


def WMA(series, N):
    def weighted_mean(x):
        w = np.arange(len(x))
        w = w / w.sum()
        return np.nanmean(w * x)

    if N == 0:
        series = series.expanding(min_periods=1).apply(weighted_mean, raw=True)
    else:
        series = series.rolling(N, min_periods=1).apply(weighted_mean, raw=True)
    return series


def PairRolling(series_left, series_right, N, func):
    if N == 0:
        series = getattr(series_left.expanding(min_periods=1), func)(series_right)
    else:
        series = getattr(series_left.rolling(N, min_periods=1), func)(series_right)
    return series


def Corr(series_left, series_right, N):
    res = PairRolling(series_left, series_right, N, "corr")
    res.loc[
        np.isclose(series_left.rolling(N, min_periods=1).std(), 0, atol=2e-05)
        | np.isclose(series_right.rolling(N, min_periods=1).std(), 0, atol=2e-05)
        ] = np.nan
    return res


def Cov(series_left, series_right, N):
    return PairRolling(series_left, series_right, N, "cov")


def Delta(series, N):
    if N == 0:
        series = series - series.iloc[0]
    else:
        series = series - series.shift(N)
    return series


def ElemOperator(series, func):
    return getattr(np, func)(series)


def Abs(series):
    return ElemOperator(series, "abs")


def Sign(series):
    series = series.astype(np.float32)
    return getattr(np, "sign")(series)


def Log(series):
    return ElemOperator(series, "log")


def Power(series, exponent):
    return getattr(np, "power")(series, exponent)


def PairOperator(series_left, series_right, func):
    return getattr(np, func)(series_left, series_right)


def Add(series_left, series_right):
    return PairOperator(series_left, series_right, "add")


def Sub(series_left, series_right):
    return PairOperator(series_left, series_right, "subtract")


def Mul(series_left, series_right):
    return PairOperator(series_left, series_right, "multiply")


def Div(series_left, series_right):
    return PairOperator(series_left, series_right, "divide")


def Greater(series_left, series_right):
    return PairOperator(series_left, series_right, "maximum")


def Less(series_left, series_right):
    return PairOperator(series_left, series_right, "minimum")


def And(series_left, series_right):
    return PairOperator(series_left, series_right, "bitwise_and")


def Or(series_left, series_right):
    return PairOperator(series_left, series_right, "bitwise_or")


def Not(series):
    return ElemOperator(series, "bitwise_not")


def Gt(series_left, series_right):
    return PairOperator(series_left, series_right, "greater")


def Ge(series_left, series_right):
    return PairOperator(series_left, series_right, "greater_equal")


def Lt(series_left, series_right):
    return PairOperator(series_left, series_right, "less")


def Le(series_left, series_right):
    return PairOperator(series_left, series_right, "less_equal")


def Eq(series_left, series_right):
    return PairOperator(series_left, series_right, "equal")


def Ne(series_left, series_right):
    return PairOperator(series_left, series_right, "not_equal")


def Mask(series, instument):
    return series


def IdxMax(series, N):
    if N == 0:
        series = series.expanding(min_periods=1).apply(lambda x: x.argmax() + 1, raw=True)
    else:
        series = series.rolling(N, min_periods=1).apply(lambda x: x.argmax() + 1, raw=True)
    return series


def IdxMin(series, N):
    if N == 0:
        series = series.expanding(min_periods=1).apply(lambda x: x.argmin() + 1, raw=True)
    else:
        series = series.rolling(N, min_periods=1).apply(lambda x: x.argmin() + 1, raw=True)
    return series


def If(series_cond, series_left, series_right):
    return pd.Series(np.where(series_cond, series_left, series_right), index=series_cond.index)


def BBANDS(series, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0):
    return getattr(talib, "BBANDS")(series, timeperiod, nbdevup, nbdevdn, matype)


def MACD(series, fast_period, slow_period, signal_period):
    return getattr(talib, "MACD")(series, fast_period, slow_period, signal_period)
