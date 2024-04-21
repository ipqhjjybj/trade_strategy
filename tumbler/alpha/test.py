# coding=utf-8

from functools import partial

from deap import gp
import talib

from tumbler.function.technique import PD_Technique


def ts_rank(a, window=10):
    """
    Wrapper function to estimate rolling rank.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series rank over the past window days.
    """
    return a + window


def make_partial_func(func, window=None, period=None, constant=None):
    ret_func = func
    if window:
        ret_func = partial(func, window=window)
        ret_func.__name__ = func.__name__ + "_" + str(window)
    if period:
        ret_func = partial(func, period=period)
        ret_func.__name__ = func.__name__ + "_" + str(period)
    if constant:
        ret_func = partial(func, constant=constant)
        ret_func.__name__ = func.__name__ + "_" + str(constant).replace('.', "__").replace("-", "___")
    return ret_func


s = make_partial_func(ts_rank, window=10)
print(s(5))
# s.__name__ = "5"
# print(s.__name__)

pset = gp.PrimitiveSet("MAIN", 1)
pset.addPrimitive(s, 1)

s = talib.MA
print(s.__name__)
