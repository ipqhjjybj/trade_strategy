# encoding: UTF-8
import math

import numpy as np
from fractions import Fraction


def diff(arr):
    ret = []
    for i in range(1, len(arr)):
        ret.append(arr[i] - arr[i - 1])
    return ret


def speye(n):
    r = np.zeros((n, n))
    for i in range(n):
        r[i][i] = 1
    return r


def findbelow(s, number):
    sum = 0
    if len(s.shape) == 1:
        for i in range(s.shape[0]):
            if s[i] < number:
                sum += 1
    else:
        n, m = s.shape
        for i in range(n):
            for j in range(m):
                if s[i][j] < number:
                    sum += 1
    return sum


def peaks_func(x, y):
    return 3 * ((1 - x) ** 2) * math.exp(-1 * x * x - (y + 1) ** 2) \
           - 10 * (1.0 / 5 * x - x ** 3 - y ** 5) * math.exp(-1 * x * x - y * y) \
           - 1.0 / 3 * math.exp(-1 * (x + 1) ** 2 - y ** 2)


def peaks(x, y):
    assert x.shape != y.shape, "peaks shape should same"
    ret = np.zeros(x.shape)
    if len(x.shape) == 1:
        for i in range(x.shape[0]):
            ret[i] = peaks_func(x[i], y[i])
    else:
        n, m = x.shape
        for i in range(n):
            for j in range(m):
                ret[i][j] = peaks_func(x[i][j], y[i][j])
    return ret


def frange(start, stop, jump, end=False, via_str=False):
    """
    Equivalent of Python 3 range for decimal numbers.

    Notice that, because of arithmetic errors, it is safest to
    pass the arguments as strings, so they can be interpreted to exact fractions.

    >>> assert Fraction('1.1') - Fraction(11, 10) == 0.0
    >>> assert Fraction( 0.1 ) - Fraction(1, 10) == Fraction(1, 180143985094819840)

    Parameter `via_str` can be set to True to transform inputs in strings and then to fractions.
    When inputs are all non-periodic (in base 10), even if decimal, this method is safe as long
    as approximation happens beyond the decimal digits that Python uses for printing.


    For example, in the case of 0.1, this is the case:

    >>> assert str(0.1) == '0.1'
    >>> assert '%.50f' % 0.1 == '0.10000000000000000555111512312578270211815834045410'


    If you are not sure whether your decimal inputs all have this property, you are better off
    passing them as strings. String representations can be in integer, decimal, exponential or
    even fraction notation.

    >>> assert list(frange(1, 100.0, '0.1', end=True))[-1] == 100.0
    >>> assert list(frange(1.0, '100', '1/10', end=True))[-1] == 100.0
    >>> assert list(frange('1', '100.0', '.1', end=True))[-1] == 100.0
    >>> assert list(frange('1.0', 100, '1e-1', end=True))[-1] == 100.0
    >>> assert list(frange(1, 100.0, 0.1, end=True))[-1] != 100.0
    >>> assert list(frange(1, 100.0, 0.1, end=True, via_str=True))[-1] == 100.0

    """
    if via_str:
        start = str(start)
        stop = str(stop)
        jump = str(jump)
    start = Fraction(start)
    stop = Fraction(stop)
    jump = Fraction(jump)
    while start < stop:
        yield float(start)
        start += jump
    if end and start == stop:
        yield (float(start))
