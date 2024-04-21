# coding=utf-8

import numpy as np
import talib
import pandas as pd
from copy import copy

from datetime import timedelta


def get_min_and_index(arr, ii, jj):
    if ii > jj:
        ii, jj = jj, ii
    pre_min = arr[jj]
    ind = jj
    for i in range(ii, jj):
        if arr[i] < pre_min:
            pre_min = arr[i]
            ind = i

    return pre_min, ind


def get_max_and_index(arr, ii, jj):
    if ii > jj:
        ii, jj = jj, ii
    pre_max = arr[jj]
    ind = jj
    for i in range(ii, jj):
        if arr[i] > pre_max:
            pre_max = arr[i]
            ind = i

    return pre_max, ind

