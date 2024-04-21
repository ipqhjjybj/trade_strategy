# encoding: UTF-8

import pandas as pd
import numpy as np

from math import fabs
from math import sqrt

from tumbler.function.technique import PD_Technique


def quick_compute_factor(symbol="btc_usdt", sllippage=7, rate=0.0003, size=1):
    filepath = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_min1_pd.csv" \
        .format(symbol)
    filepath = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_1h_1.csv" \
        .format(symbol)
    df = pd.read_csv(filepath)
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos")
    df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
    print(df)


if __name__ == "__main__":
    quick_compute_factor()
