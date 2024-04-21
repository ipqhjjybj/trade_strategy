# coding=utf-8

import pandas as pd
import talib
from collections import defaultdict

from tumbler.algo_trade import trade_algorithm_all
from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.bar import BarGenerator
from tumbler.function.technique import PD_Technique, FundManagement, MultiIndexMethod
from tumbler.constant import Direction
from tumbler.constant import Direction, EvalType


sllippage=0
rate=0.001
size=1

ratingdic = defaultdict(float)
symbol = "btc_usdt"
period = 6

strategy_df_dict = {}

filepath = "/Users/shenzhuoheng/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_1h_{}.csv".format(symbol, period)

for algo, strategy_name, description in trade_algorithm_all:
	df = pd.read_csv(filepath)
	df = eval(algo)
	df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
	strategy_df_dict[strategy_name] = list(df["income"])

df = pd.DataFrame(strategy_df_dict)
correlation_matrix = df.corr()
print(correlation_matrix)
correlation_matrix.to_csv("a.csv")