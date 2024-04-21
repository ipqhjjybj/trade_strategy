# coding=utf-8

import pandas as pd
import talib

from tumbler.algo_trade import trade_algorithm_all
from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.bar import BarGenerator
from tumbler.function.technique import PD_Technique, FundManagement, MultiIndexMethod
from tumbler.constant import Direction
from tumbler.constant import Direction, EvalType


symbol = "btc_usdt"
sllippage=0
rate=0.001
size=1
period=8


filepath = "/Users/shenzhuoheng/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_1h_{}.csv" \
        .format(symbol, period)


to_sort_val = []
for algo, strategy_name, description in trade_algorithm_all:

	df = pd.read_csv(filepath)
	df = eval(algo)
	df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
	ans_dic = PD_Technique.assume_strategy(df)
	msg = "{},sharpe_val:{}, trade_times:{}, total_income:{}, rate:{}".format(strategy_name, ans_dic["sharpe_ratio"], ans_dic["trade_times"], ans_dic["total_income"], ans_dic["rate"])

	to_sort_val.append((ans_dic["sharpe_ratio"], msg))

to_sort_val.sort(reverse=True)
for val, msg in to_sort_val:
	print(val, msg)



