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
all_nums = 0
for symbol in ["btc_usdt", "eth_usdt", "bnb_usdt", "sol_usdt"]:
#for symbol in ["sol_usdt"]:
	for period in [2,4,6,8,12]:
		all_nums += 1
		filepath = "/Users/shenzhuoheng/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/{}_1h_{}.csv" \
		        .format(symbol, period)


		to_sort_val = []
		for algo, strategy_name, description in trade_algorithm_all:

			df = pd.read_csv(filepath)
			df = eval(algo)
			df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income")
			ans_dic = PD_Technique.assume_strategy(df)
			
			msg = "{},sharpe_val:{}, trade_times:{}, total_income:{}, rate:{}".format(strategy_name, ans_dic["sharpe_ratio"], ans_dic["trade_times"], ans_dic["total_income"], ans_dic["rate"])

			ratingdic[strategy_name] += ans_dic["sharpe_ratio"]
			#ratingdic[strategy_name] += ans_dic["total_income"]
			#to_sort_val.append((ans_dic["sharpe_ratio"], msg))

# 0.5459663535283722 ema_strategy_10_60
# 0.507772676771389 ema_strategy_5_60
# 0.47384444322704067 three_line_strategy
# 0.43034408831128934 ema_strategy_5_30
# 0.40750781087387217 boll_strategy_50
# 0.38920355686615404 one_line_strategy_90
# 0.3793107171459673 one_line_strategy_60
# 0.3406756789145863 kingkeltner_strategy
# 0.3028239754038477 ema_slope_trend_follower
# 0.28772743475010437 roc_strategy
# 0.27520253822801727 boll_strategy_30
# 0.22207746843102857 one_line_strategy_30
# 0.22207746843102857 one_line_strategy
# 0.192603908435509 ema_rsi_strategy
# 0.13394678429323043 trix_strategy
# 0.1308970551766128 dmi_strategy
# 0.05274967858864425 regression_strategy
# 0.0236385140888417 macd_strategy
# -0.011337776305245396 osc_strategy
# -0.261916295034592 four_week_strategy

arr = []
for k, v in ratingdic.items():
	arr.append((v,k))
arr.sort(reverse=True)
for k, v in arr:
	print(k / all_nums, v)

