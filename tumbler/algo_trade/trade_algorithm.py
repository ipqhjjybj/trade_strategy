# coding=utf-8
import pandas as pd
import talib

from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.function.bar import BarGenerator
from tumbler.function.technique import PD_Technique, FundManagement, MultiIndexMethod
from tumbler.constant import Direction, EvalType
import tumbler.function.figure as figure


# algorithum,  strategy, descritpion
trade_algorithm_all = [
	("PD_Technique.trix_strategy(df, n=20, ma_length=3, name=\"pos\")", "trix_strategy", "this is a strategy"),
	("PD_Technique.osc_strategy(df, fast_length=5, slow_length=20, ma_osc=20, name=\"pos\")", "osc_strategy", "this is a strategy"),
	("PD_Technique.one_line_strategy(df, n=30, name=\"pos\")", "one_line_strategy_30", ""),
	("PD_Technique.one_line_strategy(df, n=60, name=\"pos\")", "one_line_strategy_60", ""),
	("PD_Technique.one_line_strategy(df, n=90, name=\"pos\")", "one_line_strategy_90", ""),
	("PD_Technique.macd_strategy(df, fast_length=12, slow_length=26, macd_length=9, name=\"pos\")", "macd_strategy", ""),
	("PD_Technique.boll_strategy(df, n=30, offset=1, name=\"pos\")", "boll_strategy_30", ""),
	("PD_Technique.boll_strategy(df, n=50, offset=1, name=\"pos\")", "boll_strategy_50", ""),
	("PD_Technique.ema_strategy(df, fast_length=5, slow_length=30, name=\"pos\")", "ema_strategy_5_30", ""),
	("PD_Technique.ema_strategy(df, fast_length=5, slow_length=60, name=\"pos\")", "ema_strategy_5_60", ""),
	("PD_Technique.ema_strategy(df, fast_length=10, slow_length=60, name=\"pos\")", "ema_strategy_10_60", ""),
	("PD_Technique.ema_strategy(df, fast_length=5, slow_length=90, name=\"pos\")", "ema_strategy_5_90", ""),
	("PD_Technique.dmi_strategy(df, length=14, name=\"pos\")", "dmi_strategy", ""),
	("PD_Technique.ema_slope_trend_follower(df, ma_average_type=\"EMA\", slopeflen=5, slopeslen=21, trendfilter=True, trendfilterperiod=200, trendfiltertype=\"EMA\", volatilityfilter=False, volatilitystdevlength=20, volatilitystdevmalength=30, name=\"pos\")", "ema_slope_trend_follower", ""),
	("PD_Technique.three_line_strategy(df, fast_length=5, mid_length=10, long_length=20, name=\"pos\")", "three_line_strategy", ""),
	("PD_Technique.roc_strategy(df, n=65, name=\"pos\")", "roc_strategy", ""),
	("PD_Technique.regression_strategy(df, n=20, name=\"pos\")", "regression_strategy", ""),
	("PD_Technique.four_week_strategy(df, n=30, name=\"pos\")", "four_week_strategy", ""),
	("PD_Technique.kingkeltner_strategy(df, n=30, name=\"pos\")", "kingkeltner_strategy", ""),
	("PD_Technique.ema_rsi_strategy(df, fast_length=5, slow_length=20, rsi_length=7, rsi_buy_value=70,rsi_sell_value=30, name=\"pos\")", "ema_rsi_strategy", ""),
]



