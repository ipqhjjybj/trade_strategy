# coding=utf-8

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from tumbler.function.technique import PD_Technique, MultiIndexMethod
from tumbler.function import is_arr_sorted
from tumbler.function import parse_maxint_from_str

from .basic_func import numpy_standardize, numpy_winsorize
from .strategy_module import FactorModel


class AlphaEstimate(object):
    @staticmethod
    def estimate_rank_alpha(df, func_name, show_figure=False):
        rank_name = "rank_" + func_name
        rate_name = "rate_1"

        df = PD_Technique.rate(df, 1, field="close", name=rate_name)
        # 删掉每个的前30行
        df = MultiIndexMethod.get_multi_index_drop_nums(df, 30)

        # 排序
        df = MultiIndexMethod.get_multi_index_rank_by_key2(df, func_name, reverse=False)

        model_df = FactorModel.buy_sell_model1(df, 3, 7, 3, 7, rank_name, rate_name, 0.001)
        model_df.drop(model_df.index[-1], inplace=True)

        if show_figure:
            model_df["tot_income"].plot()
            plt.show()

        res_df = MultiIndexMethod.get_rank_ave_score(df, rank_colume_name=rank_name,
                                                     rank_rate_name=rate_name, denominator=4)
        res_df.drop(res_df.index[-1], inplace=True)
        mean_arr = [res_df[col].mean() for col in res_df.columns]
        flag_obvious = is_arr_sorted(mean_arr)

        return flag_obvious

    @staticmethod
    def estimate_time_alpha(df, code, colume_name):
        max_int = parse_maxint_from_str(code)
        max_int = max(max_int, 30)

        tdf = MultiIndexMethod.get_multi_index_drop_nums(df, max_int)

        tdf["pos"] = tdf[colume_name].apply(lambda x: -1.0 if x < -1 else (1.0 if x > 1 else 0.0))
        tdf = PD_Technique.quick_income_compute(tdf, sllippage=0, rate=0.001,
                                                size=1, name="income", name_rate="income_rate", pos_name="pos")
        score = tdf["income_rate"][-1]
        sharpe, trade_times = PD_Technique.assume_strategy(tdf)

        return score, sharpe, trade_times


class AssumeClass(object):
    def __init__(self):
        self.assume_pairs = []

    def add_pair(self, symbol, period, path):
        self.assume_pairs.append((symbol, period, path))

    @staticmethod
    def transfer_code(code):
        code = code.replace("returns", 'df["returns"]')
        code = code.replace("volume", 'df["volume"]')
        code = code.replace("close", 'df["close"]')
        code = code.replace("open", 'df["open"]')
        code = code.replace("high", 'df["high"]')
        code = code.replace("low", 'df["low"]')
        code = code.replace("vadd", 'np.add')
        code = code.replace("vsub", 'np.subtract')
        code = code.replace("vmul", 'np.multiply')
        code = code.replace("vdiv", 'np.divide')
        code = code.replace("vneg", 'np.negative')
        code = code.replace("vcos", 'np.cos')
        code = code.replace("vsin", 'np.sin')
        return "numpy_standardize(numpy_winsorize({}))".format(code)

    def assume_code(self, code):
        code = self.transfer_code(code)
        rd = {}
        for symbol, period, path in self.assume_pairs:
            df = pd.read_csv(path, index_col=[0, 2], skipinitialspace=True)
            df["returns"] = df['close'] / df['close'].shift(1) - 1
            df["res"] = eval(code)

            score, sharpe, trade_times = AlphaEstimate.estimate_time_alpha(df, code, "res")
            rd[symbol] = {"score": score, "sharpe": sharpe, "trade_times": trade_times}
        return rd
