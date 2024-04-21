# encoding: UTF-8

import pandas as pd
import numpy as np
from scipy import optimize
from collections import defaultdict
from datetime import datetime

from math import fabs
from math import sqrt
from copy import copy

import talib

from tumbler.function.bar import *
from tumbler.constant import Direction, EvalType

'''
多因子资料 ，讲解 alpha101
http://www.qianshancapital.com/h-nd-329.html
'''


class IncStdAvg(object):
    '''
    # https://blog.csdn.net/ssmixi/article/details/104927111
    增量计算海量数据平均值和标准差,方差
    1.数据
    obj.avg为平均值
    obj.std为标准差
    obj.n为数据个数
    对象初始化时需要指定历史平均值,历史标准差和历史数据个数(初始数据集为空则可不填写)
    2.方法
    obj.incre_in_list()方法传入一个待计算的数据list,进行增量计算,获得新的avg,std和n(海量数据请循环使用该方法)
    obj.incre_in_value()方法传入一个待计算的新数据,进行增量计算,获得新的avg,std和n(海量数据请将每个新参数循环带入该方法)

    c = IncStdAvg()
    c.incre_in_value(1)
    '''

    def __init__(self, h_avg=0, h_std=0, n=0):
        self.avg = h_avg
        self.std = h_std
        self.n = n

    def incre_in_list(self, new_list):
        avg_new = np.mean(new_list)
        incre_avg = (self.n * self.avg + len(new_list) * avg_new) / \
                    (self.n + len(new_list))
        std_new = np.std(new_list, ddof=1)
        incre_std = np.sqrt((self.n * (self.std ** 2 + (incre_avg - self.avg) ** 2) + len(new_list)
                             * (std_new ** 2 + (incre_avg - avg_new) ** 2)) / (self.n + len(new_list)))
        self.avg = incre_avg
        self.std = incre_std
        self.n += len(new_list)
        return self.avg, self.std, self.n

    def incre_in_value(self, value):
        incre_avg = (self.n * self.avg + value) / (self.n + 1)
        incre_std = np.sqrt((self.n * (self.std ** 2 + (incre_avg - self.avg)
                                       ** 2) + (incre_avg - value) ** 2) / (self.n + 1))
        self.avg = incre_avg
        self.std = incre_std
        self.n += 1
        return self.avg, self.std, self.n


class DFMethod(object):
    @staticmethod
    def split_data(df, train_start_time, train_end_time, valid_start_time,
                   valid_end_time, test_start_time, test_end_time):
        train_df = df[df.datetime >= str(train_start_time)]
        train_df = train_df[train_df.datetime <= str(train_end_time)]
        valid_df = df[df.datetime >= str(valid_start_time)]
        valid_df = valid_df[valid_df.datetime <= str(valid_end_time)]
        test_df = df[df.datetime >= str(test_start_time)]
        test_df = test_df[test_df.datetime <= str(test_end_time)]
        return train_df, valid_df, test_df

    @staticmethod
    def get_features_label(df, label_name="rate_1"):
        df = df.drop(labels=["symbol", "datetime", "open", "high", "low", "close", "volume"], axis=1)
        features = df.drop(labels=[label_name], axis=1)
        label = df[label_name]
        return features, label

    @staticmethod
    def count(series, n):
        '''
        统计最近n 行中 , > 0 的出现次数
        '''
        if n <= 0:
            return pd.Series([])
        ret = []
        num = 0
        for i in range(len(series)):
            if i >= n:
                num -= series[i - n] > 0
            num += series[i] > 0
            ret.append(num)
        return pd.Series(ret)

    @staticmethod
    def prod(series, n):
        '''
        序列最近n个数的累乘
        '''
        if n <= 0:
            return pd.Series([])
        ret = []
        num = 1
        for i in range(len(series)):
            if i >= n:
                num = num / series[i - n]
            num *= series[i]
            ret.append(num)
        return pd.Series(ret)

    @staticmethod
    def rank(series, n):
        '''
        滚动求出，每个数字在最近最近n个数中排名第几
        '''
        if n <= 0:
            return pd.Series([])
        nums = []
        ret = []
        for i in range(len(series)):
            if i >= n:
                nums.remove(series[i - n])
            val = series[i]
            index = len(nums)
            nums.append(val)
            j = index - 1
            while j >= 0 and nums[j] > nums[j + 1]:
                k = nums[j]
                nums[j] = nums[j + 1]
                nums[j + 1] = k
                j = j - 1
                index = j + 1
            ret.append(index + 1)
        return pd.Series(ret)

    @staticmethod
    def question_mark(condition_series, true_series, false_series):
        ret = []
        for i in range(len(condition_series)):
            if condition_series[i]:
                ret.append(true_series[i])
            else:
                ret.append(false_series[i])
        return pd.Series(ret)

    @staticmethod
    def sum_if(series, n, condition_series):
        '''
        滚动求出，最近n个数字中，满足条件的累计
        '''
        if n <= 0:
            return pd.Series([])
        ret = []
        num = 0
        for i in range(len(condition_series)):
            if i >= n:
                if condition_series[i - n]:
                    num -= series[i - n]
            if condition_series[i]:
                num += series[i]
            ret.append(num)
        return pd.Series(ret)

    @staticmethod
    def corr(series_a, series_b, n):
        if n <= 0:
            return pd.Series([])
        ret = []
        va = []
        vb = []
        for i in range(len(series_a)):
            if i >= n:
                va.pop(0)
                vb.pop(0)
            va.append(series_a[i])
            vb.append(series_b[i])
            val = np.mean(np.multiply((va - np.mean(va)), (vb - np.mean(vb)))) / (np.std(vb) * np.std(va))
            ret.append(val)
        return pd.Series(ret)

    @staticmethod
    def coviance(series_a, series_b, n):
        '''
        序列A,B 过去n天的协方差
        '''
        if n <= 0:
            return pd.Series([])
        ret = []
        va = []
        vb = []
        for i in range(len(series_a)):
            if i >= n:
                va.pop(0)
                vb.pop(0)
            va.append(series_a[i])
            vb.append(series_b[i])
            val = np.cov(va, vb)[0, 1]
            ret.append(val)
        return pd.Series(ret)

    # @staticmethod
    # def decay_linear(series, n):
    #     result = pd.DataFrame(np.nan, index=data.index, columns=data.columns)
    #
    #     weight = np.arange(n) + 1.
    #     weight = weight / weight.sum()
    #     for i in range(n, data.shape[0]):
    #         t = data.index[i]
    #         result.ix[t, :] = data[i - window:i].T.dot(weight)
    #     return result


class MultiIndexMethod(object):
    @staticmethod
    def get_df_filter_symbols_arr(df, arr):
        '''
        实现 return df[df[symbol] in [arr]] 功能
        '''
        to_delete_arr = []
        rows = df.index
        for row in rows:
            coin = row[0]
            if coin not in arr:
                to_delete_arr.append(row)
        return df.drop(to_delete_arr)

    @staticmethod
    def get_filter_symbols(df, min_num_rows=365):
        '''
        过滤掉df 中, 行数少于 365 的票
        '''
        coin_dict = defaultdict(list)
        rows = df.index
        for row in rows:
            coin = row[0]
            coin_dict[coin].append(row)

        to_delete_arr = []
        for coin, values in coin_dict.items():
            if len(values) < min_num_rows:
                to_delete_arr.extend(values)
        return df.drop(to_delete_arr)

    @staticmethod
    def get_multi_index_drop_nums(df, drop_first_nums=1):
        '''
        删去多元索引中，按照第一个索引的前几行
        '''
        rows = df.index
        ret = []
        start_num = 0
        pre_key = ""
        for row in rows:
            key = row[0]
            if key != pre_key:
                start_num = 0
            if start_num < drop_first_nums:
                ret.append(row)
                start_num += 1
            pre_key = key
        return df.drop(ret)

    @staticmethod
    def get_multi_index_rank_by_key2(df, colume_name, rank_colume_name="", reverse=True):
        '''
        计算多元索引中的，按照 第二列 groupby 的rank排序
        '''
        if not rank_colume_name:
            rank_colume_name = "rank_{}".format(colume_name)
        k_dic = defaultdict(list)
        rows = df.index
        values = df[colume_name]
        for i in range(len(rows)):
            _, key = rows[i]
            val = values[i]
            k_dic[key].append((val, i))

        ret = []
        for _, arr in k_dic.items():
            arr.sort(reverse=reverse)
            for i in range(len(arr)):
                _, index = arr[i]
                ret.append((index, i + 1))

        ret.sort()
        ret = [x[1] for x in ret]
        df[rank_colume_name] = np.array(ret)
        return df

    @staticmethod
    def get_rank_ave_score(df, rank_colume_name="", rank_rate_name="", denominator=3):
        '''
        rank_colume_name, 排名的那列
        rank_rate_name, 未来一天的涨跌幅
        对于 denominator = 3
        通过排名， 按照 排名前1/3 放第一组， 中间的放第二组， 剩余的放第三组
        '''
        all_stocks = set([])
        k_dic = defaultdict(list)
        rows = df.index
        rank_colume_values = df[rank_colume_name]
        rank_rate_values = df[rank_rate_name]
        for i in range(len(rows)):
            stock, key = rows[i]
            rank = rank_colume_values[i]
            rate = rank_rate_values[i]

            k_dic[key].append((stock, int(rank), rate))
            all_stocks.add(stock)

        len_stocks = len(all_stocks)
        index_arr = []
        result = {}
        for i in range(denominator):
            result[i] = []

        keys = list(k_dic.keys())
        keys.sort()
        for key in keys:
            arr = k_dic[key]
            ll = len(arr)
            if ll < len_stocks:
                continue

            if ll % denominator == 0:
                group_num = int(ll / denominator)
            else:
                group_num = int(ll / denominator) + 1

            res = [(0, 0)] * denominator
            for stock, rank, rate in arr:
                ind = int((rank - 1) / group_num)
                pre_val, pre_num = res[int((rank - 1) / group_num)]
                pre_val += rate
                pre_num += 1
                res[ind] = (pre_val, pre_num)

            for i in range(denominator):
                val, num = res[i]
                if num > 0:
                    result[i].append(val * 1.0 / num)
                else:
                    result[i].append(0)

            index_arr.append(key)

        return pd.DataFrame(result, index=index_arr)


class AlphaManager(object):
    '''
    guotai , gtja --> 国泰君安因子
    '''

    @staticmethod
    def roc_001(df, n=30, name="roc_001"):
        df[name] = (df["close"] - df["close"].shift(n)) / df["close"]
        return df

    @staticmethod
    def guotai_002(df, name="gtja_002"):
        '''
        0.00038574033678748985
        0.001587436145166893
        0.0017855712838669472
        0.0023019808975009754
        '''

        df[name] = (((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"])).shift(1) * -1
        return df

    @staticmethod
    def guotai_014(df, name="gtja_014"):
        '''
        0.0017225357906924552
        0.0017969255305477602
        0.0013445084325328265
        0.0011967589095492632
        '''
        df[name] = (df["close"] - df["close"].shift(5))
        return df

    @staticmethod
    def guotai_015(df, name="gtja_015"):
        '''
        0.0006210342509323341
        0.0017767200431171755
        0.0022235545802726
        0.001439419789000196
        '''
        df[name] = (df["open"] / df["close"].shift(1)) - 1
        return df

    @staticmethod
    def guotai_018(df, name="gtja_018"):
        '''
        0.003078843077781962
        0.001169664654799442
        0.0014143665116997947
        0.0003978544190411067
        '''
        df[name] = df["close"] / df["close"].shift(5)
        return df

    @staticmethod
    def guotai_020(df, name="gtja_020"):
        '''
        0.0036485300616642476
        0.000979174762788801
        0.0012494464781694717
        0.00018357736069978531
        '''
        df[name] = (df["close"] - df["close"].shift(6)) / df["close"].shift(6) * 100
        return df

    @staticmethod
    def guotai_024(df, name="gtja_024"):
        '''
        0.001381737698658929
        0.0017271152773609768
        0.0016044195931141734
        0.0013474560941882272
        :param df:
        :param name:
        :return:
        '''
        df[name] = (df["close"] - df["close"].shift(5)).rolling(5).mean().shift()
        return df

    @staticmethod
    def guotai_029(df, name="gtja_029"):
        '''
        0.002471212532189659
        0.0018330418465865334
        0.0010581981717935228
        0.0006982761127525911
        '''
        df[name] = (df["close"] - df["close"].shift(6)) / df["close"].shift(6) * df["volume"]
        return df

    @staticmethod
    def guotai_031(df, name="gtja_031"):
        '''
        0.0033699237669555606
        0.0010860018589064183
        0.000881732052454495
        0.0007230709850058315
        '''
        df[name] = (df["close"] - df["close"].rolling(12).mean()) / df["close"].rolling(12).mean() * 100
        return df

    @staticmethod
    def guotai_034(df, name="gtja_034"):
        '''
        0.0007230709850058315
        0.000881732052454495
        0.0010860018589064183
        0.0033699237669555606
        '''
        df[name] = df["close"].rolling(12).mean() / df["close"]
        return df


class PD_Technique(object):
    @staticmethod
    def least_squres(x, y):
        '''
        y = beta * x + alpha
        '''
        n = len(x)
        beta = (n * np.sum(x * y) - np.sum(x) * np.sum(y)) / (n * np.sum(x * x) - np.sum(x) * np.sum(x))
        alpha = np.sum(y) / n - beta * (np.sum(x) / n)
        return beta, alpha

    @staticmethod
    def least_squres_new(x, y):
        def reds(p):
            # 计算以p为参数的直线和数据之间的误差
            k, b = p
            return y - (k * x + b)

        # leastsq 使得reds()输出最小，参数的初始值是【1,0】
        r = optimize.leastsq(reds, np.array([1, 0]))
        k, b = r[0]
        return k, b

    @staticmethod
    def break_continue_sigal(va, array=False):
        n = len(va)
        bigger_num = 0
        smaller_num = 0
        b_arr = np.zeros(n)
        s_arr = np.zeros(n)
        ind_arr = np.zeros(n)
        for i in range(n):
            if va[i] > 0:
                bigger_num += 1
            elif va[i] < 0:
                smaller_num += 1
            b_arr[i] = bigger_num
            s_arr[i] = smaller_num
            ind_arr[i] = i + 1
        ret = va * (b_arr - s_arr) / ind_arr
        if array:
            return ret
        else:
            if n == 0:
                return 0
            return ret[n - 1]

    @staticmethod
    def ema_std(df, n, field="close", name=None, normalization=False):
        '''
        还未测试
        '''
        if not name:
            name = "ema_std_{}".format(n)
        df = PD_Technique.ema(df, n, field="close", name="tmp_ema", normalization=False)
        df[name] = df["tmp_ema"].rolling(window=n, center=False).std()
        if normalization:
            df[name] = df[name] / df[field]
        df = df.drop(labels=["tmp_ema"], axis=1)
        return df

    @staticmethod
    def ema(df, n, field="close", name=None, normalization=False):
        if not name:
            name = "ema_{}".format(n)
        df[name] = df[field].ewm(n).mean().shift()
        if normalization:
            df[name] = df[name] / df[field]
        return df

    @staticmethod
    def sma(df, period, field="close", name=None, normalization=False):
        if not name:
            name = "sma_{}".format(period)
        df[name] = df[field].rolling(period).mean().shift()
        if normalization:
            df[name] = df[name] / df[field]
        return df

    @staticmethod
    def rsi(df, n, field="close", name=None, normalization=False):
        if not name:
            name = "rsi_{}".format(n)
        close = df[field]
        delta = close.diff()
        delta = delta[1:]
        prices_up = delta.copy()
        prices_down = delta.copy()
        prices_up[prices_up < 0] = 0
        prices_down[prices_down > 0] = 0
        roll_up = prices_up.rolling(n).mean()
        roll_down = prices_down.abs().rolling(n).mean()
        rs = roll_up / roll_down
        df[name] = 100.0 - (100.0 / (1.0 + rs))
        if normalization:
            df[name] = df[name] / 100.0
        return df

    @staticmethod
    def macd(df, fast_length, slow_length, macd_length, field="close", name_macd=None, name_signal=None):
        if not name_macd:
            name_macd = "macd_{}_{}_{}_".format(fast_length, slow_length, macd_length)
        if not name_signal:
            name_signal = "macd_signal_{}_{}_{}_".format(fast_length, slow_length, macd_length)
        close = df[field]
        ema_12 = close.ewm(span=fast_length, min_periods=fast_length).mean()
        ema_26 = close.ewm(span=slow_length, min_periods=slow_length).mean()
        df[name_macd] = ema_12 - ema_26
        df[name_signal] = df[name_macd].ewm(span=macd_length, min_periods=macd_length).mean()
        return df

    @staticmethod
    def mtm(df, n, field="close", name=None):
        if not name:
            name = "mtm_{}".format(n)
        df[name] = df[field] - df[field].shift(n)
        return df

    @staticmethod
    def rise(df, n, field="close", name=None):
        if not name:
            name = "rise_{}".format(n)
        df[name] = (df[field] - df[field].shift(n)) / df[field].shift(n)
        return df

    @staticmethod
    def bbands(df, n, numsd=2.0, field="close", bb_ave_name=None, up_band_name=None, dn_band_name=None):
        if not bb_ave_name:
            bb_ave_name = "bb_ave_name_{}".format(n)
        if not up_band_name:
            up_band_name = "up_band_name_{}".format(n)
        if not dn_band_name:
            dn_band_name = "dn_band_name_{}".format(n)
        ave = df[field].rolling(window=n, center=False).mean()
        sd = df[field].rolling(window=n, center=False).std()
        df[bb_ave_name] = ave
        df[up_band_name] = ave + (sd * numsd)
        df[dn_band_name] = ave - (sd * numsd)
        return df

    @staticmethod
    def psar(df, iaf=0.02, maxaf=0.2):
        '''
        Calculation of Parabolic SAR
        '''
        length = len(df)
        psar = df['close'][0:len(df['Close'])]
        psarbull = [None] * length
        psarbear = [None] * length
        bull = True
        af = iaf
        ep = df['low'][0]
        hp = df['high'][0]
        lp = df['low'][0]
        for i in range(2, length):
            if bull:
                psar[i] = psar[i - 1] + af * (hp - psar[i - 1])
            else:
                psar[i] = psar[i - 1] + af * (lp - psar[i - 1])
            reverse = False
            if bull:
                if df['low'][i] < psar[i]:
                    bull = False
                    reverse = True
                    psar[i] = hp
                    lp = df['low'][i]
                    af = iaf
            else:
                if df['high'][i] > psar[i]:
                    bull = True
                    reverse = True
                    psar[i] = lp
                    hp = df['high'][i]
                    af = iaf
            if not reverse:
                if bull:
                    if df['high'][i] > hp:
                        hp = df['high'][i]
                        af = min(af + iaf, maxaf)
                    if df['low'][i - 1] < psar[i]:
                        psar[i] = df['low'][i - 1]
                    if df['low'][i - 2] < psar[i]:
                        psar[i] = df['low'][i - 2]
                else:
                    if df['low'][i] < lp:
                        lp = df['low'][i]
                        af = min(af + iaf, maxaf)
                    if df['high'][i - 1] > psar[i]:
                        psar[i] = df['high'][i - 1]
                    if df['high'][i - 2] > psar[i]:
                        psar[i] = df['high'][i - 2]
            if bull:
                psarbull[i] = psar[i]
            else:
                psarbear[i] = psar[i]
        # return {"dates":dates, "high":high, "low":low, "close":close, "psar":psar, "psarbear":psarbear, "psarbull":psarbull}
        # return psar, psarbear, psarbull
        df['psar'] = psar
        # df['psarbear'] = psarbear
        # df['psarbull'] = psarbull
        return df

    @staticmethod
    def roc(df, n, field="close", name=None):
        if not name:
            name = "roc_{}".format(n)
        df[name] = (df[field] - df[field].shift(n)) / df[field].shift(n)
        return df

    @staticmethod
    def droc(df, n, field='close', name=None):
        '''
        实现新的涨幅，去掉一个最大值，去掉一个最小值，然后累加涨幅
        '''
        if not name:
            name = "droc_{}".format(n)
        df["tmp"] = (df[field] - df[field].shift(1)) / df[field].shift(1)
        df[name] = df["tmp"].rolling(n).sum() - df["tmp"].rolling(n).min() - df["tmp"].rolling(n).max()
        df = df.drop(labels=["tmp"], axis=1)
        return df

    @staticmethod
    def cci(df, n, constant=0.015, name=None):
        if not name:
            name = "cci_{}".format(n)
        tp = (df['high'] + df['low'] + df['close']) / 3
        df[name] = pd.Series((tp - tp.rolling(window=n, center=False).mean())
                             / (constant * tp.rolling(window=n, center=False).std()))
        return df

    @staticmethod
    def kelch(df, n, name_ch_m=None, name_ch_u=None, name_ch_d=None):
        '''
        Calcualtion of Keltner Channels
        '''
        if not name_ch_m:
            name_ch_m = "kel_ch_m_{}".format(n)
        if not name_ch_u:
            name_ch_u = "kel_ch_u_{}".format(n)
        if not name_ch_d:
            name_ch_d = "kel_ch_d_{}".format(n)
        df[name_ch_m] = pd.Series(((df['high'] + df['low'] + df['close'])
                                   / 3).rolling(window=n, center=False).mean(), name=name_ch_m)
        df[name_ch_u] = pd.Series(((4 * df['high'] - 2 * df['low'] + df['close'])
                                   / 3).rolling(window=n, center=False).mean(), name=name_ch_u)
        df[name_ch_d] = pd.Series(((-2 * df['high'] + 4 * df['low'] + df['close'])
                                   / 3).rolling(window=n, center=False).mean(), name=name_ch_d)
        return df

    @staticmethod
    def tema(df, n, field="close", name=None):
        '''
        Calculation of Triple Exponential Moving Average
        '''
        if not name:
            name = "tema_{}".format(n)
        ema = df[field].ewm(span=3, min_periods=0, adjust=True, ignore_na=False).mean()
        df[name] = (3 * ema - 3 * ema * ema) + (ema * ema * ema)
        return df

    @staticmethod
    def atr(df, n, name_tr=None, name_atr=None, name_natr=None):
        if not name_tr:
            name_tr = "tr_{}".format(n)
        if not name_atr:
            name_atr = "atr_{}".format(n)
        if not name_natr:
            name_natr = "natr_{}".format(n)
        df["hl"] = df["high"] - df["low"]
        df["abs_hc"] = df["high"] - df["close"].shift(1)
        df["abs_lc"] = df["low"] - df["close"].shift(1)
        df[name_tr] = df[["hl", "abs_hc", "abs_lc"]].max(axis=1)
        df[name_atr] = df[name_tr].rolling(window=n).mean()
        df[name_natr] = df[name_tr] / df["close"]
        df = df.drop(labels=["hl", "abs_hc", "abs_lc"], axis=1)
        return df

    @staticmethod
    def dmi(df, n, name_tr=None, name_atr=None, name_natr=None, name_plus_di=None, name_minus_di=None,
            name_adx_di=None):
        if not name_tr:
            name_tr = "tr_{}".format(n)
        if not name_atr:
            name_atr = "atr_{}".format(n)
        if not name_natr:
            name_natr = "natr_{}".format(n)
        if not name_plus_di:
            name_plus_di = "plus_di_{}".format(n)
        if not name_minus_di:
            name_minus_di = "minus_di_{}".format(n)
        if not name_adx_di:
            name_adx_di = "adx_di_{}".format(n)

        df = PD_Technique.atr(df, n, name_tr, name_atr, name_natr)
        df['up_move'] = df['high'] - df['high'].shift(1)
        df['down_move'] = df['low'].shift(1) - df['low']
        df['zero'] = 0

        df['plus_dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > df['zero']), df['up_move'], 0)
        df['minus_dm'] = np.where((df['up_move'] < df['down_move']) & (df['down_move'] > df['zero']), df['down_move'],
                                  0)

        df[name_plus_di] = 100 * (df['plus_dm'] / df[name_atr]).ewm(span=n, min_periods=0, adjust=True,
                                                                    ignore_na=False).mean()
        df[name_minus_di] = 100 * (df['minus_dm'] / df[name_atr]).ewm(span=n, min_periods=0, adjust=True,
                                                                      ignore_na=False).mean()

        df[name_adx_di] = 100 * (abs((df[name_plus_di] - df[name_minus_di])
                                     / (df[name_plus_di] + df[name_minus_di]))).ewm(span=n,
                                                                                    min_periods=0, adjust=True,
                                                                                    ignore_na=False).mean()
        df = df.drop(labels=["up_move", "down_move", "zero", "plus_dm", "minus_dm"], axis=1)
        return df

    @staticmethod
    def mfi(df, n, name=None):
        '''
        Calculation of Money Flow Index
        '''
        if not name:
            name = "mfi_{}".format(n)
        df["tp"] = (df["high"] + df["low"] + df["close"]) / 3
        df["rmf"] = df["tp"] + df["volume"]
        df["pmf"] = np.where(df['tp'] > df['tp'].shift(1), df['tp'], 0)
        df['nmf'] = np.where(df['tp'] < df['tp'].shift(1), df['tp'], 0)

        # money flow ratio
        df['mfr'] = df['pmf'].rolling(window=n, center=False).sum() / df['nmf'].rolling(window=n, center=False).sum()
        df[name] = 100 - 100 / (1 + df['mfr'])
        df = df.drop(labels=["tp", "rmf", "pmf", "nmf", "mfr"], axis=1)
        return df

    @staticmethod
    def wr(df, n, name=None):
        '''
        Calculation of William %R
        '''
        if not name:
            name = "wr_{}".format(n)
        highest_high = df['high'].rolling(window=n, center=False).max()
        lowest_low = df['low'].rolling(window=n, center=False).min()
        df[name] = (-100) * ((highest_high - df['close']) / (highest_high - lowest_low))
        return df

    @staticmethod
    def rate(df, n, field="close", name=None):
        if not name:
            name = "rate_{}".format(n)
        df[name] = df[field].shift(-n) / df[field] - 1
        return df

    @staticmethod
    def rate_target(df, p=0.5, n=10, field="close", name_target="target"):
        name_rate = "tmp_rate_{}".format(n)
        PD_Technique.rate(df, n, field=field, name=name_rate)
        df[name_target] = df[name_rate].apply(lambda x: 1 if x >= p else (-1 if x <= -p else 0))
        # df[name_target] = df[name_rate].apply(lambda x: round(x/p))
        df = df.drop(labels=[name_rate], axis=1)
        return df

    @staticmethod
    def zigzag_strategy(df, retrace_pct=2, name=None):
        '''
        zigzag 策略
        折返超过2%，认为是新的 关键点，关键点随新高新低更新
        '''
        if not name:
            name = "zigzag_pos_{}".format(retrace_pct)
        close_arr = list(df["close"])
        pos_ret = []
        swing_price_ret = []
        pos = 0
        swing_price = -1
        up_dn = 0
        if len(close_arr) > 0:
            swing_price = close_arr[0]
        for i in range(len(close_arr)):
            swing_high_price = -1
            swing_low_price = -1
            if i >= 1:
                if i == 1:
                    if close_arr[i - 1] >= close_arr[i]:
                        swing_high_price = close_arr[i - 1]
                    if close_arr[i - 1] <= close_arr[i]:
                        swing_low_price = close_arr[i - 1]
                else:
                    if close_arr[i - 1] > close_arr[i] and close_arr[i - 1] >= close_arr[i - 2]:
                        swing_high_price = close_arr[i - 1]
                    if close_arr[i - 1] < close_arr[i] and close_arr[i - 1] <= close_arr[i - 2]:
                        swing_low_price = close_arr[i - 1]

                save_swing_flag = False
                if swing_high_price != -1:
                    if up_dn <= 0 and swing_high_price >= swing_price * (1 + retrace_pct / 100.0):
                        up_dn = 1
                        save_swing_flag = True
                    elif up_dn == 1 and swing_high_price >= swing_price:
                        save_swing_flag = True
                    if save_swing_flag:
                        swing_price = swing_high_price
                elif swing_low_price != -1:
                    if up_dn >= 0 and swing_low_price <= swing_price * (1 - retrace_pct / 100.0):
                        up_dn = -1
                        save_swing_flag = True
                    elif up_dn == -1 and swing_low_price <= swing_price:
                        save_swing_flag = True
                    if save_swing_flag:
                        swing_price = swing_low_price

            swing_price_ret.append(swing_price)
            if i >= 2:
                if pos != 1 and swing_price_ret[i - 2] < swing_price_ret[i - 1] and swing_low_price != -1:
                    pos = 1
                elif pos != -1 and swing_price_ret[i - 2] > swing_price_ret[i - 1] and swing_high_price != -1:
                    pos = -1
            pos_ret.append(pos)
        df[name] = pos_ret
        return df

    @staticmethod
    def osc_strategy(df, fast_length=5, slow_length=20, ma_osc=20, name=None):
        '''
        很一般的策略
        '''
        if not name:
            name = "osc_pos_{}_{}".format(fast_length, slow_length)

        # PD_Technique.ema(df, fast_length, name="ema_f")
        # PD_Technique.ema(df, slow_length, name="ema_s")
        # df["osc"] = df["ema_f"] - df["ema_s"]
        df["ma_f"] = df["close"].rolling(window=fast_length, center=False).mean()
        df["ma_s"] = df["close"].rolling(window=slow_length, center=False).mean()
        df["osc"] = df["ma_f"] - df["ma_s"]
        df["osc_ma"] = df["osc"].rolling(window=ma_osc, center=False).mean()
        df["val"] = df["osc"] - df["osc_ma"]
        df[name] = df["val"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(labels=["ma_f", "ma_s", "val", "osc_ma", "osc"], axis=1)
        # df = df.drop(labels=["ema_f", "ema_s", "val", "osc_ma", "osc"], axis=1)
        return df

    @staticmethod
    def trix_strategy(df, n=20, ma_length=3, name=None):
        '''
        btc (20,3) 15000 比较一般的策略
        eth 600
        trix 策略， trix 穿透 trix_ma操作
        '''
        if not name:
            name = "trix_pos_{}_{}".format(n, ma_length)
        df["trix"] = talib.TRIX(df.close, timeperiod=n)
        df["trix_ma"] = df["trix"].rolling(window=ma_length, center=False).mean()
        df["val"] = df["trix"] - df["trix_ma"]
        df[name] = df["val"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(labels=["trix", "trix_ma", "val"], axis=1)
        return df

    @staticmethod
    def regression_strategy(df, n=10, name=None):
        '''
        感觉挺一般，不建议用吧,  ,, 这个策略为什么ETH表现这么好?
        n = 20  btc:16000, eth:1700
        回归策略，斜率>0做多，斜率<0做空
        '''
        if not name:
            name = "reg_pos_{}".format(n)
        reg_arr = []
        x = np.array(range(1, n + 1))
        y = np.array([])
        close_arr = list(df["close"])
        ll = len(close_arr)
        for i in range(ll):
            y = np.append(y, close_arr[i])
            if i >= n:
                y = np.delete(y, 0)
                k, b = PD_Technique.least_squres(x, y)
                reg_arr.append(k)
            else:
                reg_arr.append(0)
        df["reg"] = np.array(reg_arr)
        df[name] = df["reg"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["reg"], axis=1)
        return df

    @staticmethod
    def reg_boll_strategy(df, n=10, offset=1.96, name=None):
        '''
        btc:-15000 以上
        eth:-600 以上
        ltc: 盈利100
        n = 20, offset=1
        感觉，，这是个非常反向的策略，，为啥呢。。。
        斜率 + 标准差过滤
        '''
        if not name:
            name = "reg_boll_pos_{}".format(n)
        close_arr = list(df["close"])
        df["dev"] = df["close"].rolling(window=n, center=False).std()
        x = np.array(range(1, n + 1))
        y = np.array([])
        ll = len(close_arr)
        k_arr = []
        b_arr = []
        for i in range(ll):
            y = np.append(y, close_arr[i])
            if i >= n:
                y = np.delete(y, 0)
                k, b = PD_Technique.least_squres(x, y)
                k_arr.append(k)
                b_arr.append(b)
            else:
                k_arr.append(0)
                b_arr.append(0)

        df["k"] = np.array(k_arr)
        df["b"] = np.array(b_arr)
        df["up_line"] = df["k"] * df["close"] + df["b"] + df["dev"] * offset
        df["down_line"] = df["k"] * df["close"] + df["b"] - df["dev"] * offset
        df[name] = (df["close"] - df["up_line"]).apply(lambda x: 1 if x > 0 else 0) \
                   + (df["close"] - df["down_line"]).apply(lambda x: -1 if x < 0 else 0)
        #df[name][df[name] == 0] = np.NAN
        #df.loc[df[name] == 0][name] = np.NAN
        #df[name] = df[name].fillna(method='ffill')
        #df[name] = df[name].ffill()

        df = df.drop(["dev", "k", "b", "up_line", "down_line"], axis=1)
        return df

    @staticmethod
    def mtm_strategy(df, n=30, name=None):
        '''
        mtm 策略

        '''
        if not name:
            name = "mtm_pos_{}".format(n)

        df = PD_Technique.mtm(df, n, name="mtm_tmp")
        df[name] = df["mtm_tmp"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["mtm_tmp"], axis=1)
        return df

    @staticmethod
    def roc_strategy(df, n=5, name=None):
        '''
        roc 策略
        n = 30 表现不错
        btc: 19000, eth:1500, ltc:亏钱
        '''
        if not name:
            name = "roc_pos_{}".format(n)

        df = PD_Technique.roc(df, n, name="roc_tmp")
        df[name] = df["roc_tmp"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["roc_tmp"], axis=1)
        return df

    @staticmethod
    def macd_strategy(df, fast_length, slow_length, macd_length, name=None):
        '''
        macd 策略， 好像周期越长，效果越好。。 或者周期非常短，看绩效也是正的
        表现很一般, btc: 10000, eth:700, ltc: 110
        '''
        if not name:
            name = "macd_pos_{}_{}_{}".format(fast_length, slow_length, macd_length)

        df = PD_Technique.macd(df, fast_length, slow_length, macd_length,
                               name_macd="name_macd", name_signal="name_signal")
        df["tmp_result"] = df["name_macd"] - df["name_signal"]
        df[name] = df["tmp_result"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["name_macd", "name_signal", "tmp_result"], axis=1)
        return df

    @staticmethod
    def macd_ma_strategy(df, fast_length=5, slow_length=26, macd_length=9, ma_length=144, name=None):
        '''
        macd 均线策略
        '''
        if not name:
            name = "macd_ma_pos_{}_{}_{}".format(fast_length, slow_length, macd_length, macd_length)

        df = PD_Technique.macd(df, fast_length, slow_length, macd_length,
                               name_macd="name_macd", name_signal="name_signal")
        df["tmp_result"] = df["name_macd"] - df["name_signal"]
        df["pos_macd"] = df["tmp_result"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        # sma(df, period, field="close", name=None, normalization=False)
        df = PD_Technique.sma(df, ma_length, name="tmp_result")
        df["pos_ma"] = df["tmp_result"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df["tmp_result"] = df["pos_ma"] + df["pos_macd"]
        df[name] = df["tmp_result"].apply(lambda x: -1.0 if x < -1.5 else (1.0 if x > 1.5 else 0.0))
        df = df.drop(["name_macd", "name_signal", "tmp_result", "pos_macd", "pos_ma"], axis=1)
        return df

    @staticmethod
    def one_line_strategy(df, n=5, name=None):
        '''
        这个其实还是不错的
        n = 20
        单EMA 策略
        btc:20000
        eth:1000
        ltc:亏钱
        '''
        if not name:
            name = "line_pos_{}".format(n)
        name_ema = "ema_{}".format(n)
        PD_Technique.ema(df, n, name=name_ema)
        df["tmp_ema"] = df[name_ema].shift(1)
        df["tmp_result"] = df[name_ema] - df["tmp_ema"]
        df[name] = df["tmp_result"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["tmp_ema", "tmp_result", name_ema], axis=1)
        return df

    @staticmethod
    def three_line_strategy(df, fast_length=5, mid_length=10, long_length=20, name=None):
        '''
        三均线策略 ，也即多头排列
        btc:25000 效果很好, eth:1100, ltc:略有盈利
        fast_length=5, mid_length=10, long_length=20
        '''
        if not name:
            name = "three_line_pos_{}_{}_{}".format(fast_length, mid_length, long_length)
        name_short_ema = "ema_{}".format(fast_length)
        name_mid_ema = "ema_{}".format(mid_length)
        name_long_ema = "ema_{}".format(long_length)
        PD_Technique.ema(df, fast_length, name=name_short_ema)
        PD_Technique.ema(df, mid_length, name=name_mid_ema)
        PD_Technique.ema(df, long_length, name=name_long_ema)
        df["t1"] = df[name_short_ema] - df[name_mid_ema]
        df["t2"] = df[name_mid_ema] - df[name_long_ema]
        df["g1"] = df["t1"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df["g2"] = df["t2"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df["g"] = df["g1"] + df["g2"]
        df[name] = df["g"].apply(lambda x: -1.0 if x < -1.5 else (1.0 if x > 1.5 else 0.0))
        df = df.drop(["t1", "t2", "g1", "g2", "g", name_short_ema, name_mid_ema, name_long_ema], axis=1)
        return df

    @staticmethod
    def add_line_strategy(df, fast_length=5, mid_length=10, long_length=20, name=None):
        '''
        三个不同参数计算的EMA，按照东西加权得到
        btc: 20000, eth:1000, ltc:亏钱
        '''
        if not name:
            name = "add_line_pos_{}_{}_{}".format(fast_length, mid_length, long_length)
        name_short_ema = "ema_{}".format(fast_length)
        name_mid_ema = "ema_{}".format(mid_length)
        name_long_ema = "ema_{}".format(long_length)
        PD_Technique.ema(df, fast_length, name=name_short_ema)
        PD_Technique.ema(df, mid_length, name=name_mid_ema)
        PD_Technique.ema(df, long_length, name=name_long_ema)
        df["val"] = (df[name_short_ema] + df[name_mid_ema] + df[name_long_ema]) / 3
        df["minus"] = df["val"] - df["val"].shift(1)
        df[name] = df["minus"].apply(lambda x: -1.0 if x < 0 else (1.0 if x > 0 else 0.0))
        df = df.drop(["val", "minus", name_short_ema, name_mid_ema, name_long_ema], axis=1)
        return df

    @staticmethod
    def ema_add_percent(df, fast_length=5, slow_length=60, b_rate=0.1, name=None):
        '''
        入场信号如来后加一定百分比入场
        '''
        if not name:
            name = f"ema_add_{fast_length}_{slow_length}_{b_rate}"
        fast_length = 5
        slow_length = 60
        b_rate = 0.01
        func_arr = [
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"gt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 + {b_rate}))",
                f"reverse(gt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 + {b_rate})))",
                Direction.LONG.value,
                1
            ],
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"lt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 - {b_rate}))",
                f"reverse(lt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 - {b_rate})))",
                Direction.SHORT.value,
                1
            ],
        ]
        df = PD_Technique.eval(df, func_arr, name=name)
        return df

    @staticmethod
    def ma_close_strategy(df, fast_length=10, slow_length=30, name=None):
        '''
        tb 案例1的例子，测试下策略
        '''
        if not name:
            name = "ma_close_{}_{}".format(fast_length, slow_length)
        func_arr = [
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"and(close > ma(close, {slow_length}), every(close >= ma(close, {fast_length}), 4))",
                f"every(close < ma(close, {slow_length}), 4)",
                Direction.LONG.value,
                1
            ],
            # [
            #     EvalType.MANY_CONDITIONS_FUNC.value,
            #     "and(close < ma(close, 30), every(close < ma(close, 10), 4))",
            #     "every(close >= ma(close, 10), 4)",
            #     Direction.SHORT.value,
            #     1
            # ]
        ]

        # func_arr = [
        #     [
        #         EvalType.MANY_CONDITIONS_FUNC.value,
        #         "every(close >= ma(close, 10), 4)",
        #         "every(close < ma(close, 10), 4)",
        #         Direction.LONG.value,
        #         1
        #     ],
        #     [
        #         EvalType.MANY_CONDITIONS_FUNC.value,
        #         "every(close < ma(close, 10), 4)",
        #         "every(close >= ma(close, 10), 4)",
        #         Direction.SHORT.value,
        #         1
        #     ]
        # ]
        df = PD_Technique.eval(df, func_arr, name="pos")
        return df

    @staticmethod
    def kdj_strategy(df, n=60, m1=3, m2=3, open_v=80, end_v=20, name=None):
        if not name:
            name = "kdj_pos_{}_{}_{}".format(n, m1, m2)
        open = df["open"]
        high = df["high"]
        low = df["low"]
        close = df["close"]

        rsv = (close - llv(low, n)) / (hhv(high, n) - llv(low, n)) * 100
        k = sma(rsv, m1)
        d = sma(k, m2)
        j = 3 * k - 2 * d
        df[name] = j.apply(lambda x: -1.0 if x < end_v else (1.0 if x > open_v else 0.0))
        # k = ma(rsv, n)
        # df[name] = k.apply(lambda x: -1.0 if x < end_v else (1.0 if x > open_v else 0.0))
        return df

    @staticmethod
    def ema_strategy(df, fast_length=5, slow_length=20, name=None):
        '''
        双EMA 策略
        这个很好, btc:18000, eth:1100, ltc:略有盈利
        '''
        if not name:
            name = "ema_pos_{}_{}".format(fast_length, slow_length)
        name_short_ema = "ema_{}".format(fast_length)
        name_long_ema = "ema_{}".format(slow_length)
        PD_Technique.ema(df, fast_length, name=name_short_ema)
        PD_Technique.ema(df, slow_length, name=name_long_ema)
        df['tmp_ema'] = df[name_short_ema] / df[name_long_ema] - 1
        df[name] = df["tmp_ema"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["tmp_ema", name_short_ema, name_long_ema], axis=1)
        return df

    @staticmethod
    def dmi_strategy(df, length=14, name=None):
        '''
        dmi 策略
        '''
        if not name:
            name = "dmi_pos_{}".format(length)
        df["plus_di"] = talib.PLUS_DI(
            df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy(), timeperiod=length
        )
        df["minus_di"] = talib.MINUS_DI(
            df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy(), timeperiod=length
        )
        df["tmp_ema"] = df["plus_di"] - df["minus_di"]
        df[name] = df["tmp_ema"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["tmp_ema", "plus_di", "minus_di"], axis=1)
        return df

    @staticmethod
    def ema_talib_strategy(df, fast_length=5, slow_length=20, name=None):
        '''
        ema talib策略
        '''
        if not name:
            name = "ema_pos_{}_{}".format(fast_length, slow_length)

        func_arr = [
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"gt(ema(close, {fast_length}), ema(close, {slow_length}))",
                f"reverse(gt(ema(close, {fast_length}), ema(close, {slow_length})))",
                Direction.LONG.value,
                1
            ],
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"lt(ema(close, {fast_length}), ema(close, {slow_length}))",
                f"reverse(lt(ema(close, {fast_length}), ema(close, {slow_length})))",
                Direction.SHORT.value,
                1
            ],
        ]
        df = PD_Technique.eval(df, func_arr, name=name)
        return df

    @staticmethod
    def dmac_strategy(df, fast_length=5, slow_length=20, b_rate=0, name=None):
        '''
        EMA 修改版
        DMAC策略中，当短均线（STMA）超过长均线（LTMA）达到一定幅度（B），
        即STMA>LTMA*(1+B)，触发多头信号；当短均线（STMA）位于长均线（LTMA）以下达到一定幅度（B），即STMA<LTMA*(1-B)，触发空头信号。
        '''
        if not name:
            name = "dmac_pos_{}_{}".format(fast_length, slow_length)

        func_arr = [
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"gt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 + {b_rate}))",
                f"reverse(gt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 + {b_rate})))",
                Direction.LONG.value,
                1
            ],
            [
                EvalType.MANY_CONDITIONS_FUNC.value,
                f"lt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 - {b_rate}))",
                f"reverse(lt(ema(close, {fast_length}) , ema(close, {slow_length}) * (1 - {b_rate})))",
                Direction.SHORT.value,
                1
            ],
        ]
        df = PD_Technique.eval(df, func_arr, name=name)
        return df

    @staticmethod
    def ema_rsi_strategy(df, fast_length=5, slow_length=20, rsi_length=14, rsi_buy_value=70, rsi_sell_value=30,
                         name=None):
        '''
        ema 策略，只在rsi超买时开多，rsi超卖时开空
        '''
        if not name:
            name = "ema_rsi_{}_{}_{}".format(fast_length, slow_length, rsi_length)
        name_short_ema = "ema_{}".format(fast_length)
        name_long_ema = "ema_{}".format(slow_length)
        PD_Technique.ema(df, fast_length, name=name_short_ema)
        PD_Technique.ema(df, slow_length, name=name_long_ema)

        pos_arr = np.zeros(len(df["close"]))
        pos = 0
        ema_short_arr = df[name_short_ema]
        ema_long_arr = df[name_long_ema]
        rsi_value_arr = talib.RSI(df["close"], timeperiod=14)
        for i in range(1, len(ema_long_arr)):
            if pos == 0:
                if rsi_value_arr[i] > rsi_buy_value and ema_short_arr[i] > ema_long_arr[i] \
                        and ema_short_arr[i - 1] < ema_long_arr[i - 1]:
                    pos = 1

                elif rsi_value_arr[i] < rsi_sell_value and ema_short_arr[i] < ema_long_arr[i] \
                        and ema_short_arr[i - 1] > ema_long_arr[i - 1]:
                    pos = -1

            elif pos > 0:
                if ema_short_arr[i] < ema_long_arr[i]:
                    pos = 0
                    if rsi_value_arr[i] < rsi_sell_value:
                        pos = -1

            elif pos < 0:
                if ema_short_arr[i] > ema_long_arr[i]:
                    pos = 0
                    if rsi_value_arr[i] > rsi_buy_value:
                        pos = 1

            pos_arr[i] = pos
        df[name] = pos_arr
        df = df.drop([name_short_ema, name_long_ema], axis=1)
        return df

    @staticmethod
    def ma_strategy(df, fast_length=5, slow_length=20, name=None):
        '''
        ma 策略
        4小时效果可以，后面钝化了
        '''
        if not name:
            name = "ma_pos_{}_{}".format(fast_length, slow_length)
        name_short_ma = "ma_{}".format(fast_length)
        name_long_ma = "ma_{}".format(slow_length)
        df[name_short_ma] = talib.MA(df["close"], fast_length)
        df[name_long_ma] = talib.MA(df["close"], slow_length)
        df['tmp_ma'] = df[name_short_ma] / df[name_long_ma] - 1
        df[name] = df["tmp_ma"].apply(lambda x: -1.0 if x < 0.0 else (1.0 if x > 0.0 else 0.0))
        df = df.drop(["tmp_ma", name_short_ma, name_long_ma], axis=1)
        return df

    @staticmethod
    def sar_strategy(df, acceleration=0.02, maximum=0.2, name=None):
        '''
        sar 策略
        好像还行，后面钝化明显
        '''
        if not name:
            name = "sar_pos_{}_{}".format(acceleration, maximum)
        sar_array = talib.SAR(df["high"], df["low"], acceleration, maximum)
        pos_arr = []
        for i in range(len(sar_array)):
            if i < 2:
                pos_arr.append(0)
            else:
                if sar_array[i - 1] < min(sar_array[i], sar_array[i - 2]):
                    pos_arr.append(1)
                elif sar_array[i - 1] > max(sar_array[i], sar_array[i - 2]):
                    pos_arr.append(-1)
                else:
                    pos_arr.append(pos_arr[-1])

        df[name] = np.array(pos_arr)
        return df

    @staticmethod
    def boll_strategy(df, n=50, offset=1.2, name=None):
        '''
        布林策略
        这个很好,
        btc: 22800
        eth: 1600
        ltc: 盈利60+
        '''
        if not name:
            name = "boll_pos_{}".format(n)
        PD_Technique.bbands(df, n, numsd=offset, bb_ave_name="bb_ave", up_band_name="up_band", dn_band_name="dn_band")
        df[name] = (df["close"] - df["up_band"]).apply(lambda x: 1 if x > 0 else 0) \
                   + (df["close"] - df["dn_band"]).apply(lambda x: -1 if x < 0 else 0)
        #df[name][df[name] == 0] = np.NAN
        #df.loc[df[name] == 0][name] = np.NAN
        #df[name] = df[name].fillna(method='ffill')
        #df[name] = df[name].ffill()
        df = df.drop(labels=["bb_ave", "up_band", "dn_band"], axis=1)
        return df

    @staticmethod
    def kingkeltner_strategy(df, n=40, name=None):
        '''
        keltner 通道策略
        n = 30
        eth: 1900
        btc: 16000
        ltc: 好像亏钱
        '''
        if not name:
            name = "keltner_pos_{}".format(n)
        PD_Technique.kelch(df, n, name_ch_m="name_ch_m", name_ch_u="name_ch_u", name_ch_d="name_ch_d")
        df[name] = (df["close"] - df["name_ch_u"]).apply(lambda x: 1 if x > 0 else 0) \
                   + (df["close"] - df["name_ch_d"]).apply(lambda x: -1 if x < 0 else 0)
        #df[name][df[name] == 0] = np.NAN
        #df.loc[df[name] == 0][name] = np.NAN
        #df[name] = df[name].fillna(method='ffill')
        #df[name] = df[name].ffill()
        df = df.drop(labels=["name_ch_m", "name_ch_u", "name_ch_d"], axis=1)
        return df

    @staticmethod
    def supertrend_strategy(df, n=10, multiplier=3.0, is_atr=True, name=None):
        '''
        supertrend 策略
        效果还不错的 4h级别
        sharpe_val:0.6005349343333246, trade_times:216, total_income:57435.23162000021, rate:0.26379546557013545
        '''
        df["tr"] = talib.TRANGE(df["high"], df["low"], df["close"])
        if is_atr:
            df["atr_val"] = talib.ATR(df["high"], df["low"], df["close"], timeperiod=n)
        else:
            df["atr_val"] = talib.SMA(df["tr"], n)
        df["src"] = (df["high"] + df["low"]) / 2
        df["name_up"] = df["src"] + df["atr_val"] * multiplier
        df["name_down"] = df["src"] - df["atr_val"] * multiplier
        df[name] = 0
        pre_pos = 0
        for i in range(1, len(df["src"])):
            if df["close"][i] < df["name_up"][i - 1]:
                df["name_up"][i] = min(df["name_up"][i], df["name_up"][i - 1])
            else:
                pre_pos = 1

            if df["close"][i] > df["name_down"][i - 1]:
                df["name_down"][i] = max(df["name_down"][i], df["name_down"][i - 1])
            else:
                pre_pos = -1
            df[name][i] = pre_pos
        df = df.drop(labels=["src", "name_up", "name_down", "tr"], axis=1)
        return df

    @staticmethod
    def ema_slope_trend_follower(df, ma_average_type="EMA", source_ma_length=130, slopeflen=9, slopeslen=21,
                                 trendfilter=True, trendfilterperiod=200, trendfiltertype="EMA",
                                 volatilityfilter=False, volatilitystdevlength=20, volatilitystdevmalength=30,
                                 name=None):
        '''
        :param ma_average_type: EMA or MA
        :param trendfiltertype: EMA or SMA
        '''
        if len(df["close"]) < max(source_ma_length, slopeflen, slopeslen, trendfilterperiod, volatilitystdevlength):
            df[name] = np.zeros(len(df["close"]))
            return df

        if ma_average_type == "EMA":
            df["out"] = talib.EMA(df["close"].to_numpy(), timeperiod=source_ma_length)
        else:
            df["out"] = talib.MA(df["close"].to_numpy(), timeperiod=source_ma_length)
        df["slp"] = df["out"].shift() / df["out"]
        df["emaslopef"] = talib.EMA(df["slp"].to_numpy(), slopeflen)
        df["emaslopes"] = talib.EMA(df["slp"].to_numpy(), slopeslen)

        if trendfilter:
            if trendfiltertype == "EMA":
                df["trendcondition"] = df["close"] - talib.EMA(df["close"].to_numpy(), trendfilterperiod)
            else:
                df["trendcondition"] = df["close"] - talib.SMA(df["close"].to_numpy(), trendfilterperiod)
        df["volatilitycondition"] = talib.STDDEV(df["close"].to_numpy(), timeperiod=5, nbdev=1) - talib.SMA(
            talib.STDDEV(df["close"].to_numpy(), volatilitystdevlength), volatilitystdevmalength
        )

        if trendfilter and volatilityfilter:
            df["conditionentryL"] = (df["emaslopef"] > df["emaslopes"]) & (df["trendcondition"] > 0) & (
                        df["volatilitycondition"] > 0)
            df["conditionentryS"] = (df["emaslopef"] < df["emaslopes"]) & (df["trendcondition"] < 0) & (
                        df["volatilitycondition"] > 0)
        elif trendfilter:
            df["conditionentryL"] = (df["emaslopef"] > df["emaslopes"]) & (df["trendcondition"] > 0)
            df["conditionentryS"] = (df["emaslopef"] < df["emaslopes"]) & (df["trendcondition"] < 0)
        elif volatilityfilter:
            df["conditionentryL"] = (df["emaslopef"] > df["emaslopes"]) & (df["volatilitycondition"] > 0)
            df["conditionentryS"] = (df["emaslopef"] < df["emaslopes"]) & (df["volatilitycondition"] > 0)
        else:
            df["conditionentryL"] = (df["emaslopef"] > df["emaslopes"])
            df["conditionentryS"] = (df["emaslopef"] < df["emaslopes"])

        df["conditionexitL"] = crossdown(df["emaslopef"], df["emaslopes"])
        df["conditionexitS"] = crossup(df["emaslopef"], df["emaslopes"])
        pre_pos = 0
        pos_arr = np.zeros(len(df["conditionexitS"]))
        for i in range(len(pos_arr)):
            if df["conditionentryL"][i] > 0 and df["conditionexitL"][i] <= 0:
                pre_pos = 1
            elif df["conditionentryS"][i] > 0 and df["conditionexitS"][i] <= 0:
                pre_pos = -1
            elif pre_pos == 1 and df["conditionexitL"][i] > 0:
                pre_pos = 0
            elif pre_pos == -1 and df["conditionexitS"][i] > 0:
                pre_pos = 0

            pos_arr[i] = pre_pos
        df[name] = pos_arr
        df = df.drop(labels=["out", "slp", "emaslopef", "emaslopes", "trendcondition", "volatilitycondition",
                             "conditionentryL", "conditionentryS", "conditionexitL", "conditionexitS"], axis=1)
        return df

    @staticmethod
    def aligator_strategy(df, cf=5, cm=8, cs=13, d_cf=3, d_cm=5, d_cs=8, name=None):
        '''
        鳄鱼线策略
        '''
        if not name:
            name = "aligator_pos_{}_{}_{}_{}_{}_{}".format(cf, cm, cs, d_cf, d_cm, d_cs)

        shangfenxing = []
        xiafengxing = []

        PD_Technique.ema(df, cf, name="lips_N")
        PD_Technique.ema(df, cm, name="teech_N")
        PD_Technique.ema(df, cs, name="croco_N")

        high_array = list(df["high"])
        low_array = list(df["low"])
        close_array = list(df["close"])
        lips_n = list(df["lips_N"])
        teeth_n = list(df["teech_N"])
        croco_n = list(df["croco_N"])
        n = len(high_array)

        max_canshu = max(d_cf, d_cm, d_cs)

        pos_ret = []
        pre_pos = 0
        for i in range(n):
            pos = pre_pos
            if i >= 4:
                if high_array[i - 2] > max(high_array[i], high_array[i - 1], high_array[i - 3], high_array[i - 4]):
                    shangfenxing.append(high_array[i - 2])
                if low_array[i - 2] < min(low_array[i], low_array[i - 1], low_array[i - 3], low_array[i - 4]):
                    xiafengxing.append(low_array[i - 2])

            if i >= max_canshu - 1 and i > 0:
                lips = lips_n[i - d_cf]
                teeth = teeth_n[i - d_cm + 1]

                break_up = -1
                for j in range(len(shangfenxing) - 1, -1, -1):
                    if shangfenxing[j] > teeth:
                        break_up = shangfenxing[j]
                        break

                break_down = -1
                for j in range(len(xiafengxing) - 1, -1, -1):
                    if xiafengxing[j] < teeth:
                        break_down = xiafengxing[j]
                        break

                if pre_pos == 0:
                    if 0 < break_up <= close_array[i]:
                        pos = 1
                    if 0 < close_array[i] <= break_down:
                        pos = -1
                elif pre_pos > 0:
                    if close_array[i] < lips:
                        pos = 0
                elif pre_pos < 0:
                    if close_array[i] > lips:
                        pos = 0

            pos_ret.append(pos)
            pre_pos = pos

        df[name] = np.array(pos_ret)
        df = df.drop(["lips_N", "teech_N", "croco_N"], axis=1)
        return df

    @staticmethod
    def four_week_strategy(df, n=30, name=None):
        '''
        一般般, btc:15000, eth:1300
        四周突破策略, 好像亏钱
        '''
        if not name:
            name = "four_week_pos_{}".format(n)

        df[name] = (df["high"] - df["high"].rolling(window=n, center=False).max().shift()).apply(
            lambda x: 1 if x > 0 else 0) \
                   + (df["low"] - df["low"].rolling(window=n, center=False).min().shift()).apply(
            lambda x: -1 if x < 0 else 0)
        #df[name][df[name] == 0] = np.NAN
        #df.loc[df[name] == 0][name] = np.NAN
        #df = df.fillna(method='ffill')
        #df = df.ffill()
        return df

    @staticmethod
    def open_hhv_llv_strategy(df, n=30, name=None):
        '''
        高低突破策略
        '''
        if not name:
            name = "hhv_llv_pos_{}".format(n)

        df[name] = (df["close"] - df["close"].rolling(window=n, center=False).max().shift()).apply(
            lambda x: 1 if x > 0 else 0) \
                   + (df["close"] - df["close"].rolling(window=n, center=False).min().shift()).apply(
            lambda x: -1 if x < 0 else 0)
        #df[name][df[name] == 0] = np.NAN
        #df.loc[df[name] == 0][name] = np.NAN
        #df = df.fillna(method='ffill')
        #df = df.ffill()
        return df

    @staticmethod
    def ht_trend_strategy(df, atr_window=20, atr_xishu=0.25, name=None):
        '''
        ht 趋势策略
        感觉这个不行
        '''
        if not name:
            name = "ht_trend_pos_{}".format(atr_window)

        df["ht_trend"] = talib.HT_TRENDLINE(df["close"])
        df["atr_val"] = talib.ATR(df["high"], df["low"], df["close"], timeperiod=atr_window)
        df["signal"] = df["ht_trend"] - (atr_xishu * df["atr_val"] + df["close"])
        df[name] = df["signal"].apply(lambda x: -1.0 if x < -0 else (1.0 if x > 0 else 0.0))
        # df[name] = df["signal"].apply(lambda x: -1.0 if x < -0 else (1.0 if x > 0 else 0.0))
        df[name] = df[name] * -1
        df = df.drop(labels=["ht_trend", "atr_val", "signal"], axis=1)
        return df

    @staticmethod
    def ma_ema_atr_strategy(df, ma_length=5, ema_length=300, atr_window=40, atr_xishu=0.5, name=None):
        '''
        感觉这个也有点问题
        crossdown(ma(close, 5), ema(close, 300) - 0.5 * atr(high, low, close, 40))[-1]
        '''
        if not name:
            name = "ma_ema_atr_{}".format(ma_length, ema_length, atr_window)
        df["ma"] = talib.MA(df["close"], timeperiod=ma_length)
        df["ema"] = talib.EMA(df["close"], timeperiod=ema_length)
        df["atr"] = talib.ATR(df["high"], df["low"], df["close"], timeperiod=atr_window)
        df["signal"] = df["ma"] - (df["ema"] - atr_xishu * df["atr"])
        df[name] = df["signal"].apply(lambda x: -1.0 if x < -0 else (1.0 if x > 0 else 0.0))
        # df[name] = df[name] * -1
        df = df.drop(labels=["ma", "ema", "atr", "signal"], axis=1)
        return df

    @staticmethod
    def cci_strategy(df, n=20, fazhi=80, name=None):
        '''
        还可以, 9000-10000
        '''
        if not name:
            name = "cci_pos_{}".format(n)
        df = PD_Technique.cci(df, n=20, constant=0.015, name="tmp_cci")
        df[name] = df["tmp_cci"].apply(lambda x: -1.0 if x < -fazhi else (1.0 if x > fazhi else 0.0))
        df = df.drop(labels=["tmp_cci"], axis=1)
        return df

    @staticmethod
    def ema_stddev_strategy(df, fast_length=5, dev_length=13, dev_xishu=1.4, name=None):
        '''
        好像有问题
        '''
        if not name:
            name = "ema_stddev_pos_{}_{}".format(fast_length, dev_length)
        df["ema"] = talib.EMA(df["close"], timeperiod=fast_length)
        df["dev"] = talib.STDDEV(df["close"], timeperiod=dev_length, nbdev=dev_xishu)
        df["signal"] = df["ema"] - (df["close"] + df["dev"])
        df[name] = df["signal"].apply(lambda x: -1.0 if x < 0 else (1.0 if x > 0 else 0.0))
        # df[name] = df[name] * -1
        df = df.drop(labels=["ema", "dev", "signal"], axis=1)
        return df

    @staticmethod
    def boll_reverse(df, n=50, offset=1.2, name=None):
        '''
        回归
        还未测试

        看品种，回归策略
        '''
        if not name:
            name = "boll_reverse_pos_{}".format(n)
        PD_Technique.bbands(df, n, numsd=offset, bb_ave_name="bb_ave", up_band_name="up_band", dn_band_name="dn_band")
        df[name] = (df["close"] - df["up_band"]).apply(lambda x: -1 if x > 0 else 0) \
                   + (df["close"] - df["dn_band"]).apply(lambda x: 1 if x < 0 else 0)
        #df[name][df[name] == 0] = np.NAN
        #df.loc[df[name] == 0][name] = np.NAN
        #df[name] = df[name].fillna(method='ffill')
        #df[name] = df[name].ffill()
        df = df.drop(labels=["bb_ave", "up_band", "dn_band"], axis=1)
        return df

    @staticmethod
    def boll_reverse_mid(df, n=50, offset=1.2, name=None):
        '''
        亏的
        还未完全测试，只是跑通
        '''
        if not name:
            name = "boll_reverse_mid_pos_{}".format(n)
        PD_Technique.bbands(df, n, numsd=offset, bb_ave_name="bb_ave", up_band_name="up_band", dn_band_name="dn_band")

        close_arr = list(df["close"])
        up_band_arr = list(df["up_band"])
        dn_band_arr = list(df["dn_band"])
        bb_ave_arr = list(df["bb_ave"])

        pre_pos = 0
        now_pos_ret = []
        ll = len(up_band_arr)
        for i in range(ll):
            if close_arr[i] > up_band_arr[i]:
                pre_pos = -1
            elif close_arr[i] < dn_band_arr[i]:
                pre_pos = 1
            elif pre_pos == -1 and close_arr[i] < bb_ave_arr[i]:
                pre_pos = 0
            elif pre_pos == 1 and close_arr[i] > bb_ave_arr[i]:
                pre_pos = 0
            now_pos_ret.append(pre_pos)

        df[name] = np.array(now_pos_ret)
        df = df.drop(labels=["bb_ave", "up_band", "dn_band"], axis=1)
        return df

    @staticmethod
    def quick_income_compute(df, sllippage, rate, size=1, name="income",
                             name_rate="income_rate", pos_name="pos", debug=False):
        '''
        支持多股票同时回测
        :param df: 传入的矩阵df
        :param sllippage: 滑点
        :param rate: 交易的手续费 0.1 表示千一
        :return:
        '''
        if "symbol" in df.columns:
            symbol_arr = list(df["symbol"])
        else:
            symbol_arr = [col[0] for col in df.index]
        if debug:
            df.to_csv("c.log")
        win_times = 0
        loss_times = 0
        total_fee = 0
        datetime_arr = list(df["datetime"])
        close_arr = list(df["close"])
        open_arr = list(df["open"])
        close_arr.append(close_arr[-1])  # 多加一个close, 用于后面补充计算
        open_arr.append(open_arr[-1])  # 多加一个open, 用于后面补充计算
        datetime_arr.append(datetime_arr[-1])  # 多加一个open, 用于后面补充计算
        pos_arr = list(df[pos_name])
        for i in range(len(pos_arr)):
            if str(pos_arr[i]) == "nan":
                pos_arr[i] = 0

        income_rate_ret = []
        income_ret = []
        ll = len(pos_arr)
        income_rate = 0
        income = 0
        pre_pos = 0
        last_entry_price = 0
        new_entry_price = 0
        last_entry_time = ""
        new_entry_time = ""
        exit_time = ""

        for i in range(ll):
            if i > 0 and symbol_arr[i] != symbol_arr[i - 1]:
                pre_pos = 0
                last_entry_price = 0
                new_entry_price = 0
                last_entry_time = ""
                new_entry_time = ""
            fee = size * abs(pos_arr[i] - pre_pos) * (open_arr[i + 1] * rate + sllippage)
            pc_pos = 0
            exit_price = 0
            if pre_pos > 0:
                if pos_arr[i] < pre_pos:
                    pc_pos = min(pre_pos, pre_pos - pos_arr[i])
                    exit_price = open_arr[i + 1]
                    exit_time = datetime_arr[i + 1]

                    if pos_arr[i] < 0:
                        new_entry_price = open_arr[i + 1]
                        new_entry_time = datetime_arr[i + 1]

                elif pos_arr[i] > pre_pos:
                    pc_pos = 0
                    exit_price = 0
                    exit_time = ""
                    new_sz = pos_arr[i] - pre_pos
                    new_entry_price = (last_entry_price * abs(pre_pos) + open_arr[i + 1] * new_sz) / abs(pos_arr[i])
                    new_entry_time = datetime_arr[i + 1]

            elif pre_pos < 0:
                if pos_arr[i] < pre_pos:
                    pc_pos = 0
                    exit_price = 0
                    exit_time = ""
                    new_sz = abs(pos_arr[i] - pre_pos)
                    new_entry_price = (last_entry_price * abs(pre_pos) + open_arr[i + 1] * new_sz) / abs(pos_arr[i])
                    new_entry_time = datetime_arr[i + 1]

                elif pos_arr[i] > pre_pos:
                    pc_pos = min(pos_arr[i] - pre_pos, abs(pre_pos))
                    exit_price = open_arr[i + 1]
                    exit_time = datetime_arr[i + 1]
                    if pos_arr[i] > 0:
                        new_entry_price = open_arr[i + 1]
                        new_entry_time = datetime_arr[i + 1]
            else:
                if pos_arr[i] > 0:
                    new_entry_price = open_arr[i + 1]
                    new_entry_time = datetime_arr[i + 1]
                elif pos_arr[i] < 0:
                    new_entry_price = open_arr[i + 1]
                    new_entry_time = datetime_arr[i + 1]

            if pre_pos > 0:
                direction = 1
            elif pre_pos < 0:
                direction = -1
            else:
                direction = 0

            if i + 1 < ll and symbol_arr[i + 1] == symbol_arr[i]:
                pnl = size * pc_pos * (exit_price - last_entry_price) * direction
            else:
                pnl = size * pc_pos * (close_arr[i] - last_entry_price) * direction

            if debug:
                print(f"datetime:{datetime_arr[i]}, pos:{pos_arr[i]}")
            if abs(pnl) > 0:
                if debug:
                    print(
                        f"{last_entry_time}-{exit_time},pnl:{pnl},fee:{fee},close:{close_arr[i]},exit_price:{exit_price},"
                        f"last_entry_price:{last_entry_price},pos:{pos_arr[i]},pc_pos:{pc_pos},direction:{direction}")
                income += pnl - fee
                income_rate += (pnl - fee) / close_arr[i]

                if pnl - fee > 0:
                    win_times += 1
                else:
                    loss_times += 1
            total_fee += fee

            income_ret.append(income)
            income_rate_ret.append(income_rate)
            last_entry_price = new_entry_price
            last_entry_time = new_entry_time
            pre_pos = pos_arr[i]

        df[name] = np.array(income_ret)
        df[name_rate] = np.array(income_rate_ret)
        print(f"[quick_income_compute] total_income:{income_ret[-1]} total_fee:{total_fee}"
              f" win_times:{win_times} loss_times:{loss_times}")
        return df

    @staticmethod
    def quick_rate_compute(df, slippage, fee_rate, dates_arr, add_hours=8, pos_name="pos",
                           direction=Direction.BOTH.value):
        '''
        传入pos信息， 以及在交易的某几天，得到每日的交易绩效
        1. 需要注意的是，交易日都是8点开始的K线，所以需要增加 add_hours，默认是8

        :return
        @fee_dic = {"2018-01-01 00:00:00": 23} 交易日手续费
        @slippage_dic = {"2018-01-01 00:00:00": 23} 交易日滑点
        @income_dic = {"2018-01-01 00:00:00": 23} 交易日盈亏 (扣除手续费与滑点)
        '''
        close_arr = list(df["close"])
        open_arr = list(df["open"])
        datetime_arr = list(df["datetime"])
        pos_arr = list(df[pos_name])
        for i in range(len(pos_arr)):
            if str(pos_arr[i]) == "nan":
                pos_arr[i] = 0

        fee_dic = {}
        slippage_dic = {}
        income_dic = {}

        pre_dateime_str = ""
        ll = len(pos_arr)
        entry_pos = 0
        for i in range(ll):
            tmp_datetime_str = (datetime.strptime(str(datetime_arr[i]), '%Y-%m-%d %H:%M:%S') - timedelta(
                hours=add_hours)).strftime("%Y-%m-%d") + " 00:00:00"
            # print(f"i:{i}, tmp_datetime_str:{tmp_datetime_str} pre_dateime_str:{pre_dateime_str}")
            # 判断是否在交易日, 如在交易日，则累加进收入数据里
            if tmp_datetime_str in dates_arr:
                if tmp_datetime_str not in fee_dic.keys():
                    fee_dic[tmp_datetime_str] = 0
                    slippage_dic[tmp_datetime_str] = 0
                    income_dic[tmp_datetime_str] = 0

                if i > 0:
                    pre_signal_pos = pos_arr[i - 1]
                    # print(f"pre_signal_pos:{pre_signal_pos}")
                    if pre_signal_pos != entry_pos:
                        change_size = 0
                        if direction == Direction.BOTH.value:
                            change_size = abs(pre_signal_pos - entry_pos)
                            entry_pos = pre_signal_pos
                        elif direction == Direction.LONG.value:
                            if pre_signal_pos >= 0:
                                change_size = abs(pre_signal_pos - entry_pos)
                                entry_pos = pre_signal_pos
                            else:
                                change_size = abs(entry_pos)
                                entry_pos = 0
                        elif direction == Direction.SHORT.value:
                            if pre_signal_pos <= 0:
                                change_size = abs(pre_signal_pos - entry_pos)
                                entry_pos = pre_signal_pos
                            else:
                                change_size = abs(entry_pos)
                                entry_pos = 0
                        else:
                            print("[quick_rate_compute] direction error!")

                        # print(f"change_size:{change_size}")
                        if change_size > 0:
                            fee_dic[tmp_datetime_str] += change_size * fee_rate
                            slippage_dic[tmp_datetime_str] += change_size * slippage / close_arr[i]
                            income_dic[tmp_datetime_str] -= change_size * fee_rate + change_size * slippage / close_arr[
                                i]

                    # # debug
                    # if abs(entry_pos) > 0:
                    #     print(f"add {tmp_datetime_str} {open_arr[i]}, {close_arr[i]}, "
                    #           f"{(close_arr[i] - open_arr[i]) / open_arr[i] * entry_pos}")

                    income_dic[tmp_datetime_str] += (close_arr[i] - open_arr[i]) / open_arr[i] * entry_pos

                    # # debug
                    # if abs(entry_pos) > 0:
                    #     print(f"income_dic[{tmp_datetime_str}]:{income_dic[tmp_datetime_str]}")
            else:
                if i > 0:
                    change_size = abs(entry_pos)
                    if change_size > 0:
                        if pre_dateime_str in fee_dic.keys():
                            fee_dic[pre_dateime_str] += change_size * fee_rate
                            slippage_dic[pre_dateime_str] += change_size * slippage / close_arr[i - 1]

                            # print("debug add {}".format(change_size * fee_rate + change_size * slippage / close_arr[i - 1]))
                            income_dic[pre_dateime_str] -= change_size * fee_rate + change_size * slippage \
                                                           / close_arr[i - 1]
                        else:
                            print(f"[quick_rate_compute] error pre_dateime_str:{pre_dateime_str}!")

                    entry_pos = 0

            pre_dateime_str = tmp_datetime_str

            # debug
            # print(f"after i:{i}, tmp_datetime_str:{tmp_datetime_str}"
            #       f" pre_dateime_str:{pre_dateime_str} entry_pos:{entry_pos}")

        return fee_dic, slippage_dic, income_dic

    @staticmethod
    def quick_compute_current_drawdown(df,
                                       name_cur_down="cur_drawdown",
                                       name_max_drawdown="max_drawdown",
                                       name_max_drawdown_percent="max_drawdown_percent",
                                       column="income"):
        '''
        通过income计算 当前一次离最近的高点的最大回撤
        '''
        income_arr = list(df[column])
        cur_drawdown_arr = []
        max_drawdown_arr = []
        max_drawdown_percent_arr = []
        n = len(income_arr)
        now_max_income = 0
        pre_max_drawdown = 0
        for i in range(0, n):
            now_max_income = max(income_arr[i], now_max_income)
            cur_down = now_max_income - income_arr[i]
            pre_max_drawdown = max(pre_max_drawdown, cur_down)
            cur_drawdown_arr.append(cur_down)
            max_drawdown_arr.append(pre_max_drawdown)
            if abs(now_max_income) < 1e-8:
                max_drawdown_percent_arr.append(0)
            else:
                max_drawdown_percent_arr.append(pre_max_drawdown / now_max_income)

        df[name_cur_down] = np.array(cur_drawdown_arr)
        df[name_max_drawdown] = np.array(max_drawdown_arr)
        df[name_max_drawdown_percent] = np.array(max_drawdown_percent_arr)
        return df

    @staticmethod
    def eval(df, func_arr, name="pos"):
        pos_arr = np.zeros(len(df.index))
        if len(func_arr) == 0:
            df[name] = pos_arr
            return df

        if not isinstance(func_arr[0], list):
            func_arr = [func_arr]

        eval_many_arr = []
        eval_many_pct_s_arr = []
        eval_many_atr_s_arr = []
        eval_many_condition_arr = []
        for arr in func_arr:
            u_type = arr[0]
            arr = arr[1:]

            if u_type == EvalType.SIMPLE_FUNC.value:
                eval_many_arr.append(arr)
            elif u_type == EvalType.PCT_STOP_FUNC.value:
                eval_many_pct_s_arr.append(arr)
            elif u_type == EvalType.ATR_STOP_FUNC.value:
                eval_many_atr_s_arr.append(arr)
            elif u_type == EvalType.MANY_CONDITIONS_FUNC.value:
                eval_many_condition_arr.append(arr)
            else:
                print("func_arr:{} length not right!".format(arr))
                continue

        if len(eval_many_arr) > 0:
            ms_df = PD_Technique.eval_many(df, eval_many_arr, name)
            pos_arr = pos_arr + ms_df[name]

        if len(eval_many_pct_s_arr) > 0:
            ms_df = PD_Technique.eval_many_pct_s(df, eval_many_pct_s_arr, name)
            pos_arr = pos_arr + ms_df[name]

        if len(eval_many_atr_s_arr) > 0:
            ms_df = PD_Technique.eval_many_atr_s(df, eval_many_atr_s_arr, name)
            pos_arr = pos_arr + ms_df[name]

        if len(eval_many_condition_arr) > 0:
            ms_df = PD_Technique.eval_many_conditions(df, eval_many_condition_arr, name)
            pos_arr = pos_arr + ms_df[name]

        df[name] = pos_arr
        return df

    @staticmethod
    def eval_many(df, func_arr, name="pos"):
        '''
        多空双做

        func_arr = [
            ("crossup(ema(close, 5), ema(close, 20))", 1),
            ("crossdown(ema(close, 5), ema(close, 20))", 1)
            ("sar_long(high, low, 0.009)", 1),
            ("sar_short(high, low, 0.009)", 1)
        ]
        '''

        def format_func_c(s):
            if "PD_Technique" in s:
                s = s.replace('df', "eval_df")
            return s

        open = df["open"]
        high = df["high"]
        low = df["low"]
        close = df["close"]
        eval_df = copy(df)

        df[name] = 0
        for func_str, pos_v in func_arr:
            tpos = eval(format_func_c(func_str))
            df[name] += tpos * pos_v
        return df

    @staticmethod
    def eval_many_pct_s(df, func_arr, name="pos"):
        '''
        简单入场 + 止损出场
        pct sun
        func_arr = [
            # ("crossup(ema(close, 8), close + 1.4109 * stddev(close, 13))", 0.01, 1),
            # ("crossdown(open, ref(llv(close, 200), 1))", 0.05, 1)
        ]
        '''

        def replace_pct_func(s, _pct):
            tps = [
                ["crossdown", "crossdown_s"],
                ["crossup", "crossup_s"],
                ["sar_long", "sar_long_s"],
                ["sar_short", "sar_short_s"],
            ]
            for ori, tp in tps:
                s = s.replace(ori, tp)
            s = s[:-1] + ", {}, close, high, low)".format(_pct)
            return s

        open = df["open"]
        high = df["high"]
        low = df["low"]
        close = df["close"]

        df[name] = 0
        for func_str, pct, pos_v in func_arr:
            func_str = replace_pct_func(func_str, pct)
            tpos = eval(func_str)
            df[name] += tpos * pos_v
        return df

    @staticmethod
    def eval_many_atr_s(df, func_arr, name="pos"):
        '''
        简单入场 + atr止损出场
        pct sun
        func_arr = [
            # ("crossup(ema(close, 8), close + 1.4109 * stddev(close, 13))", 14, 2, 1),
            # ("crossdown(open, ref(llv(close, 200), 1))", 0.05, 1)
        ]
        '''

        def replace_atr_func(s, _atr_period, _atr_ratio):
            tps = [
                ["crossdown", "crossdown_a"],
                ["crossup", "crossup_a"],
                ["sar_long", "sar_long_a"],
                ["sar_short", "sar_short_a"],
            ]
            for ori, tp in tps:
                s = s.replace(ori, tp)
            s = s[:-1] + ", {}, {}, close, high, low)".format(_atr_period, _atr_ratio)
            return s

        open = df["open"]
        high = df["high"]
        low = df["low"]
        close = df["close"]

        df[name] = 0
        for func_str, atr_period, atr_ratio, pos_v in func_arr:
            func_str = replace_atr_func(func_str, atr_period, atr_ratio)
            tpos = eval(func_str)
            df[name] += tpos * pos_v
        return df

    @staticmethod
    def eval_many_conditions(df, func_arr, name="pos"):
        '''
        条件入场 + 条件出场
        func_arr = [
            [
                "and(crossup(ema(close, 5), ema(close, 20))", "gt(rsi(close, 14), 10))",
                "and(crossdown(ema(close, 5), ema(close, 20)))",
                LONG,
                1
            ],
            [(""), (""), SHORT, 1]
        ]
        '''

        def format_func_c(s):
            tps = [
                ["crossup", "crossup_c"],
                ["gt", "gt_c"],
                ["crossdown", "crossdown_c"],
                ["lt", "lt_c"],
                ["and", "and_c"],
                ["or", "or_c"],
                ["reverse", "reverse_c"]
            ]
            for ori, tp in tps:
                s = s.replace(ori, tp)
            return s

        def get_pos(o_sig, c_sig, direction):
            p = 0
            pos = np.zeros(len(o_sig))
            for i in range(len(pos)):
                if p == 0 and o_sig[i] > 0 and c_sig[i] == 0:
                    if direction == Direction.LONG.value:
                        p = 1
                    else:
                        p = -1
                elif p != 0 and c_sig[i] > 0:
                    p = 0
                pos[i] = p
            return pos

        open = df["open"]
        high = df["high"]
        low = df["low"]
        close = df["close"]

        df[name] = 0
        for entry_func_str, exit_func_str, direction, pos_v in func_arr:
            o_sig = eval(format_func_c(entry_func_str))
            c_sig = eval(format_func_c(exit_func_str))

            tpos = get_pos(o_sig, c_sig, direction)
            df[name] += tpos * pos_v

        return df

    @staticmethod
    def assume_get_day_df(df):
        '''
        通过income计算策略
        '''
        trade_times = 0
        entry_price = 0
        pre_pos = 0
        now_day = ""
        daily_result = []
        close_daily_result = []
        date_result = []
        close_arr = list(df["close"])
        income_arr = list(df["income"])
        if "datetime" in df.columns:
            datetime_arr = list(df["datetime"])
            if datetime_arr and not isinstance(datetime_arr[0], str):
                datetime_arr = [str(x) for x in datetime_arr]
        else:
            datetime_arr = [col[1] for col in df.index]
        pos_arr = list(df["pos"])
        n = len(income_arr)
        for i in range(n):
            pos = pos_arr[i]
            if pos != pre_pos:
                trade_times += 1
            close = close_arr[i]
            if pos != pre_pos:
                if pre_pos == 0:
                    entry_price = close
                elif pre_pos > 0:
                    if pos < 0:
                        entry_price = close
                    elif pos > pre_pos:
                        entry_price = (entry_price * pre_pos + (pos - pre_pos) * close) * 1.0 / pos
                else:
                    if pos > 0:
                        entry_price = close
                    elif pos < pre_pos:
                        entry_price = (entry_price * pre_pos + (pos - pre_pos) * close) * 1.0 / pos

            dt = datetime_arr[i]
            d, t = dt.split(' ')
            income = income_arr[i]
            if (now_day and now_day != d) or (i == n - 1 and now_day == d):
                # 下面的计算有问题，还没找到原因
                # daily_result.append(income + (close - entry_price) * pos)
                # print(d, pos, pre_pos, entry_price, close, income_arr[i], (close - entry_price) * pos)
                daily_result.append(income)
                close_daily_result.append(close)
                date_result.append(d)

            now_day = d
            pre_pos = pos

        df = pd.DataFrame(np.array(daily_result), columns=['balance'])
        df["close"] = np.array(close_daily_result)
        df["date"] = date_result
        df["cb_ratio"] = PD_Technique.break_continue_sigal(df["balance"], array=True)
        return df, pos_arr, trade_times

    @staticmethod
    def assume_strategy_df(df, trade_days=365, capital=1000000):
        df["balance"] = df["balance"] + capital
        df["return"] = np.log(df["balance"] / df["balance"].shift(1)).fillna(0)
        daily_return = df["return"].mean() * 100
        return_std = df["return"].std() * 100
        total_income = list(df["balance"])[-1]

        sharpe_ratio = daily_return / return_std * np.sqrt(trade_days)
        if "close" in df.columns and len(df["close"]) > 0:
            rate = (total_income - capital) / list(df["close"])[-1] / (len(df["close"]) / 365.0)
        else:
            rate = 0

        df["balance"] = df["balance"] - capital

        df = PD_Technique.quick_compute_current_drawdown(df, name_cur_down="cur_down",
                                                         name_max_drawdown="max_down",
                                                         name_max_drawdown_percent="max_down_percent",
                                                         column="balance")
        return {
            "sharpe_ratio": sharpe_ratio,
            "total_income": total_income - capital,
            "max_down_val": max(list(df["max_down"])),
            "max_down_percent": max(list(df["max_down_percent"])),
            "rate": rate,
            "cb_ratio": PD_Technique.break_continue_sigal(df["balance"], array=False),
            "df": df
        }

    @staticmethod
    def assume_strategy(df, trade_days=365, capital=1000000):
        '''
        快速获得一个策略的绩效报告
        '''
        df, pos_arr, trade_times = PD_Technique.assume_get_day_df(df)
        ans_dic = PD_Technique.assume_strategy_df(df, trade_days, capital)
        trade_total_days = len(df["balance"])
        ans_dic["trade_times_per_day"] = trade_times * 1.0 / trade_total_days
        ans_dic["trade_total_days"] = trade_total_days
        ans_dic["trade_times"] = trade_times
        return ans_dic

    @staticmethod
    def assume_incomes(balances):
        df = pd.DataFrame(balances, columns=['balance'])
        return PD_Technique.assume_strategy_df(df)

    @staticmethod
    def assume_dic(dic):
        arr = list(dic.keys())
        arr.sort()
        ret = []
        for x in arr:
            ret.append(dic[x])
        return PD_Technique.assume_incomes(ret)

    @staticmethod
    def assume_strategy_rise(df):
        '''
        追踪策略
        获得策略最近的一日涨跌幅，最近三日，最近一周，最近一月，最近三个月，最近六个月，最近一年的收益情况
        '''

        def _c_rise(a, b):
            v = (bal_arr[max(n - a - 1, 0)] - bal_arr[max(n - b - 1, 0)]) / close_price
            return v

        close_price = list(df["close"])[-1]
        df, pos_arr, trade_times = PD_Technique.assume_get_day_df(df)
        bal_arr = list(df["balance"])
        n = len(bal_arr)

        return {
            "1d": _c_rise(0, 1),
            "3d": _c_rise(0, 3),
            "7d": _c_rise(0, 7),
            "1m": _c_rise(0, 30),
            "3m": _c_rise(0, 90),
            "1-3m": _c_rise(31, 90),
            "6m": _c_rise(0, 180),
            "1-6m": _c_rise(31, 180),
            "3-6m": _c_rise(91, 180),
            "1y": _c_rise(0, 365),
            "1m-1y": _c_rise(31, 365),
            "3m-1y": _c_rise(91, 365),
            "6m-1y": _c_rise(181, 365),
            "2y": _c_rise(0, 730),
            "1m-2y": _c_rise(31, 730),
            "3m-2y": _c_rise(91, 730),
            "6m-2y": _c_rise(181, 730),
            "1y-2y": _c_rise(366, 730),
            "all": _c_rise(0, 10000),
            "6m-9m": _c_rise(181, 270),
            "9m-12m": _c_rise(271, 360),
            "12m-15m": _c_rise(361, 450),
            "15m-18m": _c_rise(451, 540),
            "18m-21m": _c_rise(541, 630),
            "21m-24m": _c_rise(631, 720),
        }


class FundManagement(object):
    @staticmethod
    def adjust_positions(df, entry_max_nums=3, name_tot_pos="pos", min_level_percent=0.1,
                         sllippage=0, rate=0.001, size=1):
        '''
        df 需要先计算 income, max_drawdown, cur_drawdown
        如果当前回撤值 达到 最大回撤的1/3，加仓，达到2/3加仓， 达到1加仓
        '''
        fibo_arr = [1, 1]
        for i in range(entry_max_nums):
            fibo_arr.append(fibo_arr[i - 1] + fibo_arr[i - 2])
        df = PD_Technique.quick_income_compute(df, sllippage, rate, size=size, name="income", pos_name=name_tot_pos)
        df = PD_Technique.quick_compute_current_drawdown(df, name_cur_down="cur_down", name_max_drawdown="max_down")

        total_pos_list = []
        add_position_val = 0
        bef_multi = 0
        now_com_drawdown = 0
        pos_list = list(df[name_tot_pos])
        cur_drawdown_list = list(df["cur_down"])
        max_drawdown_list = list(df["max_down"])
        close_list = list(df["close"])

        ret_new_pos_list = []
        n = len(cur_drawdown_list)
        for i in range(n):
            pos = pos_list[i]
            if str(pos) == "nan":
                pos = 0
            close = close_list[i]
            cur_drawdown = cur_drawdown_list[i]
            max_drawdown = max_drawdown_list[i]

            if bef_multi == 0:
                now_com_drawdown = max(now_com_drawdown, close * min_level_percent)
                now_com_drawdown = max(now_com_drawdown, max_drawdown)
                add_position_val = now_com_drawdown * 1.0 / entry_max_nums

            if cur_drawdown > 0:
                multi = int(cur_drawdown / add_position_val)
                multi = min(entry_max_nums, multi)
                multi = fibo_arr[multi]
            else:
                multi = 0
            ret_new_pos_list.append(pos * multi)
            total_pos_list.append(pos * multi + pos)
            bef_multi = multi

        df[name_tot_pos] = np.array(total_pos_list)
        df = df.drop(labels=["income", "cur_down", "max_down"], axis=1)
        return df


class Technique(object):
    @staticmethod
    def subtract(price1, price2):
        n = len(price1)
        ret = np.zeros(n)
        for i in range(n):
            ret[i] = price1[i] - price2[i]
        return ret

    @staticmethod
    def plus(price1, price2):
        n = len(price1)
        ret = np.zeros(n)
        for i in range(n):
            ret[i] = price1[i] + price2[i]
        return ret

    @staticmethod
    def x_average(price, length):
        n = len(price)
        s_fcactor = 2.0 / (length + 1)
        x_average1 = np.zeros(n)
        for i in range(0, n):
            if i == 0:
                x_average1[i] = price[i]
            else:
                x_average1[i] = x_average1[i - 1] + s_fcactor * (price[i] - x_average1[i - 1])
        return x_average1

    @staticmethod
    def highest_np(price, length):
        n = len(price)
        ret = np.zeros(n)
        for i in range(n):
            ret[i] = price[i]
            j = i - 1
            while j > i - length and j >= 0:
                ret[i] = max(ret[i], price[j])
                j = j - 1
        return ret

    @staticmethod
    def highest(price, length):
        n = min(len(price), length)
        mx = 0
        for i in range(1, n + 1):
            mx = max(price[-i], mx)
        return mx

    @staticmethod
    def lowest_np(price, length):
        n = len(price)
        ret = np.zeros(n)
        for i in range(n):
            ret[i] = price[i]
            j = i - 1
            while j > i - length and j >= 0:
                ret[i] = min(ret[i], price[j])
                j = j - 1
        return ret

    @staticmethod
    def lowest(price, length):
        n = min(len(price), length)
        mv = 9999999
        for i in range(1, n + 1):
            mv = min(price[-i], mv)
        return mv

    @staticmethod
    def summation(price, length):
        n = len(price)
        ret = np.zeros(n)
        if n > 0:
            ret[0] = price[0] * length
        for i in range(1, n):
            if i < length:
                ret[i] = price[i] + ret[i - 1] - price[0]
            else:
                ret[i] = price[i] + ret[i - 1] - price[i - length]
        return ret

    @staticmethod
    def variance_ps(price, length, data_type=1):
        """
        求估计方差
        """
        n = len(price)
        ret = np.zeros(n)
        _divisor = length - 1
        if 1 == data_type:
            _divisor = length
        if _divisor > 0:
            _mean = Technique.average(price, length)
            ret[0] = 0
            for i in range(1, n):
                tmp = 0.0
                for j in range(length):
                    tmp += (_mean[i] - price[max(0, i - j)]) * (_mean[i] - price[max(0, i - j)])
                ret[i] = tmp
            for i in range(n):
                ret[i] = ret[i] * 1.0 / length
        else:
            for i in range(n):
                ret[i] = 0.0
        return ret

    @staticmethod
    def standard_dev(price, length, data_type=1):
        """
        STD
        """
        n = len(price)
        ret = np.zeros(n)
        _varpsValue = Technique.variance_ps(price, length, data_type)
        for i in range(n):
            if _varpsValue[i] > 0:
                ret[i] = sqrt(_varpsValue[i])
            else:
                ret[i] = 0.0
        return ret

    @staticmethod
    def average(price, length):
        n = len(price)
        ret = np.zeros(n)
        tmp_sum = Technique.summation(price, length)
        for i in range(n):
            ret[i] = tmp_sum[i] * 1.0 / length
        return ret

    @staticmethod
    def macd(close_array, fast_length, slow_length, macd_length):
        n = len(close_array)
        t_ave_fast = Technique.x_average(close_array, fast_length)
        t_ave_slow = Technique.x_average(close_array, slow_length)

        macd_value = np.zeros(n)
        for i in range(n):
            macd_value[i] = t_ave_fast[i] - t_ave_slow[i]

        avg_macd = Technique.x_average(macd_value, macd_length)

        macd_diff = np.zeros(n)
        for i in range(n):
            macd_diff[i] = macd_value[i] - avg_macd[i]

        return macd_value, avg_macd, macd_diff

    @staticmethod
    def kdj(high_array, low_array, close_array, length, slow_length, smooth_length):
        n = len(high_array)
        highest_value = Technique.highest(high_array, length)
        lowest_value = Technique.lowest(low_array, length)
        hl_minus = Technique.subtract(highest_value, lowest_value)
        cl_minus = Technique.subtract(close_array, lowest_value)
        sum_hl_value = Technique.summation(hl_minus, slow_length)
        sum_cl_value = Technique.summation(cl_minus, slow_length)
        k_arr = np.zeros(n)
        d_arr = np.zeros(n)
        j_arr = np.zeros(n)
        for i in range(n):
            if fabs(sum_hl_value[i]) > 0.0000001:
                k_arr[i] = sum_cl_value[i] * 100.0 / sum_hl_value[i]
            else:
                k_arr[i] = 0.0
        d_arr = Technique.average(k_arr, smooth_length)
        for i in range(n):
            j_arr[i] = 3 * k_arr[i] - 2 * d_arr[i]
        return k_arr, d_arr, j_arr

    @staticmethod
    def rsi(close_array, length):
        n = len(close_array)
        net_chg_avg = 0
        tot_chg_avg = 0
        pre_net_chg_avg = 0.0
        pre_tot_chg_avg = 0.0
        chg_ratio = 0.0
        rsi_value = np.zeros(n)
        for i in range(n):
            if i < length:
                net_chg_avg = (close_array[i] - close_array[max(i - length, 0)]) * 1.0 / length
                sum_val = 0.0
                for j in range(length):
                    sum_val += fabs(close_array[max(i - j, 0)] - close_array[max(i - j - 1, 0)])
                sum_val = sum_val * 1.0 / length
                tot_chg_avg = sum_val
            else:
                sf = 1.0 / length
                change = close_array[i] - close_array[max(i - 1, 0)]
                net_chg_avg = pre_net_chg_avg + sf * (change - pre_net_chg_avg)
                tot_chg_avg = pre_tot_chg_avg + sf * (abs(change) - pre_tot_chg_avg)

            if abs(tot_chg_avg) > 0.000001:
                chg_ratio = net_chg_avg * 1.0 / tot_chg_avg
            else:
                chg_ratio = 0
            rsi_value[i] = 50 * (chg_ratio + 1)

            pre_net_chg_avg = net_chg_avg
            pre_tot_chg_avg = tot_chg_avg
        return rsi_value

    @staticmethod
    def boll(close_array, n, dev=1):
        close_array = np.array(list(close_array))
        upperband, middleband, lowerband = talib.BBANDS(close_array,
                                                        timeperiod=n, nbdevup=dev, nbdevdn=dev, matype=0)
        ret = np.zeros(len(close_array))
        for i in range(len(close_array)):
            if close_array[i] >= upperband[i]:
                ret[i] = (close_array[i] - upperband[i]) / close_array[i]
            elif close_array[i] <= lowerband[i]:
                ret[i] = ((close_array[i] - lowerband[i]) / close_array[i])
            else:
                ret[i] = 0
        return ret

    @staticmethod
    def er(val_arr, n=3):
        '''
        实现噪声因子，来帮助全市场选币
        ER(i) = abs(P[n] - P[n-i+1]) / sum(abs(P[i]-P[i-1]))
        '''
        val_arr = list(val_arr)
        sp = 0
        ret = []
        ll = len(val_arr)
        for i in range(ll):
            if i > 0:
                sp += abs(val_arr[i] - val_arr[i - 1])
            if i >= n - 1:
                if i - n >= 0:
                    sp -= abs(val_arr[i - n + 1] - val_arr[i - n])
                ap = abs(val_arr[i] - val_arr[i - n + 1])

                try:
                    ret.append(ap / sp)
                except Exception as ex:
                    print(f"[er] {ex} ap:{ap}, sp:{sp}")
                    ret.append(0)
            else:
                ret.append(0)
        return np.array(ret)

    @staticmethod
    def der(val_arr, n=3):
        '''
        实现噪声因子，来帮助全市场选币
        ER(i) = (P[n] - P[n-i+1]) / sum(abs(P[i]-P[i-1]))
        '''
        val_arr = list(val_arr)
        sp = 0
        ret = []
        ll = len(val_arr)
        for i in range(ll):
            if i > 0:
                sp += abs(val_arr[i] - val_arr[i - 1])
            if i >= n - 1:
                if i - n >= 0:
                    sp -= abs(val_arr[i - n + 1] - val_arr[i - n])
                ap = val_arr[i] - val_arr[i - n + 1]
                try:
                    ret.append(ap / sp)
                except Exception as ex:
                    print(f"[er] {ex} ap:{ap}, sp:{sp}")
                    ret.append(0)
            else:
                ret.append(0)
        return np.array(ret)

    @staticmethod
    def true_range(high_array, low_array, close_array):
        """
        TrueRange
        """
        n = len(close_array)
        tr_arr = np.zeros(n)
        if n > 0:
            tr_arr[0] = high_array[0] - low_array[0]
        for i in range(1, n):
            true_high = max(high_array[i], close_array[i - 1])
            true_low = min(low_array[i], close_array[i - 1])
            tr_arr[i] = true_high - true_low
        return tr_arr

    @staticmethod
    def avg_true_range(high_array, low_array, close_array, length):
        """
        ATR
        """
        tr_arr = Technique.true_range(high_array, low_array, close_array)
        return Technique.average(tr_arr, length)

    @staticmethod
    def cross_over(price1, price2):
        """
        求是否上穿
        """
        n = min(len(price1), len(price2))
        if n > 0:
            if price1[-1] > price2[-1]:
                counter = 2
                con1 = price1[-2] == price2[-2]
                while con1 is True and (counter < n):
                    counter = counter + 1
                    con1 = price1[-counter] == price2[-counter]
                pre_con = price1[-counter] < price2[-counter]
                return pre_con
        return False

    @staticmethod
    def cross_under(price1, price2):
        """
        求是否下破
        """
        n = min(len(price1), len(price2))
        if n > 0:
            if price1[-1] < price2[-1]:
                counter = 2
                con1 = price1[-2] == price2[-2]
                while con1 is True and (counter < n):
                    counter = counter + 1
                    con1 = price1[-counter] == price2[-counter]
                pre_con = price1[-counter] > price2[-counter]
                return pre_con
        return False

    @staticmethod
    def cci(high_array, low_array, close_array, length, avg_length):
        n = len(close_array)
        tmp_value = np.zeros(n)
        for i in range(n):
            tmp_value[i] = high_array[i] + low_array[i] + close_array[i]
        _mean = Technique.average(tmp_value, length)
        avg_dev = 0
        for i in range(n):
            avg_dev = avg_dev + abs(tmp_value[i] - _mean)

        cci_value = np.zeros(n)

        if avg_dev > 0:
            for i in range(n):
                cci_value[i] = (tmp_value - _mean) / (0.015 * avg_dev)

        cci_avg = Technique.average(cci_value, avg_length)
        return (cci_value, cci_avg)
