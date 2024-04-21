# coding=utf-8
from collections import defaultdict

import pandas as pd
import numpy as np

from tumbler.function.technique import PD_Technique
from tumbler.service import log_service_manager


class FactorModel(object):
    @staticmethod
    def buy_sell_model1(df, buy_stock_num, buy_keep_stock_num, sell_stock_num, sell_keep_stock_num,
                        rank_colume_name, keep_period, fee_rate=0.001):
        '''
        需要保证
        buy_stock_num <= buy_keep_stock_num
        sell_stock_num <= sell_keep_stock_num
        '''
        if buy_stock_num > buy_keep_stock_num or sell_stock_num > sell_keep_stock_num:
            log_service_manager.write_log("[buy_sell_model1] argument not right!")
            return df

        rank_rate_name = "rate_{}".format(1)
        df = PD_Technique.rate(df, 1, field="close", name=rank_rate_name)

        all_stocks = set([])
        k_dic = defaultdict(list)
        rows = df.index
        rank_colume_values = df[rank_colume_name]
        rank_rate_values = df[rank_rate_name]
        for i in range(len(rows)):
            stock, key = rows[i]
            rank = rank_colume_values[i]
            rate = rank_rate_values[i]

            k_dic[key].append((int(rank), stock, rate))
            all_stocks.add(stock)

        len_stocks = len(all_stocks)
        if len_stocks < buy_stock_num + sell_stock_num:
            log_service_manager.write_log("[buy_sell_model1] stocks is not enough!")
            return df

        pre_buy_day_list = []
        buy_income_rate_ret = []
        buy_trade_nums_ret = []

        pre_sell_day_list = []
        sell_income_rate_ret = []
        sell_trade_nums_ret = []

        pre_tot_income = 0
        total_income_rate = []

        keys = list(k_dic.keys())
        keys.sort()
        new_index_arr = []
        ii = 0
        for key in keys:
            arr = k_dic[key]
            ll = len(arr)
            if ll < len_stocks:
                continue

            new_index_arr.append(key)
            if ii % keep_period is not 0:
                #  继续持有
                buy_income_rate_ret.append(sum([x[2] for x in arr if x[1] in pre_buy_day_list]))
                sell_income_rate_ret.append(sum([x[2] for x in arr if x[1] in pre_sell_day_list]))

                buy_trade_nums_ret.append(0)
                sell_trade_nums_ret.append(0)

                if buy_stock_num:
                    buy_income_rate = (buy_income_rate_ret[-1] - buy_trade_nums_ret[-1] * fee_rate) / buy_stock_num
                else:
                    buy_income_rate = 0

                if sell_stock_num:
                    sell_income_rate = (sell_income_rate_ret[-1] + sell_trade_nums_ret[-1] * fee_rate) / sell_stock_num
                else:
                    sell_income_rate = 0

                total_income_rate.append(pre_tot_income + buy_income_rate - sell_income_rate)
                pre_tot_income = total_income_rate[-1]

            else:
                arr.sort()
                new_keep_buy_stocks = [x[1] for x in arr][:buy_keep_stock_num]
                new_buy_stocks_b = [x for x in new_keep_buy_stocks if x not in pre_buy_day_list]
                new_sell_stocks_b = [x for x in pre_buy_day_list if x not in new_keep_buy_stocks]
                pre_buy_day_list = new_buy_stocks_b + [x for x in pre_buy_day_list if x not in new_sell_stocks_b]

                buy_trade_nums_ret.append(len(new_buy_stocks_b) + len(new_sell_stocks_b))
                buy_income_rate_ret.append(sum([x[2] for x in arr if x[1] in pre_buy_day_list]))

                arr.reverse()
                new_keep_sell_stocks = [x[1] for x in arr][:sell_keep_stock_num]
                new_sell_stocks_s = [x for x in new_keep_sell_stocks if x not in pre_sell_day_list]
                new_buy_stocks_s = [x for x in pre_sell_day_list if x not in new_keep_sell_stocks]
                pre_sell_day_list = new_sell_stocks_s + [x for x in pre_sell_day_list if x not in new_buy_stocks_s]

                sell_trade_nums_ret.append(len(new_buy_stocks_s) + len(new_sell_stocks_s))
                sell_income_rate_ret.append(sum([x[2] for x in arr if x[1] in pre_sell_day_list]))

                if buy_stock_num:
                    buy_income_rate = (buy_income_rate_ret[-1] - buy_trade_nums_ret[-1] * fee_rate) / buy_stock_num
                else:
                    buy_income_rate = 0

                if sell_stock_num:
                    sell_income_rate = (sell_income_rate_ret[-1] + sell_trade_nums_ret[-1] * fee_rate) / sell_stock_num
                else:
                    sell_income_rate = 0

                total_income_rate.append(pre_tot_income + buy_income_rate - sell_income_rate)
                pre_tot_income = total_income_rate[-1]

            ii = ii + 1

        if buy_stock_num:
            buy_income_rate_ret = [x/buy_stock_num for x in buy_income_rate_ret]
        if sell_stock_num:
            sell_income_rate_ret = [x/sell_stock_num for x in sell_income_rate_ret]

        r = pd.DataFrame({
            "tot_income": total_income_rate,
            "buy_income_rate": buy_income_rate_ret,
            "sell_income_rate": sell_income_rate_ret,
            "buy_trade_nums": buy_trade_nums_ret,
            "sell_trade_nums": sell_income_rate_ret
        },
            columns=['tot_income', 'buy_income_rate', 'sell_income_rate', 'buy_trade_nums', 'sell_trade_nums'],
            index=new_index_arr
        )
        return r

