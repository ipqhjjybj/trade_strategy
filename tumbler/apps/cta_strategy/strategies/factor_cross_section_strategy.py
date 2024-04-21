# coding=utf-8

from copy import copy
from datetime import datetime

import pandas as pd

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import BarData
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.function.bar import BarGenerator
from tumbler.constant import Direction, Status, Interval
from tumbler.object import OrderManager
from tumbler.function.technique import PD_Technique
from tumbler.object import StrategyParameter


def example_compute_factor(df):
    '''
    example
    '''
    pass


class FactorCrossSectionStrategy(CtaTemplate):
    """
    因子策略 ， 利用pandas快速 回测一个因子
    参数
    hold_num:5  --> 表示 等金额买入前五的币种, 可以填0
    sell_num:5  --> 表示 等金额卖出后五的币种, 可以填0

    1、加载初始数据， 合并多只股票到 multi_index 序列
    2、通过multi_index, 计算得到所有币种的排名，
    3、通过不同币种的排名，每天计算各个股票的仓位， 然后交易到指定的数量
    4、计算收益情况
    """
    author = "ipqhjjybj"
    class_name = "FactorCrossSectionStrategy"

    exchange = "BINANCE"

    fixed = 1
    pos = 0
    target_pos = 0

    compute_factor = None  # 给外界算因子的函数
    bar_dict = {}
    params = []
    buy_num = 5  #
    sell_num = 5
    amount_per_stock = 1000

    is_backtesting = False

    initDays = 20  # 初始化天数

    # 策略变量
    bar_window = 1
    max_window = 20

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'params',  # parameters
        'compute_factor',  # 计算因子的函数
        'amount_per_stock',  # amount_per_stock 每只股票买入的金额
        'buy_num',  # 买入的数量
        'sell_num',  # 卖出的数量
        'exchange',  # 交易所
        'bar_window',  # 多少小时周期
        'is_backtesting',  # 是实盘还是回测
        'fixed',
        'exchange_info',
        "pos"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(FactorCrossSectionStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.order_manager_dict = {}
        for parameter in self.params:
            bg = BarGenerator(self.on_bar, window=self.bar_window,
                              on_window_bar=self.on_window_bar,
                              interval=Interval.DAY.value, quick_minute=1)
            self.bar_dict[parameter.vt_symbol] = copy(bg)
            self.order_manager_dict[parameter.vt_symbol] = OrderManager(self, parameter.vt_symbol, parameter.price_tick)

        self.df = None

        self.df_index = []
        self.df_data = []
        self.recent_bars = {}

        self.period_rate_num = 30
        self.count_period = 0

        self.target = "rank"

    def on_init(self):
        self.write_log("on_init")
        if not self.is_backtesting:
            self.load_server_bars(self.vt_symbols_subscribe, 1)
            self.write_log("on_init after")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.tick_send_order(tick)

        self.bar_dict[tick.vt_symbol].update_tick(tick)

    def on_bar(self, bar: BarData):
        self.write_log("[on_bar] bar:{}".format(bar.__dict__))
        self.bar_dict[bar.vt_symbol].update_bar(bar)

    def compute_rank(self, new_data_dict, reverse=False):
        print("[compute_rank]")
        infos = [(v, k) for k, v in new_data_dict.items()]
        infos.sort(reverse=reverse)
        return [v for k, v in infos]

    def compute_pos(self, rank_list):
        print("[compute_pos]")
        if self.trading:
            for vt_symbol in rank_list[:self.buy_num]:
                self.order_manager_dict[vt_symbol].to_target_amount(self.amount_per_stock)
                self.write_log("vt_symbol:{}, amount:{}".format(vt_symbol, self.amount_per_stock))

            for vt_symbol in rank_list[-self.sell_num:]:
                self.order_manager_dict[vt_symbol].to_target_amount(-1 * self.amount_per_stock)
                self.write_log("vt_symbol:{}, amount:{}".format(vt_symbol, -1 * self.amount_per_stock))

    def compute_work(self, df):
        print("[compute_work]")
        new_data_dict = {}
        df = df.sort_index()
        print(df)
        for para in self.params:
            ndf = df.loc[para.vt_symbol, :]
            ndf = self.compute_factor(ndf, name="factor")
            n = ndf.shape[0] - 1
            new_data_dict[para.vt_symbol] = ndf["factor"][n]
        return new_data_dict

    def on_window_bar(self, bar: BarData):
        self.write_log("[on_window_bar]:{}".format(bar.vt_symbol))
        if self.buy_num + self.sell_num > len(self.parameters):
            self.write_log("[Error] buy sell num not right!")
            return

        self.write_log("[on_window_bar] bar:{}".format(bar.__dict__))
        self.recent_bars[bar.vt_symbol] = copy(bar)

        self.df_index.append(bar.get_unique_index())
        self.df_data.append(bar.get_np_data())

        #print("self.df_data:{}".format(self.df_data))
        self.df = pd.DataFrame(self.df_data, index=BarData.get_pandas_index(self.df_index),
                               columns=BarData.get_multi_index_columes())

        data_dict = self.compute_work(self.df)
        rank_list = self.compute_rank(data_dict)
        self.compute_pos(rank_list)

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        self.order_manager_dict[order.vt_symbol].on_order(order)

    def on_trade(self, trade):
        self.write_log("[on_trade info] trade:{},{},{},{}\n".format(trade.vt_symbol,
                                    trade.order_id, trade.direction, trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
