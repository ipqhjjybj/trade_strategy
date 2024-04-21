# coding=utf-8

from copy import copy
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import product
from functools import lru_cache
from enum import Enum
from time import time
import multiprocessing
import random

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas import DataFrame
from deap import creator, base, tools, algorithms

from tumbler.function import get_vt_key, get_from_vt_key, get_round_order_price
from tumbler.constant import EMPTY_STRING, EMPTY_FLOAT, MAX_PRICE_NUM
from tumbler.constant import Status, Direction, StopOrderStatus, OrderType
from tumbler.object import TickData, TradeData, OrderData, StopOrder, BarData
from tumbler.service import mongo_service_manager
from tumbler.function.pnl import StrategyPnlStat
from tumbler.service.log_service import log_service_manager


STOPORDER_PREFIX = "8btc_stop"

sns.set_style("whitegrid")
creator.create("FitnessMax", base.Fitness, weights=(1.0,))
creator.create("Individual", list, fitness=creator.FitnessMax)


class BacktestingMode(Enum):
    """
    Direction of order/trade/position.
    """
    BAR = "BAR"                 # bar级别回测
    TICK = "TICK"               # tick级别回测


class OptimizationSetting:
    """
    Setting for runnning optimization.
    """

    def __init__(self):
        """"""
        self.params = {}
        self.target_name = ""

    def add_parameter(self, name, start, end=None, step=None):
        if not end and not step:
            self.params[name] = [start]
            return

        if start >= end:
            log_service_manager.write_log("start need less than end")
            return

        if step <= 0:
            log_service_manager.write_log("step need bigger than 0")
            return

        value = start
        value_list = []

        while value <= end:
            value_list.append(value)
            value += step

        self.params[name] = value_list

    def set_target(self, target_name):
        self.target_name = target_name

    def generate_setting(self):
        """
        产生 暴力循环的 参数迭代配置
        """
        keys = self.params.keys()
        values = self.params.values()
        products = list(product(*values))

        settings = []
        for p in products:
            setting = dict(zip(keys, p))
            settings.append(setting)

        return settings

    def generate_setting_ga(self):
        """
        产生 遗传学习算法的 参数迭代配置
        """
        settings_ga = []
        settings = self.generate_setting()
        for d in settings:
            param = [tuple(i) for i in d.items()]
            settings_ga.append(param)
        return settings_ga


class BacktestingEngine:
    gateway_name = "BACKTESTING"

    def __init__(self):
        self.vt_symbol = ""
        self.symbol = ""
        self.exchange = None
        self.start = None
        self.end = None
        self.rate = 0
        self.slippage = 0
        self.size = 1
        self.price_tick = 0
        self.capital = 1000000
        self.mode = BacktestingMode.TICK.value

        self.strategy_class = None
        self.strategy = None
        self.tick = None
        self.bar = None
        self.datetime = None

        self.interval = None
        self.days = 0
        self.callback = None
        self.history_data = []

        self.stop_order_count = 0
        self.stop_orders = {}
        self.active_stop_orders = {}

        self.limit_order_count = 0
        self.limit_orders = {}
        self.active_limit_orders = {}

        self.trade_count = 0
        self.trades = {}

        self.logs = []

        self.daily_results = {}
        self.daily_df = None

        self.state_pnl = StrategyPnlStat(self.vt_symbol)

    def clear_data(self):
        """
        Clear all data of last backtesting.
        """
        self.strategy = None
        self.tick = None
        self.bar = None
        self.datetime = None

        self.stop_order_count = 0
        self.stop_orders.clear()
        self.active_stop_orders.clear()

        self.limit_order_count = 0
        self.limit_orders.clear()
        self.active_limit_orders.clear()

        self.trade_count = 0
        self.trades.clear()

        self.logs.clear()
        self.daily_results.clear()

    def get_contract(self, vt_symbol):
        return None

    def get_account(self, vt_account_id):
        return None

    def set_parameters(self, vt_symbol, interval, start, rate,
                       slippage, size, price_tick, capital=0, end=None, mode=BacktestingMode.BAR.value):

        self.mode = mode
        self.vt_symbol = vt_symbol
        self.interval = interval
        self.rate = rate
        self.slippage = slippage
        self.size = size
        self.price_tick = price_tick
        self.start = start

        self.symbol, exchange_str = get_from_vt_key(self.vt_symbol)
        self.exchange = exchange_str

        if capital:
            self.capital = capital

        if end:
            self.end = end

        if mode:
            self.mode = mode

    def add_strategy(self, strategy_class, setting):
        self.strategy_class = strategy_class
        self.strategy = strategy_class(self, strategy_class.__name__, setting)

    def load_data(self, filename=""):
        self.output("start load_data")

        if not self.end:
            self.end = datetime.now()

        if self.start >= self.end:
            self.output("start_date should less than end")
            return

        if len(self.history_data) > 0:
            self.output("already has data，all data num:{}".format(len(self.history_data)))
            return

        if filename:
            data = BarData.load_file_data(filename)
            data = [bar for bar in data if self.end >= bar.datetime >= self.start]
            self.history_data.extend(data)
            self.output("[1] load history data finished，all data num:{}".format(len(self.history_data)))
            return

        self.history_data.clear()  # Clear previously loaded history data

        # Load 30 days of data each time and allow for progress update
        progress_delta = timedelta(days=30)
        total_delta = self.end - self.start

        start = self.start
        end = self.start + progress_delta
        progress = 0

        while start < self.end:
            end = min(end, self.end)  # Make sure end time stays within set range

            if self.mode == BacktestingMode.BAR.value:
                data = load_bar_data(self.symbol, self.exchange, self.interval, start, end)
            else:
                data = load_tick_data(self.symbol, self.exchange, start, end)

            self.history_data.extend(data)

            progress += progress_delta / total_delta
            progress = min(progress, 1)
            progress_bar = "#" * int(progress * 10)
            self.output("in data speed:{} [{}%]".format(progress_bar, progress * 100))

            start = end
            end += progress_delta

        self.output("[2] load history data finished，all data num:{}".format(len(self.history_data)))

    def run_backtesting(self):
        if self.mode == BacktestingMode.BAR.value:
            func = self.new_bar
        else:
            func = self.new_tick

        self.strategy.on_init()

        # Use the first [days] of history data for initializing strategy
        day_count = 0
        ix = 0

        # for ix, data in enumerate(self.history_data):
        #     if self.datetime and data.datetime.day != self.datetime.day:
        #         day_count += 1
        #         if day_count >= self.days:
        #             break

        #     self.datetime = data.datetime
        #     self.callback(data)

        self.strategy.inited = True
        self.output("strategy.inited")

        self.strategy.on_start()
        self.strategy.trading = True
        self.output("now go to trading")

        # Use the rest of history data for running backtesting
        for data in self.history_data[ix:]:
            self.datetime = data.datetime
            func(data)

        self.output("end trading")

    def calculate_result(self):
        self.output("calculate_result")

        if not self.trades:
            self.output("no tradeds!")
            return

        # Add trade data into daily reuslt.
        for trade in self.trades.values():
            d = trade.datetime.date()
            daily_result = self.daily_results[d]
            daily_result.add_trade(trade)

        # Calculate daily result by iteration.
        pre_close = 0
        start_pos = 0

        for daily_result in self.daily_results.values():
            daily_result.calculate_pnl(pre_close, start_pos, self.size, self.rate, self.slippage)

            pre_close = daily_result.close_price
            start_pos = daily_result.end_pos

        # Generate dataframe
        results = defaultdict(list)

        for daily_result in self.daily_results.values():
            for key, value in daily_result.__dict__.items():
                results[key].append(value)

        self.daily_df = DataFrame.from_dict(results).set_index("date")

        self.output("end calculate result")
        return self.daily_df

    def calculate_statistics(self, df=None, output=True):
        self.output("calculate_statistics")

        # Check DataFrame input exterior
        if df is None:
            df = self.daily_df

        # Check for init DataFrame
        if df is None:
            # Set all statistics to 0 if no trade.
            start_date = ""
            end_date = ""
            total_days = 0
            profit_days = 0
            loss_days = 0
            end_balance = 0
            max_drawdown = 0
            max_ddpercent = 0
            total_net_pnl = 0
            daily_net_pnl = 0
            total_commission = 0
            daily_commission = 0
            total_slippage = 0
            daily_slippage = 0
            total_turnover = 0
            daily_turnover = 0
            total_trade_count = 0
            daily_trade_count = 0
            total_return = 0
            annual_return = 0
            daily_return = 0
            return_std = 0
            sharpe_ratio = 0
            return_drawdown_ratio = 0
        else:
            # Calculate balance related time series data
            df["balance"] = df["net_pnl"].cumsum() + self.capital
            df["return"] = np.log(df["balance"] / df["balance"].shift(1)).fillna(0)
            df["highlevel"] = (
                df["balance"].rolling(
                    min_periods=1, window=len(df), center=False).max()
            )
            df["drawdown"] = df["balance"] - df["highlevel"]
            df["ddpercent"] = df["drawdown"] / df["highlevel"] * 100

            # Calculate statistics value
            start_date = df.index[0]
            end_date = df.index[-1]

            total_days = len(df)
            profit_days = len(df[df["net_pnl"] > 0])
            loss_days = len(df[df["net_pnl"] < 0])

            end_balance = df["balance"].iloc[-1]
            max_drawdown = df["drawdown"].min()
            max_ddpercent = df["ddpercent"].min()

            total_net_pnl = df["net_pnl"].sum()
            daily_net_pnl = total_net_pnl / total_days

            total_commission = df["commission"].sum()
            daily_commission = total_commission / total_days

            total_slippage = df["slippage"].sum()
            daily_slippage = total_slippage / total_days

            total_turnover = df["turnover"].sum()
            daily_turnover = total_turnover / total_days

            total_trade_count = df["trade_count"].sum()
            daily_trade_count = total_trade_count / total_days

            total_return = (end_balance / self.capital - 1) * 100
            annual_return = total_return / total_days * 240
            daily_return = df["return"].mean() * 100
            return_std = df["return"].std() * 100

            if return_std:
                sharpe_ratio = daily_return / return_std * np.sqrt(240)
            else:
                sharpe_ratio = 0

            return_drawdown_ratio = -total_return / max_ddpercent

        # Output
        if output:
            self.output("-" * 30)
            self.output("首个交易日：\t{}".format(start_date))
            self.output("最后交易日：\t{}".format(end_date))

            self.output("总交易日：\t{}".format(total_days))
            self.output("盈利交易日：\t{}".format(profit_days))
            self.output("亏损交易日：\t{}".format(loss_days))

            self.output("起始资金：\t{}".format(self.capital))
            self.output("结束资金：\t{}".format(end_balance))

            self.output("总收益率：\t{}%".format(total_return))
            self.output("年化收益：\t{}%".format(annual_return))
            self.output("最大回撤: \t{}".format(max_drawdown))
            self.output("百分比最大回撤: {}%".format(max_ddpercent))

            self.output("总盈亏：\t{}".format(total_net_pnl))
            self.output("总手续费：\t{}".format(total_commission))
            self.output("总滑点：\t{}".format(total_slippage))
            self.output("总成交金额：\t{}".format(total_turnover))
            self.output("总成交笔数：\t{}".format(total_trade_count))

            self.output("日均盈亏：\t{}".format(daily_net_pnl))
            self.output("日均手续费：\t{}".format(daily_commission))
            self.output("日均滑点：\t{}".format(daily_slippage))
            self.output("日均成交金额：\t{}".format(daily_turnover))
            self.output("日均成交笔数：\t{}".format(daily_trade_count))

            self.output("日均收益率：\t{}%".format(daily_return))
            self.output("收益标准差：\t{}%".format(return_std))
            self.output("Sharpe Ratio：\t{}".format(sharpe_ratio))
            self.output("收益回撤比：\t{}".format(return_drawdown_ratio))

            self.output("总共交易次数: \t{}".format(self.state_pnl.total_trades))
            self.output("总共赚钱次数: \t{}".format(self.state_pnl.total_win_num))
            self.output("总共亏钱次数: \t{}".format(self.state_pnl.total_loss_num))
            self.output("最大连续赚钱次数: \t{}".format(self.state_pnl.max_con_win_num))
            self.output("最大连续亏钱次数: \t{}".format(self.state_pnl.max_con_loss_num))
            self.output("连续赚钱数据分布: \t{}".format(self.state_pnl.show_win_num_state()))
            self.output("连续亏钱数据分布: \t{}".format(self.state_pnl.show_loss_num_state()))
            self.output("最大连续赚钱时间: \t{}".format(self.state_pnl.get_max_continue_win_time()))
            self.output("最大连续亏钱时间: \t{}".format(self.state_pnl.get_max_continue_loss_time()))
            self.output("最大仓位: \t{}".format(self.state_pnl.max_position))
            self.output("仓位分布: \t{}".format(self.state_pnl.get_position_layout()))

            self.output("debug: \t{}".format(self.state_pnl.get_debug()))

        statistics = {
            "start_date": start_date,
            "end_date": end_date,
            "total_days": total_days,
            "profit_days": profit_days,
            "loss_days": loss_days,
            "capital": self.capital,
            "end_balance": end_balance,
            "max_drawdown": max_drawdown,
            "max_ddpercent": max_ddpercent,
            "total_net_pnl": total_net_pnl,
            "daily_net_pnl": daily_net_pnl,
            "total_commission": total_commission,
            "daily_commission": daily_commission,
            "total_slippage": total_slippage,
            "daily_slippage": daily_slippage,
            "total_turnover": total_turnover,
            "daily_turnover": daily_turnover,
            "total_trade_count": total_trade_count,
            "daily_trade_count": daily_trade_count,
            "total_return": total_return,
            "annual_return": annual_return,
            "daily_return": daily_return,
            "return_std": return_std,
            "sharpe_ratio": sharpe_ratio,
            "return_drawdown_ratio": return_drawdown_ratio,
        }

        return statistics

    def show_chart(self, df=None):
        """"""
        # Check DataFrame input exterior
        if df is None:
            df = self.daily_df

        # Check for init DataFrame
        if df is None:
            return

        plt.figure(figsize=(10, 16))

        balance_plot = plt.subplot(4, 1, 1)
        balance_plot.set_title("Balance")
        df["balance"].plot(legend=True)

        drawdown_plot = plt.subplot(4, 1, 2)
        drawdown_plot.set_title("Drawdown")
        drawdown_plot.fill_between(range(len(df)), df["drawdown"].values)

        # pnl_plot = plt.subplot(4, 1, 3)
        # pnl_plot.set_title("Daily Pnl")
        # df["net_pnl"].plot(kind="bar", legend=False, grid=False, xticks=[])
        #
        # distribution_plot = plt.subplot(4, 1, 4)
        # distribution_plot.set_title("Daily Pnl Distribution")
        # df["net_pnl"].hist(bins=50)

        plt.show()

    def run_optimization(self, optimization_setting, output=True):
        # Get optimization setting and target
        settings = optimization_setting.generate_setting()
        target_name = optimization_setting.target_name

        if not settings:
            self.output("优化参数组合为空，请检查")
            return

        if not target_name:
            self.output("优化目标未设置，请检查")
            return

        # Use multiprocessing pool for running backtesting with different setting
        pool = multiprocessing.Pool(multiprocessing.cpu_count())

        results = []
        for setting in settings:
            result = (pool.apply_async(optimize, (
                target_name,
                self.strategy_class,
                setting,
                self.vt_symbol,
                self.interval,
                self.start,
                self.rate,
                self.slippage,
                self.size,
                self.price_tick,
                self.capital,
                self.end,
                self.mode
            )))
            results.append(result)

        pool.close()
        pool.join()

        # Sort results and output
        result_values = [result.get() for result in results]
        result_values.sort(reverse=True, key=lambda result: result[1])

        if output:
            for value in result_values:
                msg = "augument:{}, target:{}".format(value[0], value[1])
                self.output(msg)

        return result_values

    def run_ga_optimization(self, optimization_setting, population_size=100, ngen_size=30, output=True):
        # Get optimization setting and target
        settings = optimization_setting.generate_setting_ga()
        target_name = optimization_setting.target_name

        if not settings:
            self.output("run_ga_optimization no settings")
            return

        if not target_name:
            self.output("run_ga_optimization target_name empty")
            return

        # Define parameter generation function
        def generate_parameter():
            return random.choice(settings)

        def mutate_individual(individual, indpb):
            size = len(individual)
            paramlist = generate_parameter()
            for i in range(size):
                if random.random() < indpb:
                    individual[i] = paramlist[i]
            return individual,

        # Create ga object function
        global ga_target_name
        global ga_strategy_class
        global ga_setting
        global ga_vt_symbol
        global ga_interval
        global ga_start
        global ga_rate
        global ga_slippage
        global ga_size
        global ga_price_tick
        global ga_capital
        global ga_end
        global ga_mode

        ga_target_name = target_name
        ga_strategy_class = self.strategy_class
        ga_setting = settings[0]
        ga_vt_symbol = self.vt_symbol
        ga_interval = self.interval
        ga_start = self.start
        ga_rate = self.rate
        ga_slippage = self.slippage
        ga_size = self.size
        ga_price_tick = self.price_tick
        ga_capital = self.capital
        ga_end = self.end
        ga_mode = self.mode

        # Set up genetic algorithem
        toolbox = base.Toolbox()
        toolbox.register("individual", tools.initIterate, creator.Individual, generate_parameter)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)
        toolbox.register("mate", tools.cxTwoPoint)
        toolbox.register("mutate", mutate_individual, indpb=1)
        toolbox.register("evaluate", ga_optimize)
        toolbox.register("select", tools.selNSGA2)

        total_size = len(settings)
        pop_size = population_size  # number of individuals in each generation
        lambda_ = pop_size  # number of children to produce at each generation
        mu = int(pop_size * 0.8)  # number of individuals to select for the next generation

        cxpb = 0.95  # probability that an offspring is produced by crossover
        mutpb = 1 - cxpb  # probability that an offspring is produced by mutation
        ngen = ngen_size  # number of generation

        pop = toolbox.population(pop_size)
        hof = tools.ParetoFront()  # end result of pareto front

        stats = tools.Statistics(lambda ind: ind.fitness.values)
        np.set_printoptions(suppress=True)
        stats.register("mean", np.mean, axis=0)
        stats.register("std", np.std, axis=0)
        stats.register("min", np.min, axis=0)
        stats.register("max", np.max, axis=0)

        # Multiprocessing is not supported yet.
        # pool = multiprocessing.Pool(multiprocessing.cpu_count())
        # toolbox.register("map", pool.map)

        # Run ga optimization
        self.output("参数优化空间：{}".format(total_size))
        self.output("每代族群总数：{}".format(pop_size))
        self.output("优良筛选个数：{}".format(mu))
        self.output("迭代次数：{}".format(ngen))
        self.output("交叉概率：{}".format(cxpb))
        self.output("突变概率：{}".format(mutpb))

        start = time()

        algorithms.eaMuPlusLambda(pop, toolbox, mu, lambda_, cxpb, mutpb, ngen, stats, halloffame=hof)

        end = time()
        cost = int((end - start))

        self.output("遗传算法优化完成，耗时{}秒".format(cost))

        # Return result list
        results = []

        for parameter_values in hof:
            setting = dict(parameter_values)
            target_value = ga_optimize(parameter_values)[0]
            results.append((setting, target_value, {}))

        return results

    def update_daily_close(self, price):
        d = self.datetime.date()

        daily_result = self.daily_results.get(d, None)
        if daily_result:
            daily_result.close_price = price
        else:
            self.daily_results[d] = DailyResult(d, price)

    def new_bar(self, bar):
        self.bar = bar
        self.datetime = bar.datetime

        self.cross_limit_order()
        self.cross_stop_order()
        self.strategy.on_bar(bar)
        self.state_pnl.on_bar(bar)

        self.update_daily_close(bar.close_price)

    def new_tick(self, tick):
        self.tick = tick
        self.datetime = tick.datetime

        self.cross_limit_order()
        self.cross_stop_order()
        self.strategy.on_tick(tick)

        self.update_daily_close(tick.last_price)

    def cross_limit_order(self):
        """
        Cross limit order with last bar/tick data.
        """
        if self.mode == BacktestingMode.BAR.value:
            long_cross_price = self.bar.low_price
            short_cross_price = self.bar.high_price
            long_best_price = self.bar.open_price
            short_best_price = self.bar.open_price
        else:
            long_cross_price = self.tick.ask_prices[0]
            short_cross_price = self.tick.bid_prices[0]
            long_best_price = long_cross_price
            short_best_price = short_cross_price

        for order in list(self.active_limit_orders.values()):
            # Push order update with status "not traded" (pending).
            is_submitting = False
            if order.status == Status.SUBMITTING.value:
                is_submitting = True
                order.status = Status.NOTTRADED.value
                self.strategy.on_order(order)

            # Check whether limit orders can be filled.
            long_cross = (
                    order.direction == Direction.LONG.value
                    and order.price >= long_cross_price
                    and long_cross_price > 0
            )

            short_cross = (
                    order.direction == Direction.SHORT.value
                    and order.price <= short_cross_price
                    and short_cross_price > 0
            )

            if not long_cross and not short_cross:
                continue

            self.active_limit_orders.pop(order.vt_order_id)

            # Push order udpate with status "all traded" (filled).
            order.traded = order.volume
            order.status = Status.ALLTRADED.value
            self.strategy.on_order(order)

            # Push trade update
            self.trade_count += 1

            trade_price = order.price
            if long_cross:
                if is_submitting:
                    trade_price = min(order.price, long_best_price)
            else:
                if is_submitting:
                    trade_price = max(order.price, short_best_price)

            trade = TradeData()
            trade.symbol = order.symbol
            trade.exchange = order.exchange
            trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)

            trade.order_id = order.order_id
            trade.vt_order_id = order.vt_order_id
            trade.trade_id = str(self.trade_count)
            trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
            trade.direction = order.direction
            trade.offset = order.offset
            trade.price = trade_price
            trade.volume = order.volume
            trade.trade_time = self.datetime.strftime("%Y-%m-%d %H:%M:%S")
            trade.datetime = self.datetime
            trade.gateway_name = self.gateway_name

            self.strategy.on_trade(trade)
            self.state_pnl.on_trade(trade)

            self.trades[trade.vt_trade_id] = trade

    def cross_stop_order(self):
        """
        Cross stop order with last bar/tick data.
        """
        if self.mode == BacktestingMode.BAR.value:
            long_cross_price = self.bar.high_price
            short_cross_price = self.bar.low_price
            long_best_price = self.bar.open_price
            short_best_price = self.bar.open_price
        else:
            long_cross_price = self.tick.last_price
            short_cross_price = self.tick.last_price
            long_best_price = long_cross_price
            short_best_price = short_cross_price

        for stop_order in list(self.active_stop_orders.values()):
            # Check whether stop order can be triggered.
            long_cross = (
                    stop_order.direction == Direction.LONG.value
                    and stop_order.price <= long_cross_price
            )

            short_cross = (
                    stop_order.direction == Direction.SHORT.value
                    and stop_order.price >= short_cross_price
            )

            if not long_cross and not short_cross:
                continue

            # Create order data.
            self.limit_order_count += 1

            order = OrderData()
            order.symbol = stop_order.symbol
            order.exchange = stop_order.exchange
            order.order_id = str(self.limit_order_count)
            order.vt_order_id = get_vt_key(order.order_id, order.exchange)
            order.direction = stop_order.direction
            order.offset = stop_order.offset
            order.price = stop_order.price
            order.volume = stop_order.volume
            order.traded = 0
            order.status = Status.SUBMITTING.value

            order.gateway_name = self.gateway_name
            order.datetime = self.datetime
            order.order_time = order.datetime.strftime("%Y-%m-%d %H:%M:%S")

            self.limit_orders[order.vt_order_id] = order

            # Create trade data.
            if long_cross:
                trade_price = max(stop_order.price, long_best_price)
            else:
                trade_price = min(stop_order.price, short_best_price)

            self.trade_count += 1

            trade = TradeData()
            trade.symbol = order.symbol
            trade.exchange = order.exchange
            trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)

            trade.order_id = order.order_id
            trade.vt_order_id = order.vt_order_id
            trade.trade_id = str(self.trade_count)
            trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
            trade.direction = order.direction
            trade.offset = order.offset
            trade.price = trade_price
            trade.volume = order.volume
            trade.trade_time = self.datetime.strftime("%Y-%m-%d %H:%M:%S")
            trade.datetime = self.datetime
            trade.gateway_name = self.gateway_name

            self.trades[trade.vt_trade_id] = trade

            # Update stop order.
            stop_order.vt_order_ids = [[order.vt_order_id, copy(order)]]
            stop_order.status = StopOrderStatus.TRIGGERED.value

            self.active_stop_orders.pop(stop_order.vt_order_id)

            # Push update to strategy.
            self.strategy.on_stop_order(stop_order)

            order.status = Status.ALLTRADED.value
            order.traded = order.volume
            self.strategy.on_order(order)

            self.strategy.on_trade(trade)
            self.state_pnl.on_trade(trade)

    def load_bar(self, vt_symbol, days, interval, callback):
        self.days = days
        self.callback = callback

    def load_tick(self, vt_symbol, days, callback):
        self.days = days
        self.callback = callback

    def send_order(self, strategy, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        price = get_round_order_price(price, self.price_tick)
        if stop:
            vt_order_id = self.send_stop_order(symbol, exchange, direction, offset, price, volume)
        else:
            vt_order_id = self.send_limit_order(symbol, exchange, direction, offset, price, volume)

        order_id, exchange = get_from_vt_key(vt_order_id)

        if not stop:
            order = OrderData()
            order.symbol = symbol
            order.exchange = exchange
            order.vt_symbol = get_vt_key(symbol, exchange)
            order.order_id = order_id
            order.vt_order_id = vt_order_id
            order.direction = direction
            order.type = OrderType.LIMIT.value
            order.offset = offset
            order.price = price
            order.volume = volume
            order.datetime = self.datetime
            order.order_time = order.datetime.strftime("%Y-%m-%d %H:%M:%S")
            order.status = Status.SUBMITTING.value

            return [[vt_order_id, order]]
        else:
            stop_order = copy(self.stop_orders[vt_order_id])
            return [[vt_order_id, stop_order]]

    def send_stop_order(self, symbol, exchange, direction, offset, price, volume):
        self.stop_order_count += 1

        stop_order = StopOrder()
        stop_order.symbol = symbol
        stop_order.exchange = exchange
        stop_order.vt_symbol = get_vt_key(symbol, exchange)
        stop_order.direction = direction
        stop_order.offset = offset
        stop_order.price = price
        stop_order.volume = volume
        stop_order.vt_order_id = "{}.{}".format(STOPORDER_PREFIX, self.stop_order_count)
        stop_order.strategy_name = self.strategy.strategy_name

        self.active_stop_orders[stop_order.vt_order_id] = copy(stop_order)
        self.stop_orders[stop_order.vt_order_id] = copy(stop_order)

        return stop_order.vt_order_id

    def send_limit_order(self, symbol, exchange, direction, offset, price, volume):
        self.limit_order_count += 1

        order = OrderData()
        order.symbol = symbol
        order.exchange = exchange
        order.vt_symbol = get_vt_key(symbol, exchange)
        order.order_id = str(self.limit_order_count)
        order.vt_order_id = get_vt_key(order.order_id, exchange)
        order.direction = direction
        order.offset = offset
        order.price = price
        order.volume = volume
        order.status = Status.SUBMITTING.value
        order.order_time = order.datetime.strftime("%Y-%m-%d %H:%M:%S")
        order.gateway_name = self.gateway_name

        order.datetime = self.datetime

        self.active_limit_orders[order.vt_order_id] = copy(order)
        self.limit_orders[order.vt_order_id] = copy(order)

        return order.vt_order_id

    def cancel_order(self, strategy, vt_order_id):
        """
        Cancel order by vt_order_id.
        """
        # self.output("cancel_order: vt_order_id:{}".format(vt_order_id))
        if vt_order_id.startswith(STOPORDER_PREFIX):
            self.cancel_stop_order(strategy, vt_order_id)
        else:
            self.cancel_limit_order(strategy, vt_order_id)

    def cancel_stop_order(self, strategy, vt_order_id):
        # self.output("cancel_stop_order vt_order_id:{}, keys:{}".format(vt_order_id, self.active_stop_orders.keys()))
        if vt_order_id not in self.active_stop_orders:
            return
        stop_order = self.active_stop_orders.pop(vt_order_id)

        stop_order.status = StopOrderStatus.CANCELLED.value
        self.strategy.on_stop_order(stop_order)

    def cancel_limit_order(self, strategy, vt_order_id):
        if vt_order_id not in self.active_limit_orders:
            return
        order = self.active_limit_orders.pop(vt_order_id)

        order.status = Status.CANCELLED.value
        self.strategy.on_order(order)

    def cancel_all(self, strategy):
        """
        Cancel all orders, both limit and stop.
        """
        vt_order_ids = list(self.active_limit_orders.keys())
        for vt_order_id in vt_order_ids:
            self.cancel_limit_order(strategy, vt_order_id)

        vt_order_ids = list(self.active_stop_orders.keys())
        for vt_order_id in vt_order_ids:
            self.cancel_stop_order(strategy, vt_order_id)

    def write_log(self, msg, strategy=None):
        """
        Write log message.
        """
        msg = "{}\t{}".format(self.datetime, msg)
        self.logs.append(msg)

    def send_email(self, msg, strategy=None):
        """
        Send email to default receiver.
        """
        pass

    def sync_strategy_data(self, strategy):
        """
        Sync strategy data into json file.
        """
        pass

    def put_strategy_event(self, strategy):
        """
        Put an event to update strategy status.
        """
        pass

    def output(self, msg):
        """
        Output message of backtesting engine.
        """
        log_service_manager.write_log("{}\t{}".format(datetime.now(), msg))

    def get_all_trades(self):
        """
        Return all trade data of current backtesting result.
        """
        return list(self.trades.values())

    def get_all_orders(self):
        """
        Return all limit order data of current backtesting result.
        """
        return list(self.limit_orders.values())

    def get_all_daily_results(self):
        """
        Return all daily result data.
        """
        return list(self.daily_results.values())


class DailyResult:
    """"""

    def __init__(self, date, close_price):
        """"""
        self.date = date
        self.close_price = close_price
        self.pre_close = 0

        self.trades = []
        self.trade_count = 0

        self.start_pos = 0
        self.end_pos = 0

        self.turnover = 0
        self.commission = 0
        self.slippage = 0

        self.trading_pnl = 0
        self.holding_pnl = 0
        self.total_pnl = 0
        self.net_pnl = 0

    def add_trade(self, trade):
        self.trades.append(trade)

    def calculate_pnl(self, pre_close, start_pos, size, rate, slippage):
        self.pre_close = pre_close

        # Holding pnl is the pnl from holding position at day start
        self.start_pos = start_pos
        self.end_pos = start_pos
        self.holding_pnl = self.start_pos * (self.close_price - self.pre_close) * size

        # Trading pnl is the pnl from new trade during the day
        self.trade_count = len(self.trades)

        for trade in self.trades:
            if trade.direction == Direction.LONG.value:
                pos_change = trade.volume
            else:
                pos_change = -trade.volume

            turnover = trade.price * trade.volume * size

            self.trading_pnl += pos_change * (self.close_price - trade.price) * size
            self.end_pos += pos_change
            self.turnover += turnover
            self.commission += turnover * rate
            self.slippage += trade.volume * size * slippage

        # Net pnl takes account of commission and slippage cost
        self.total_pnl = self.trading_pnl + self.holding_pnl
        self.net_pnl = self.total_pnl - self.commission - self.slippage

    def get_string(self):
        arr = []
        for trade in self.trades:
            arr.append((trade.direction, trade.volume, trade.price))
        msg = "{},{},{},{},{},{},{},{},{},{},{},{},{}".format(self.date, self.close_price, self.pre_close,
                                                              self.start_pos, self.end_pos, self.turnover,
                                                              self.commission, self.slippage, \
                                                              self.trading_pnl, self.holding_pnl, self.total_pnl,
                                                              self.net_pnl, arr)
        return msg


def optimize(target_name, strategy_class, setting, vt_symbol,
             interval, start, rate, slippage, size, price_tick,
             capital, end, mode):
    """
    Function for running in multiprocessing.pool
    """
    engine = BacktestingEngine()

    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=interval,
        start=start,
        rate=rate,
        slippage=slippage,
        size=size,
        price_tick=price_tick,
        capital=capital,
        end=end,
        mode=mode
    )

    engine.add_strategy(strategy_class, setting)
    engine.load_data()
    engine.run_backtesting()
    engine.calculate_result()
    statistics = engine.calculate_statistics(output=False)

    target_value = statistics[target_name]
    return str(setting), target_value, statistics


@lru_cache(maxsize=1000000)
def _ga_optimize(parameter_values):
    setting = dict(parameter_values)

    result = optimize(ga_target_name, ga_strategy_class, setting,
                      ga_vt_symbol, ga_interval, ga_start, ga_rate, ga_slippage,
                      ga_size, ga_price_tick, ga_capital, ga_end, ga_mode)
    return (result[1],)


def ga_optimize(parameter_values):
    return _ga_optimize(tuple(parameter_values))


@lru_cache(maxsize=999)
def load_bar_data(symbol, exchange, interval, start, end):
    return mongo_service_manager.load_bar_data(symbol, exchange, interval, start, end)


def load_tick_data(symbol, exchange, start, end):
    return mongo_service_manager.load_tick_data(symbol, exchange, start, end)


# GA related global value
ga_end = None
ga_mode = None
ga_target_name = None
ga_strategy_class = None
ga_setting = None
ga_vt_symbol = None
ga_interval = None
ga_start = None
ga_rate = None
ga_slippage = None
ga_size = None
ga_price_tick = None
ga_capital = None


def get_test_data1():
    ret = []
    f = open("/Users/szh/git/test_tumbler_data/fake/bsv_tick_data.csv", "r")
    for line in f:
        price = float(line)

        t = TickData()
        t.symbol = "bsv_usdt"
        t.exchange = "OKEX"
        t.vt_symbol = get_vt_key(t.symbol, t.exchange)

        t.name = "bsv/usdt"
        t.gateway_name = "test"

        # 成交数据
        t.last_price = price
        t.last_volume = 0
        t.volume = 0

        t.time = EMPTY_STRING
        t.date = EMPTY_STRING
        t.datetime = datetime.now()

        t.upper_limit = EMPTY_FLOAT
        t.lower_limit = EMPTY_FLOAT

        # 五档行情
        t.bid_prices = [t.last_price] * MAX_PRICE_NUM
        t.ask_prices = [t.last_price] * MAX_PRICE_NUM

        t.bid_volumes = [t.last_price] * MAX_PRICE_NUM
        t.ask_volumes = [t.last_price] * MAX_PRICE_NUM

        ret.append(t)

    return ret


def get_test_data2():
    ret = []
    f = open("/Users/szh/git/test_tumbler_data/fake/calendar_spread.csv", "r")
    for line in f:
        symbol, price = line.split(',')
        price = float(price)

        t = TickData()
        t.symbol = symbol
        t.exchange = "OKEXF"
        t.vt_symbol = get_vt_key(t.symbol, t.exchange)

        t.gateway_name = "test"

        # 成交数据
        t.last_price = price
        t.last_volume = 0
        t.volume = 0

        t.time = EMPTY_STRING
        t.date = EMPTY_STRING
        t.datetime = datetime.now()

        t.upper_limit = EMPTY_FLOAT
        t.lower_limit = EMPTY_FLOAT

        # 五档行情
        t.bid_prices = [t.last_price] * MAX_PRICE_NUM
        t.ask_prices = [t.last_price] * MAX_PRICE_NUM

        t.bid_volumes = [t.last_price] * MAX_PRICE_NUM
        t.ask_volumes = [t.last_price] * MAX_PRICE_NUM

        ret.append(t)
    return ret
