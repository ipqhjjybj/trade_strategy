# coding=utf-8

from datetime import datetime, timedelta
from copy import copy
import numpy as np
import matplotlib.pyplot as plt

from tumbler.function import get_vt_key, get_from_vt_key
from tumbler.service.log_service import log_service_manager
from tumbler.object import BarData

from .base import BacktestingMode
from .backtesting import load_bar_data, load_tick_data
from .batesting_symbol import BacktestingSymbol


class MultiBacktestingEngine:
    gateway_name = "MULTIBACKTESTING"

    def __init__(self):
        self.vt_symbol_engine_map = {}
        self.mode = ""
        self.strategy = None
        self.strategy_class = None
        self.days = 0
        self.callback = None
        self.datetime = None

        self.interval = None

        self.start = None
        self.end = None

        self.capital = 0

        self.order_dict = {}

        self.sum_df = None
        self.save_df = {}

        self.vt_symbols = []
        self.history_data = []

    def clear_data(self):
        """
        Clear all data of last backtesting.
        """
        for vt_symbol, engine in self.vt_symbol_engine_map.items():
            engine.clear_data()

        self.order_dict.clear()

    def set_parameters(self, parameters: list, interval, start, capital=0, end=None, mode=BacktestingMode.BAR.value):
        self.capital = capital
        self.interval = interval
        self.start = start
        self.end = end
        self.mode = mode
        for p in parameters:
            self.vt_symbols.append(p.vt_symbol)
            self.vt_symbol_engine_map[p.vt_symbol] = \
                BacktestingSymbol(self, p.vt_symbol, interval, p.rate, p.slippage, p.size, p.price_tick, mode)

        self.write_log("[self.vt_symbol_engine_map] :{}".format(self.vt_symbol_engine_map.keys()))

    def add_strategy(self, strategy_class, setting):
        self.strategy_class = strategy_class
        self.strategy = strategy_class(self, strategy_class.__name__, setting)

    def load_data(self, filename=""):
        self.output("start load_data")

        if len(self.history_data) > 0:
            self.output("already has data，all data num:{}".format(len(self.history_data)))
            return

        if filename:
            data = BarData.load_file_data(filename)
            self.history_data.extend(data)
            self.output("load history data finished，all data num:{}".format(len(self.history_data)))
            return

        progress_delta = timedelta(days=30)
        self.history_data.clear()  # Clear previously loaded history data
        # Load 30 days of data each time and allow for progress update
        start = self.start
        end = self.start + progress_delta

        for vt_symbol in self.vt_symbols:
            self.output("[load_data] vt_symbol:{}".format(vt_symbol))
            symbol, exchange = get_from_vt_key(vt_symbol)
            while start < self.end:
                if self.mode == BacktestingMode.BAR.value:
                    data = load_bar_data(symbol, exchange, self.interval, start, end)
                else:
                    data = load_tick_data(symbol, exchange, start, end)

                self.history_data.extend(data)

                start = end
                end += progress_delta

        self.history_data.sort()

    def run_backtesting(self):
        if self.mode == BacktestingMode.BAR.value:
            func = self.new_bar
        else:
            func = self.new_tick

        self.strategy.on_init()

        self.strategy.inited = True
        self.output("strategy.inited")

        self.strategy.on_start()
        self.strategy.trading = True
        self.output("now go to trading")

        # Use the rest of history data for running backtesting
        for data in self.history_data:
            self.datetime = data.datetime
            func(data)

        self.output("end trading")

    def calculate_result(self):
        self.output("calculate_result")
        for vt_symbol, engine in self.vt_symbol_engine_map.items():
            symbol_df = engine.calculate_result()
            self.output("symbol_df:{}".format(symbol_df))
            if symbol_df is not None:
                self.save_df[vt_symbol] = copy(symbol_df[["start_pos", "end_pos", "commission", "turnover",
                                                          "slippage", "trading_pnl", "trade_count",
                                                          "holding_pnl", "total_pnl", "net_pnl"]])
        for vt_symbol, df in self.save_df.items():
            if self.sum_df is None:
                self.sum_df = copy(df)
            else:
                self.sum_df = self.sum_df + copy(df)

        if self.sum_df is None:
            self.write_log("[vt_symbol] self.sum_df:{}".format(self.sum_df))
            return

        self.sum_df[:] = 0
        for vt_symbol, df in self.save_df.items():
            self.write_log("self.sum_df:{}".format(self.sum_df))
            self.write_log("vt_symbol:{} df:{}".format(vt_symbol, df))
            self.sum_df = self.sum_df + df

    def calculate_statistics(self, df=None, output=True):
        self.output("calculate_statistics")

        # Check DataFrame input exterior
        if df is None:
            df = self.sum_df

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
        # Check DataFrame input exterior
        if df is None:
            df = self.sum_df

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

        pnl_plot = plt.subplot(4, 1, 3)
        pnl_plot.set_title("Daily Pnl")
        df["net_pnl"].plot(kind="bar", legend=False, grid=False, xticks=[])

        distribution_plot = plt.subplot(4, 1, 4)
        distribution_plot.set_title("Daily Pnl Distribution")
        df["net_pnl"].hist(bins=50)

        plt.show()

    def new_bar(self, bar):
        if bar.vt_symbol is None:
            bar.vt_symbol = get_vt_key(bar.symbol, bar.exchange)
        engine = self.vt_symbol_engine_map.get(bar.vt_symbol, None)
        if engine:
            engine.new_bar(bar)

    def new_tick(self, tick):
        engine = self.vt_symbol_engine_map.get(tick.vt_symbol, None)
        if engine:
            engine.new_tick(tick)

    def load_bar(self, vt_symbol, days, interval, callback):
        self.days = days
        self.callback = callback

    def load_tick(self, vt_symbol, days, callback):
        self.days = days
        self.callback = callback

    def send_order(self, strategy, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        vt_symbol = get_vt_key(symbol, exchange)
        engine = self.vt_symbol_engine_map.get(vt_symbol, None)
        if engine:
            ret = engine.send_order(strategy, symbol, exchange, direction,
                                    offset, price, volume, stop=False, lock=False)
            for vt_order_id, order in ret:
                self.order_dict[vt_order_id] = copy(order)
            return ret
        return []

    def cancel_order(self, strategy, vt_order_id):
        order = self.order_dict.get(vt_order_id, None)
        if vt_order_id:
            engine = self.vt_symbol_engine_map.get(order.vt_symbol, None)
            if engine:
                return engine.cancel_order(strategy, vt_order_id)

    def cancel_all(self, strategy):
        for vt_symbol, engine in self.vt_symbol_engine_map.items():
            engine.cancel_order(strategy)

    def write_log(self, msg, strategy=None):
        """
        Write log message.
        """
        self.output(msg)

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
        return []

    def get_all_orders(self):
        """
        Return all limit order data of current backtesting result.
        """
        return []

    def get_all_daily_results(self):
        """
        Return all daily result data.
        """
        return []
