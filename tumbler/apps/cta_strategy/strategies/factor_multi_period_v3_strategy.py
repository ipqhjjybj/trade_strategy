# coding=utf-8

from copy import copy
from datetime import timedelta

import pandas as pd

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import BarData, TickData
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.function.bar import BarGenerator, PandasDeal
from tumbler.constant import Direction, Status, Interval
from tumbler.apps.cta_strategy.template import OrderSendModule
from tumbler.function.technique import PD_Technique


class FactorMultiPeriodV3Strategy(CtaTemplate):
    # 着重升级优化这个发单部分，以后独立开来，解耦了
    # 目前忘记有没有测试过了
    """
    因子策略，多策略多周期单品种方式， 用pandas处理策略信号

    因子策略 ， 利用pandas快速 回测一个因子
    因子值需要处理到 只有 -1 到 1之间，利于合并处理
    1、加载初始数据
    2、每根K线过来，合并，计算新的因子值，举个例子: 因子值 > 0.5 则开仓，< 0.5 则平仓, < -0.5开空仓，> -0.5平空仓
    3、计算收益情况
    """
    author = "ipqhjjybj"
    class_name = "FactorMultiPeriodV3Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BINANCE"

    fixed = 1
    pos = 0
    target_pos = 0

    bar_period_factor = []  # func, window, interval,

    is_backtesting = False
    init_days = 60  # 初始化天数

    # 策略变量
    max_window = 20

    parameters = [
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'vt_symbols_subscribe',  # vt_symbol_subscribe
        'symbol_pair',  # 交易对
        'exchange',  # 交易所
        'bar_period_factor',  # BarFactor
        'is_backtesting',  # 是实盘还是回测
        'fixed',
        'exchange_info',
        "pos",
        "init_days"
    ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading'
               ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(FactorMultiPeriodV3Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, interval=Interval.MINUTE.value, quick_minute=1)

        self.bg_pandas_array = []
        for func, window, interval in self.bar_period_factor:
            self.write_log("window:{} interval:{}".format(window, interval))
            pandas_deal = PandasDeal(func, window, interval, strategy=self)
            self.bg_pandas_array.append(pandas_deal)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.order_send_module = OrderSendModule(self, self.symbol_pair, self.exchange,
                                                 self.exchange_info["price_tick"], self.exchange_info["volume_tick"],
                                                 init_pos=self.pos)

        self.last_bar = None

        self.has_updated_contracts = False

    def on_init(self):
        self.write_log("on_init")
        if not self.is_backtesting:
            self.write_log("load_bar")
            self.load_bar(self.init_days)

            self.target_pos = 0
            self.write_log("len bg_pandas_array:{}".format(len(self.bg_pandas_array)))

            i = 0
            for bg_pandas in self.bg_pandas_array:
                self.write_log("[on_init] bg_pandas init!")
                bg_pandas.on_init()
                pos = bg_pandas.get_pos()
                self.write_log("i:{} pos:{}".format(i, pos))
                self.target_pos += pos

            self.target_pos = self.target_pos * self.fixed
            self.order_send_module.set_to_target_pos(self.target_pos)
            self.order_send_module.start()

            self.write_log("on_init after")
            self.trading = True

            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))

        for bg_pandas in self.bg_pandas_array:
            bg_pandas.start_trading()

        self.update_contracts()

    def update_contracts(self):
        if not self.has_updated_contracts:
            contract = self.get_contract(self.vt_symbol)
            if contract:
                self.order_send_module.set_price_tick(contract.price_tick)
                self.order_send_module.set_volume_tick(contract.volume_tick)
                self.has_updated_contracts = True
                self.write_log("[update_contracts] contract {} init success!".format(self.vt_symbol))
                self.write_log("[update_contracts] price_tick:{} volume_tick:{}!"
                               .format(contract.price_tick, contract.volume_tick))
            else:
                self.write_log("[update_contracts] contract {} init failed!".format(self.vt_symbol))

    def on_start(self):
        self.write_log("[on_start]")

    def on_stop(self):
        self.write_log("[on_stop]")

    def on_tick(self, tick):
        self.bg.update_tick(tick)
        self.order_send_module.on_tick(tick)

    def on_bar(self, bar: BarData):
        self.update_contracts()
        self.write_log("[on_bar] datetime:{} close_price:{}".format(bar.datetime, bar.close_price))

        if self.last_bar:
            if bar.datetime - timedelta(minutes=1) != self.last_bar.datetime:
                self.write_important_log("[on_bar] check bar time error! bar.datetime:{} last_bar.datetime:{}"
                                         .format(bar.datetime, self.last_bar.datetime))

                if not self.trading:
                    self.write_log("[on_bar] go to fake bar!")
                    fake_bar = copy(self.last_bar)
                    while fake_bar.datetime + timedelta(minutes=1) < bar.datetime:
                        fake_bar.datetime = fake_bar.datetime + timedelta(minutes=1)
                        fake_bar.open_price = fake_bar.close_price
                        fake_bar.high_price = fake_bar.high_price
                        fake_bar.low_price = fake_bar.low_price
                        fake_bar.volume = 0
                        fake_bar.open_interest = 0

                        self.write_log("[on_bar] pass fake bar:{} {} minute:{} len:{}".format(
                            fake_bar.datetime, fake_bar.close_price, fake_bar.datetime.minute, len(self.bg_pandas_array)))
                        for bg_pandas in self.bg_pandas_array:
                            #self.write_log("[on_bar] pass bg_pandas:{} {}".format(bg_pandas.window, bg_pandas.interval))
                            bg_pandas.on_bar(copy(fake_bar))

                else:
                    self.write_important_log("[on bar] [error] now is trading so not go to fake bar!")

        if self.is_backtesting:
            ticker = TickData.make_ticker(bar.close_price)
            self.order_send_module.on_tick(ticker)
            self.order_send_module.start()

        for bg_pandas in self.bg_pandas_array:
            bg_pandas.on_bar(bar)

        self.target_pos = 0
        for bg_pandas in self.bg_pandas_array:
            self.target_pos += bg_pandas.get_pos()

        self.target_pos = self.target_pos * self.fixed
        if self.trading:
            self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.order_send_module.get_now_pos()))

            self.order_send_module.set_to_target_pos(self.target_pos)

        self.last_bar = copy(bar)

    def on_order(self, order):
        msg = "[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded)
        self.write_log(msg)

        self.order_send_module.on_order(order)
        self.pos = self.order_send_module.get_now_pos()

        if order.traded > 0:
            self.write_important_log(msg)

    def on_trade(self, trade):
        self.write_important_log("[on_trade info] trade:{},{},{},{}\n"
                                 .format(trade.vt_symbol, trade.order_id, trade.direction, trade.volume))

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
