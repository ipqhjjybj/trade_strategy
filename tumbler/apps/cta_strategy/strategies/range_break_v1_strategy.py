# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData, TickData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.strategy import StrategyStatistic
from tumbler.function.technique import Technique
from tumbler.apps.cta_strategy.template import OrderSendModule


class RangeBreakV1Strategy(CtaTemplate):
    """
    这个策略 首先通过计算出每日波动幅度，然后波动突破的时候开多开空
    """
    author = "ipqhjjybj"
    class_name = "RangeBreakV1Strategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    bsetup = 0
    ssetup = 0
    bbreak = 0
    sbreak = 0
    benter = 0
    senter = 0
    oo = 0
    hh = 0
    ll = 0
    cz = 0
    hz = 0
    lz = 0

    rate = 0.5
    is_backtesting = False

    bar_window = 1
    interval = Interval.DAY.value
    fixed = 1

    pos = 0
    init_days = 20
    exit_days = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'rate',    # 系数
                  'is_backtesting', # is_backtesting 是否是回测
                  'fixed',      # 固定开仓大小
                  'interval',   # 周期
                  'pos',        # 初始仓位
                  'bar_window',  # bar线
                  'exchange_info',
                  'exit_days'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(RangeBreakV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)
        self.am = ArrayManager(10)

        self.pos = 0

        self.sell_line = 0
        self.buy_line = 0

        self.max_open_num = 4
        self.now_open_num = 0

        self.order_send_module = OrderSendModule(self, self.symbol_pair, self.exchange,
                                                 self.exchange_info["price_tick"], self.exchange_info["volume_tick"],
                                                 init_pos=self.pos)
        self.strategy_statics = StrategyStatistic()

        self.pre_price = 0

    def on_init(self):
        self.write_log("on_init")
        if not self.is_backtesting:
            self.write_log("load_bar")
            self.load_bar(self.init_days)

        self.order_send_module.start()

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)
        self.order_send_module.on_tick(tick)

    def on_bar(self, bar):
        self.bg.update_bar(bar)

        self.hh = max(bar.high_price, self.hh)
        self.ll = min(bar.low_price, self.ll)

        self.bsetup = self.lz - 0.35 * (self.hz - self.cz)
        self.ssetup = self.hz + 0.35 * (self.cz - self.lz)
        self.bbreak = self.ssetup + 0.25 * (self.ssetup - self.bsetup)
        self.sbreak = self.bsetup - 0.25 * (self.ssetup - self.bsetup)
        self.benter = (1 + 0.07)/2 * (self.lz + self.cz) - 0.07 * self.hz
        self.senter = (1 + 0.07)/2 * (self.hz + self.cz) - 0.07 * self.lz
        zgx = self.benter - (self.bsetup - self.ll) / 3
        zgs = self.senter + (self.hh - self.ssetup) / 3
        rfilter = (self.hz - self.lz) >= 0.2 * (self.oo/100.0)

        signal = 0
        update_pos_flag = False
        target_pos = 0
        if self.pre_price:
            if self.pos == 0:
                self.write_log("[on_bar] pos==0 close_price:{} bbreak:{} pre_price:{} rfilter:{}"
                               .format(bar.close_price, self.bbreak, self.pre_price, rfilter))

                self.write_log("[on_bar] pos==0 self.pre_price:{} bar.close_price:{} self.sbreak:{} rfilter:{}"
                               .format(self.pre_price, bar.close_price, self.sbreak, rfilter))
                if bar.close_price >= self.bbreak >= self.pre_price > 0 and rfilter:
                    target_pos = 1
                    update_pos_flag = True
                    signal = 1

                elif self.pre_price >= bar.close_price >= self.sbreak > 0 and rfilter:
                    target_pos = -1
                    update_pos_flag = True
                    signal = 2

            elif self.pos > 0:
                self.write_log(f"[on_bar] pos>0 ll:{self.ll}, bsetup:{self.bsetup}, "
                               f"close_price:{bar.close_price} zgx:{zgx}, pre_price:{self.pre_price} ")
                if self.ll <= self.bsetup and bar.close_price >= zgx >= self.pre_price:
                    target_pos = 1
                    update_pos_flag = True
                    signal = 3

                bk_price = self.order_send_module.get_avg_price()
                if bk_price - bar.close_price >= self.oo / 100:
                    target_pos = 0
                    update_pos_flag = True
                    signal = 4

            elif self.pos < 0:
                self.write_log(f"[on_bar] pos<0 hh:{self.hh}, ssetup:{self.ssetup}, close_price:{bar.close_price} "
                               f"zgs:{zgs} pre_price:{self.pre_price}")
                if self.hh >= self.ssetup and bar.close_price <= zgs <= self.pre_price:
                    target_pos = -1
                    update_pos_flag = True
                    signal = 5

                sk_price = self.order_send_module.get_avg_price()
                if bar.close_price - sk_price >= self.oo / 100.0:
                    target_pos = 0
                    update_pos_flag = True
                    signal = 6

        self.pre_price = bar.close_price

        if self.trading and update_pos_flag:
            self.write_log("[on_bar] signal:{}".format(signal))
            self.write_important_log("[on_bar] signal:{}".format(signal))
            self.order_send_module.set_to_target_pos(target_pos)
            self.order_send_module.to_target_pos()

        if self.is_backtesting:
            ticker = TickData.make_ticker(bar.close_price)
            ticker.datetime = bar.datetime
            self.order_send_module.on_tick(ticker)

    def on_window_bar(self, bar):
        # self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        # 清仓
        self.order_send_module.set_to_target_pos(0)
        self.order_send_module.to_target_pos()

        self.oo = bar.close_price
        self.hh = bar.close_price
        self.ll = bar.close_price
        self.cz = bar.close_price
        self.hz = bar.high_price
        self.lz = bar.low_price

        self.now_open_num = 0

        self.strategy_statics.new_pos(self.pos)

    def on_stop_order(self, stop_order):
        pass

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.order_time:{},order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.order_time, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        self.order_send_module.on_order(order)
        self.pos = self.order_send_module.get_now_pos()

    def on_trade(self, trade):
        if trade.direction == Direction.LONG.value:
            self.pos += trade.volume
        else:
            self.pos -= trade.volume
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
