# coding=utf-8

from tumbler.constant import Interval, Direction
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.apps.cta_strategy.template import CtaTemplate


class AlphaSingleTestStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "AlphaSingleTestStrategy"

    symbol_pair = "btc_usdt"
    exchange = "BITMEX"

    bar_window = 5
    interval = Interval.MINUTE.value

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'func',  # 外部调用函数
                  'bar_window',  # bar线
                  'interval'  # interval
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AlphaSingleTestStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval, quick_minute=2)
        self.am = ArrayManager(300)

        self.pos = 0
        self.f = open("data.csv", "w")
        self.f.write("symbol,exchange,datetime,open,high,low,close,volume\n")

    def on_init(self):
        self.write_log("on_init")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.bg.update_tick(tick)

    def on_bar(self, bar):
        # self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

    def on_window_bar(self, bar):
        # self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        am = self.am
        am.update_bar(bar)

        msg = f"{bar.symbol},{bar.exchange},{bar.datetime},{bar.open_price},{bar.high_price},{bar.low_price}," \
              f"{bar.close_price},{bar.volume}\n"
        self.f.write(msg)
        self.f.flush()

        if not am.inited:
            return

        algo_pos = self.func(am)
        self.write_log(f"[on_window_bar] algo_pos:{algo_pos} pos:{self.pos}, date:{bar.datetime}")
        if algo_pos > self.pos:
            self.buy(self.symbol_pair, self.exchange, bar.close_price + 100, abs(algo_pos - self.pos))
        elif algo_pos < self.pos:
            self.short(self.symbol_pair, self.exchange, bar.close_price - 100, abs(self.pos - algo_pos))

    def on_stop_order(self, stop_order):
        pass

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

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
