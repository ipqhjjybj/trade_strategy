# coding=utf-8

from copy import copy
from collections import defaultdict
from threading import Lock

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class BollingerBanditReverseStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "BollingerBanditReverseStrategy"

    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"

    # 策略参数
    bollinger_lengths = 50  # 布林通道参数
    offset = 1.25  # 布林通道参数

    interval = Interval.HOUR.value

    bar_window = 4

    fixed = 1

    pos = 0

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'vt_symbols_subscribe',  # vt_symbol_subscribe
                  "pos",  # 仓位
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bollinger_lengths',  # 布林通道参数
                  'offset',  # 布林通道参数
                  'fixed',  #
                  'bar_window',
                  'interval'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(BollingerBanditReverseStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval)
        self.am = ArrayManager(70)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.target_pos = 0

        self.order_dict = {}

    def on_init(self):
        self.write_log("on_init")
        self.load_bar(30)
        self.write_log("on_init after")

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
        self.tick_send_order(tick)
        self.bg.update_tick(tick)

    def get_already_send_volume(self):
        buy_volume, sell_volume = 0, 0
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                if order.direction == Direction.LONG.value:
                    buy_volume += order.volume
                else:
                    sell_volume += order.volume
        return buy_volume, sell_volume

    def on_bar(self, bar):
        self.write_log(
            "[on_bar] [{}] high_price:{},low_price:{},pos:{}".format(bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                                     bar.high_price, bar.low_price, self.pos))
        self.bg.update_bar(bar)

        self.cancel_all_orders()

        self.to_target_pos(bar)

    def cancel_all_orders(self):
        need_cancel_sets = []
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_window_bar(self, bar):
        self.write_log(
            "[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format(bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                                            bar.high_price, bar.low_price, self.pos))

        am = self.am
        am.update_bar(bar)

        self.target_pos = 0
        upband, dnband = am.boll(self.bollinger_lengths, self.offset)
        mid_line = am.ma(self.bollinger_lengths)

        if self.pos == 0:
            pass
        else:
            pass

        self.write_log("target_pos:{}, pos:{}".format(self.target_pos, self.pos))

        self.to_target_pos(bar)

    def to_target_pos(self, bar):
        if self.trading:
            buy_volume, sell_volume = self.get_already_send_volume()
            chazhi = self.target_pos - self.pos

            if chazhi > 0:
                uu_volume = chazhi - buy_volume
                if uu_volume > 0:
                    price = bar.close_price * 1.005
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.buy(self.symbol_pair, self.exchange, price, uu_volume)
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order
            elif chazhi < 0:
                uu_volume = chazhi + sell_volume
                if uu_volume < 0:
                    price = bar.close_price * 0.995
                    price = get_round_order_price(price, self.exchange_info["price_tick"])
                    list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(uu_volume))
                    for vt_order_id, order in list_orders:
                        self.order_dict[vt_order_id] = order

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos += new_traded
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)
            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

    def on_trade(self, trade):
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
