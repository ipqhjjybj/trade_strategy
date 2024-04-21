# coding=utf-8

from copy import copy

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.constant import Direction, Status, Interval, Offset
from tumbler.function.bar import BarGenerator, ArrayManager


class MinuteBollTestStrategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢 (中间轨道)，之后通过这个中枢确定方向。
    这个策略是一分钟级别的策略，先通过中轨确定方向，然后回到中轨开多(空)，到上下轨平多/空。
    如果中间发现方向变了，则尝试原价平多/空，否则一直拿到止损或者止盈。
    """
    author = "ipqhjjybj"
    class_name = "MinuteBollTestStrategy"

    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"

    # 策略参数
    bollinger_lengths = 100  # 布林通道参数
    offset = 3  # 布林通道参数

    interval = Interval.MINUTE.value

    bar_window = 4

    fixed = 1

    pos = 0

    exchange_info = {
        "price_tick": 0.1,
        "volume_tick": 1
    }

    parameters = ['strategy_name',  # 策略加载的唯一性名字
                  'class_name',  # 类的名字
                  'author',  # 作者
                  'vt_symbols_subscribe',  # vt_symbol_subscribe
                  "pos",  # 仓位
                  'symbol_pair',  # 交易对
                  'exchange',  # 交易所
                  'bollinger_lengths',  # 布林通道参数
                  'offset',  # 布林通道参数
                  'exchange_info',
                  'fixed',  #
                  'bar_window',
                  'interval'
                  ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(MinuteBollTestStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=self.interval)
        self.am = ArrayManager(self.bollinger_lengths + 40)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.target_pos = 0

        self.order_dict = {}

        self.direction = None

        self.pre_entry_price = 0

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

        am = self.am
        am.update_bar(bar)

        if not am.inited:
            return

        upband, dnband = am.boll(self.bollinger_lengths, self.offset)
        mid_line_arr = am.ma(self.bollinger_lengths, array=True)
        mid_price = mid_line_arr[-1]

        self.write_log("[on_bar] upband:{} dnband:{} mid_price:{}".format(upband, dnband, mid_price))

        if mid_line_arr[-1] > mid_line_arr[-2]:
            self.direction = True
        else:
            self.direction = False

        if self.pos == 0:
            price = mid_price
            volume = self.fixed
            vt_order_ids = self.send_order(self.symbol_pair, self.exchange,
                                           self.direction, Offset.OPEN.value, price, volume)
            for vt_order_id, order in vt_order_ids:
                self.order_dict[vt_order_id] = copy(order)

        elif self.pos > 0:
            if self.direction == Direction.LONG.value:
                if bar.close_price < dnband:
                    price = bar.close_price * 0.99
                    volume = abs(self.pos)
                    vt_order_ids = self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value,
                                                   Offset.CLOSE.value, price, volume)
                    for vt_order_id, order in vt_order_ids:
                        self.order_dict[vt_order_id] = copy(order)
                else:
                    price = upband
                    volume = abs(self.pos)
                    vt_order_ids = self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value,
                                                   Offset.CLOSE.value, price, volume)
                    for vt_order_id, order in vt_order_ids:
                        self.order_dict[vt_order_id] = copy(order)
            else:
                price = self.pre_entry_price
                volume = abs(self.pos)
                vt_order_ids = self.send_order(self.symbol_pair, self.exchange, Direction.SHORT.value,
                                               Offset.CLOSE.value, price, volume)
                for vt_order_id, order in vt_order_ids:
                    self.order_dict[vt_order_id] = copy(order)

        else:
            if self.direction == Direction.SHORT.value:
                if bar.close_price > upband:
                    price = bar.close_price * 1.01
                    volume = abs(self.pos)
                    vt_order_ids = self.send_order(self.symbol_pair, self.exchange_info, Direction.LONG.value,
                                                   Offset.OPEN.value, price, volume)
                    for vt_order_id, order in vt_order_ids:
                        self.order_dict[vt_order_id] = copy(order)
                else:
                    price = dnband
                    volume = abs(self.pos)
                    vt_order_ids = self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value,
                                                   Offset.CLOSE.value, price, volume)
                    for vt_order_id, order in vt_order_ids:
                        self.order_dict[vt_order_id] = copy(order)
            else:
                price = self.pre_entry_price
                volume = abs(self.pos)
                vt_order_ids = self.send_order(self.symbol_pair, self.exchange, Direction.LONG.value,
                                               Offset.CLOSE.value, price, volume)
                for vt_order_id, order in vt_order_ids:
                    self.order_dict[vt_order_id] = copy(order)

    def cancel_all_orders(self):
        need_cancel_sets = []
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)

        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

    def on_window_bar(self, bar):
        pass

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

                    if abs(self.pos) != 0:
                        self.pre_entry_price = order.price
                    else:
                        self.pre_entry_price = 0

                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)
            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded

                    if abs(self.pos) != 0:
                        self.pre_entry_price = order.price
                    else:
                        self.pre_entry_price = 0

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
