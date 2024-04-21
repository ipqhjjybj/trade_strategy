# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock
from enum import Enum

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import OrderData, BarData, TradeData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique
from tumbler.function.pnl import StrategyPnlStat

from tumbler.apps.cta_strategy.signals.ema_signal import EmaSignal
from tumbler.apps.cta_strategy.signals.haitun_signal import HaitunSignal


class LiveV2Strategy(CtaTemplate):
    """
    这个策略 首先通过计算出中枢，之后距离该中枢后，
    中枢之下开多
    中枢之上开空
    固定止损止盈
    """
    author = "ipqhjjybj"
    class_name = "LiveV2Strategy"

    symbol_pair = "btc_usd_swap"
    exchange = "OKEXS"

    bar_window = 4

    target_pos = 0
    pos = 0
    fixed = 1
    multi_xishu = 1

    parameters = ['strategy_name',              # 策略加载的唯一性名字
                  'class_name',                 # 类的名字
                  'author',                     # 作者
                  'vt_symbols_subscribe',       # vt_symbol_subscribe
                  'symbol_pair',                # 交易对
                  'exchange',                   # 交易所
                  'bar_window',                 # bar线
                  'signals',                    # signals
                  'pos',                        # pos 
                  'fixed',                      # fix
                  'multi_xishu',                # 浮亏情况下，手术增加倍数
                  'exchange_info'               # 交易所信息
                ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]


    def __init__(self, mm_engine, strategy_name, settings):
        super(LiveV2Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.bg = BarGenerator(self.on_bar, window=4, on_window_bar=self.on_window_bar, interval=Interval.HOUR.value, quick_minute=2)
        self.am = ArrayManager(40)

        self.vt_symbol = get_vt_key(self.symbol_pair, self.exchange)

        self.pnl_tj = StrategyPnlStat(self.vt_symbol)

        self.order_dict = {}

        self.signal_dict = {}
        for signal_name in settings["signals"].keys():
            sig = settings["signals"][signal_name]
            obj = None
            if signal_name == "ema":
                obj = EmaSignal(sig["fast_window"], sig["slow_window"])

            self.signal_dict[signal_name] = obj

    def on_init(self):
        self.write_log("on_init")
        self.update_account()
        self.load_bar(10)

    def update_account(self):
        # target
        """
        {'account_id': 'BTC-USD-SWAP', 'vt_account_id': 'OKEXS.BTC-USD-SWAP', 'balance': 0.0295, 'frozen': 0.0, 'available': 0.0295, 'gateway_name': 'OKEXS'}
        """
        acct = self.get_account(self.exchange_info["account_key"])
        if acct is not None:
            self.exchange_info["account_val"] = acct.balance

        self.update_account_flag = True

    def on_start(self):
        self.write_log("on_start")

    def on_stop(self):
        self.write_log("on_stop")

    def on_tick(self, tick):
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

    def cancel_all_orders(self):
        need_cancel_sets = []
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                need_cancel_sets.append(order.vt_order_id)
        
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)

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

    def on_bar(self, bar):
        self.write_log("[on_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        self.bg.update_bar(bar)

        self.cancel_all_orders()

        self.to_target_pos(bar)

    def on_window_bar(self, bar):
        self.write_log("[on_window_bar] [{}] high_price:{},low_price:{},pos:{}".format( bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),bar.high_price,bar.low_price,self.pos))
        
        am = self.am 
        am.update_bar(bar)

        for signal_name, sig_obj in self.signal_dict.items():
            sig_obj.on_bar(bar)

        if not am.inited:
            return

        self.target_pos = 0
        for signal_name, sig_obj in self.signal_dict.items():
            self.target_pos += sig_obj.get_signal_pos() * self.fixed


        self.target_pos = self.target_pos * self.multi_xishu

        self.write_log("target_pos:{}, pos:{}".format(self.target_pos, self.pos))

        self.to_target_pos(bar)

    def on_stop_order(self, stop_order):
        pass

    def on_order(self, order):
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}\n".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction ,order.price ,order.volume ,order.traded))

        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos += new_traded
                    self.on_make_new_trade(order.symbol, order.exchange, order.direction, order.price, new_traded)
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)
            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded
                    self.on_make_new_trade(order.symbol, order.exchange, order.direction, order.price, new_traded)
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)

    def on_make_new_trade(self, symbol, exchange, direction, price, volume):
        trade = TradeData()
        trade.symbol = symbol
        trade.exchange = exchange
        trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)
        trade.direction = direction
        trade.price = price
        trade.volume = volume

        self.pnl_tj.on_trade(trade)
        down_profit_rate = self.pnl_tj.get_down_profit_rate()
        down_profit = self.pnl_tj.get_down_profit()

        now_rate = down_profit * 1.0 / trade.price

        # if down_profit_rate < 0.8:
        #     self.multi_xishu = 2
        # if down_profit_rate > 0.3:
        #     self.multi_xishu = 2
        # elif down_profit_rate < 0.05:
        #     self.multi_xishu = 1

        # if now_rate > 0.5:
        #     self.multi_xishu = 2
        # elif now_rate < 0.05:
        #     self.multi_xishu = 1

        # print(now_rate, down_profit, down_profit_rate, self.multi_xishu)

        
    def on_trade(self, trade):
        self.write_trade(trade)

    def write_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id, trade.direction, trade.offset, trade.price, trade.volume)
        self.write_important_log(msg)

        self.write_log(msg)
